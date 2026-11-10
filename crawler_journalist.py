"""
crawler_journalist.py
=====================
한국기자협회(journalist.or.kr) 한국기자협회보 인사·동정 아카이브 수집기.
- 대상 섹션: 인사·동정 전용 섹션 (sec_no=63)
- 수집 모드:
  1. 일일 증분 수집 (Incremental): 최신 1~2페이지 탐색 중 기등록 기사 발견 시 즉시 조기 종료 (Early Stop)
  2. 아카이브 전수 백필 (Full Backfill): 1페이지부터 전체 474페이지까지 체크포인트 기반 순회 수집
- Rate Limit 방어: 요청 간 0.5~1.2초 무작위 딜레이 및 재시도 메커니즘 적용
- 순수 언론사 인사 특성: 언론사 보도국/편집국 인사 집중 보도로 is_media_company=1 정밀도 극대화
"""

import os
import sys
import re
import json
import time
import random
import sqlite3
import logging
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_URL = "https://www.journalist.or.kr"
SECTION_URL = "https://www.journalist.or.kr/news/section_list_all.html?sec_no=63"
CHECKPOINT_FILE = "checkpoint_journalist.json"
DEFAULT_DB = "media_personnel.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}


def get_logger(name: str = "JournalistCrawler") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger


class JournalistCrawler:
    """한국기자협회보 전문 인사 기사 크롤러"""

    _SESSION: Optional[requests.Session] = None

    @classmethod
    def get_session(cls) -> requests.Session:
        if cls._SESSION is None:
            s = requests.Session()
            s.headers.update(HEADERS)
            cls._SESSION = s
        return cls._SESSION

    @classmethod
    def fetch_with_retry(cls, url: str, params: Optional[dict] = None, max_retries: int = 3) -> Optional[requests.Response]:
        """네트워크 요청 및 지수 백오프 재시도"""
        session = cls.get_session()
        for attempt in range(max_retries):
            try:
                resp = session.get(url, params=params, timeout=10, verify=False)
                if resp.status_code == 200:
                    return resp
            except Exception as e:
                time.sleep(1.0 + attempt * 1.5)
        return None

    @classmethod
    def get_total_pages(cls) -> int:
        """인사·동정(sec_no=63) 섹션의 총 페이지 수를 동적으로 확인합니다."""
        resp = cls.fetch_with_retry(SECTION_URL, params={"page": 1})
        if not resp:
            return 474  # 안전 기본값
        soup = BeautifulSoup(resp.content, "html.parser")
        end_link = soup.select_one("li.end a, a.et")
        if end_link and "page=" in end_link.get("href", ""):
            m = re.search(r"page=(\d+)", end_link["href"])
            if m:
                return int(m.group(1))
        # 페이지 번호 목록 중 최댓값 탐색
        page_nums = []
        for a in soup.select("ul.pagination a, .paging a"):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                page_nums.append(int(m.group(1)))
        return max(page_nums) if page_nums else 474

    @classmethod
    def fetch_page_article_links(cls, page: int = 1) -> List[Dict[str, Any]]:
        """특정 페이지 내의 인사 기사 링크 목록을 수집합니다 (부고 기사는 배제)."""
        resp = cls.fetch_with_retry(SECTION_URL, params={"page": page})
        if not resp:
            return []

        soup = BeautifulSoup(resp.content, "html.parser")
        candidates: List[Dict[str, Any]] = []
        seen_nos = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "article.html?no=" not in href:
                continue

            raw_title = a.get_text(strip=True)
            if not raw_title or len(raw_title) < 3:
                continue

            # 부고 제외 및 인사 기사 패턴 검증
            if "부고" in raw_title or "[부고]" in raw_title or "<부고>" in raw_title:
                continue

            is_personnel = (
                "[인사]" in raw_title or "<인사>" in raw_title or
                "인사" in raw_title or "발령" in raw_title
            )
            if not is_personnel:
                continue

            m = re.search(r"no=(\d+)", href)
            if not m:
                continue
            art_no = m.group(1)
            if art_no in seen_nos:
                continue
            seen_nos.add(art_no)

            full_url = f"{BASE_URL}/news/article.html?no={art_no}"
            candidates.append({
                "no": art_no,
                "title": raw_title,
                "url": full_url,
                "article_id": full_url
            })

        return candidates

    @classmethod
    def fetch_article_detail(cls, url: str) -> Optional[Dict[str, Any]]:
        """기사 상세 페이지에서 제목, 본문, 발행일, 대상 언론사를 정밀 추출합니다."""
        resp = cls.fetch_with_retry(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.content, "html.parser")

        # 1. 기사 제목
        head = soup.select_one(".article_head")
        title = ""
        if head:
            title_p = head.find("p", class_="title") or head.find("h1") or head.find("h2")
            if title_p:
                title = title_p.get_text(strip=True)
        if not title:
            title = soup.title.string.split(" - ")[0].strip() if soup.title else ""

        # 2. 발행 일자 (YYYY-MM-DD)
        pub_date = ""
        if head:
            byline_div = head.find("div", class_="byline")
            if byline_div:
                b_text = byline_div.get_text(strip=True)
                m = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/](\d{1,2})[.\-/](\d{1,2})", b_text)
                if m:
                    pub_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        if not pub_date:
            m = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/](\d{1,2})[.\-/](\d{1,2})", resp.text[:1500])
            if m:
                pub_date = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            else:
                pub_date = time.strftime("%Y-%m-%d")

        # 3. 본문 텍스트 (기자협회보 공통 푸터 제거)
        body = soup.select_one("#news_body_area")
        if not body:
            return None

        raw_content = body.get_text("\n", strip=True)
        # 하단 "한국기자협회의 전체기사 보기" 및 공유 텍스트 정제
        cleaned_lines = []
        for line in raw_content.splitlines():
            line_str = line.strip()
            if not line_str:
                continue
            if "전체기사 보기" in line_str or "기자협회보" in line_str and "무단전재" in line_str:
                break
            cleaned_lines.append(line_str)

        content = "\n".join(cleaned_lines)
        if len(content) < 5:
            return None

        # 4. 대상 언론사 판별
        target_media = cls.identify_media_from_article(title, content)

        return {
            "article_id": url,
            "media_source": "한국기자협회",
            "title": title,
            "content": content,
            "publish_date": pub_date,
            "is_media_company": 1,
            "target_media": target_media,
            "origin_url": url
        }

    @classmethod
    def identify_media_from_article(cls, title: str, content: str) -> str:
        """기사 제목 및 본문 헤더에서 발령 대상 언론사명을 추출합니다."""
        # 제목 패턴: [인사] 언론사명, [인사] 언론사명 외
        m = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", title)
        if m:
            cand = m.group(1).strip()
            if cand and not cand in ["종합", "외", "등", "오늘의"]:
                return cand

        # 본문 최상단 헤더 패턴 (◇ 언론사명)
        for line in content.splitlines()[:5]:
            line_clean = line.strip()
            sec_m = re.match(r"^[◇◆■▲\[<]\s*([가-힣A-Za-z0-9]+)", line_clean)
            if sec_m:
                cand = sec_m.group(1).strip()
                if any(k in cand for k in ["일보", "신문", "방송", "뉴스", "경제", "미디어", "헤럴드", "타임스"]):
                    return cand

        return "한국기자협회보"

    @classmethod
    def crawl_incremental(
        cls,
        db_path: str = DEFAULT_DB,
        max_pages: int = 2,
        stop_on_existing: bool = True,
        logger: Optional[logging.Logger] = None
    ) -> Dict[str, Any]:
        """
        1~2페이지 내에서 신규 기사만 감지하여 즉시 동기화합니다.
        기등록 기사를 만나면 Early Stop을 수행합니다.
        """
        log = logger or get_logger()
        log.info(f"[한국기자협회] 증분 수집 시작 (최대 {max_pages}페이지, EarlyStop={stop_on_existing})")

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # 테이블 생성 보장 (media_personnel.db 표준 스키마 호환)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_articles (
            article_id TEXT PRIMARY KEY,
            media_source TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            publish_date DATE NOT NULL,
            target_media TEXT,
            is_media_company BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()

        articles_checked = 0
        new_saved = 0
        stopped_early = False
        stopped_url = None

        for page in range(1, max_pages + 1):
            log.info(f"▶ 한국기자협회 {page}페이지 스캔 중...")
            links = cls.fetch_page_article_links(page=page)
            if not links:
                break

            for item in links:
                articles_checked += 1
                art_url = item["url"]

                cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (art_url,))
                if cur.fetchone():
                    if stop_on_existing:
                        stopped_early = True
                        stopped_url = art_url
                        log.info(f"  [조기 종료] 이미 등록된 기사 감지: {item['title'][:35]} ({art_url})")
                        break
                    else:
                        continue

                # 신규 기사 상세 수집
                detail = cls.fetch_article_detail(art_url)
                if not detail:
                    continue

                cur.execute("""
                INSERT OR IGNORE INTO raw_articles (
                    article_id, media_source, title, content, publish_date,
                    target_media, is_media_company
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    detail["article_id"],
                    detail["media_source"],
                    detail["title"],
                    detail["content"],
                    detail["publish_date"],
                    detail["target_media"],
                    detail["is_media_company"]
                ))
                conn.commit()
                new_saved += 1
                log.info(f"  + 신규 저장: [{detail['publish_date']}] {detail['title'][:35]} (대상: {detail['target_media']})")

                # Rate Limit
                time.sleep(random.uniform(0.5, 1.2))

            if stopped_early:
                break

            time.sleep(random.uniform(0.6, 1.2))

        conn.close()

        log.info(f"[한국기자협회] 증분 수집 완료: 확인 {articles_checked}건, 신규 저장 {new_saved}건 (조기종료={stopped_early})")
        return {
            "source": "한국기자협회",
            "articles_checked": articles_checked,
            "new_saved": new_saved,
            "stopped_early": stopped_early,
            "stopped_url": stopped_url
        }

    @classmethod
    def crawl_backfill(
        cls,
        db_path: str = DEFAULT_DB,
        start_page: int = 1,
        end_page: Optional[int] = None,
        checkpoint_file: str = CHECKPOINT_FILE,
        reverse: bool = True,
        logger: Optional[logging.Logger] = None
    ) -> Dict[str, Any]:
        """
        한국기자협회보 아카이브 전체 기간 백필(Backfill)을 수행합니다.
        체크포인트 이어받기 및 0.5~1.2초 Rate Limiting을 적용합니다.
        """
        log = logger or get_logger()
        total_available = cls.get_total_pages()
        target_end = end_page if end_page is not None else total_available

        # 체크포인트 불러오기
        last_processed_page = None
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, "r", encoding="utf-8") as f:
                    cp = json.load(f)
                    last_processed_page = cp.get("last_page")
                    log.info(f"체크포인트 발견: 이전 처리 완료 페이지={last_processed_page}")
            except Exception:
                pass

        if last_processed_page is not None:
            if reverse:
                # 역방향 (예: 474 -> 1)
                effective_start = min(last_processed_page - 1, target_end)
            else:
                # 정방향 (예: 1 -> 474)
                effective_start = max(last_processed_page + 1, start_page)
        else:
            effective_start = target_end if reverse else start_page

        log.info("=" * 65)
        log.info(f"한국기자협회 아카이브 전수 백필 가동")
        log.info(f"총 아카이브 페이지: {total_available}페이지")
        log.info(f"탐색 방향: {'역방향 (과거 -> 최신)' if reverse else '정방향 (최신 -> 과거)'}")
        log.info(f"시작 페이지: {effective_start} | 종료 페이지: {start_page if reverse else target_end}")
        log.info("=" * 65)

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_articles (
            article_id TEXT PRIMARY KEY,
            media_source TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            publish_date DATE NOT NULL,
            target_media TEXT,
            is_media_company BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()

        page_range = (
            range(effective_start, start_page - 1, -1)
            if reverse
            else range(effective_start, target_end + 1)
        )

        total_saved = 0
        pages_processed = 0

        try:
            for p in page_range:
                log.info(f"\n[페이지 {p}/{total_available}] 기사 수집 시작...")
                links = cls.fetch_page_article_links(page=p)
                page_saved = 0

                for item in links:
                    art_url = item["url"]
                    cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (art_url,))
                    if cur.fetchone():
                        continue

                    detail = cls.fetch_article_detail(art_url)
                    if not detail:
                        continue

                    cur.execute("""
                    INSERT OR IGNORE INTO raw_articles (
                        article_id, media_source, title, content, publish_date,
                        target_media, is_media_company
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        detail["article_id"],
                        detail["media_source"],
                        detail["title"],
                        detail["content"],
                        detail["publish_date"],
                        detail["target_media"],
                        detail["is_media_company"]
                    ))
                    conn.commit()
                    page_saved += 1
                    total_saved += 1
                    time.sleep(random.uniform(0.5, 1.2))

                pages_processed += 1
                log.info(f"[페이지 {p}] 완료: {page_saved}건 신규 저장 (누적 {total_saved}건)")

                # 체크포인트 저장
                with open(checkpoint_file, "w", encoding="utf-8") as f:
                    json.dump({"last_page": p, "total_saved": total_saved, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}, f, ensure_ascii=False, indent=2)

                time.sleep(random.uniform(0.7, 1.5))

            log.info(f"\n한국기자협회 백필 완료! 총 {pages_processed}페이지 처리, {total_saved}건 신규 저장.")

        except KeyboardInterrupt:
            log.warning("\n[사용자 중단] 진행 상황이 체크포인트 파일에 보존되었습니다.")
        finally:
            conn.close()

        return {
            "pages_processed": pages_processed,
            "total_saved": total_saved
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="한국기자협회보 인사 아카이브 수집기")
    parser.add_argument("--backfill", action="store_true", help="아카이브 전체 전수 백필 실행")
    parser.add_argument("--start-page", type=int, default=1, help="시작 페이지 (기본값: 1)")
    parser.add_argument("--max-pages", type=int, default=2, help="증분 수집 시 탐색할 최대 페이지 (기본값: 2)")
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB, help="데이터베이스 경로")
    args = parser.parse_args()

    logger = get_logger()
    if args.backfill:
        JournalistCrawler.crawl_backfill(db_path=args.db_path, start_page=args.start_page, reverse=True, logger=logger)
    else:
        JournalistCrawler.crawl_incremental(db_path=args.db_path, max_pages=args.max_pages, stop_on_existing=True, logger=logger)


if __name__ == "__main__":
    main()
