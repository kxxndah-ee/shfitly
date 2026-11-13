"""
daily_sync.py
=============
연합뉴스, 뉴스1, 중앙일보, 한국기자협회 4개 매체 통합 일일 자동 증분 수집 및 이직 추적 파이프라인.
- Step 1: fetch_incremental() - 4대 소스 신규 인사 기사 증분 스캔 및 저장 (Early Stop 적용)
- Step 2: parse_unprocessed() - 미파싱 기사 정밀 파싱 및 career_events 적재
- Step 3: track_turnover()    - 6개월 윈도우, 부서 유사도, 직급 점수 기반 이직 판정 및 프로필 갱신
- Step 4: 실행 결과 요약 로깅 및 스케줄러 (Windows Task Scheduler, Linux cron, 자체 데몬 모드) 지원
"""

import os
import sys
import re
import time
import json
import random
import logging
import sqlite3
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import quote

import requests
import urllib3
from bs4 import BeautifulSoup

# 경고 무시 및 UTF-8 보장
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from crawler_journalist import JournalistCrawler
from parser_pipeline import (
    parse_article_content,
    normalize_rank_score,
    categorize_dept,
    clean_name,
    DEPT_CATEGORIES,
    RANK_SCORE_MAP
)
from data_pipeline import (
    PersonnelTrackerEngine,
    PersonnelRegexParser,
    is_media_organization,
    is_excluded_department
)

DB_NAME = "media_personnel.db"
TRACKER_DB_NAME = "media_tracker.db"
LOG_FILE = "daily_sync.log"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

# 9대 부서 카테고리 간 유사도 행렬
CATEGORY_SIMILARITY = {
    ("정치/행정", "사회/법조"): 0.80,
    ("정치/행정", "국제/외신"): 0.80,
    ("정치/행정", "논설/해설"): 0.85,
    ("경제/산업/금융", "IT/과학"): 0.80,
    ("경제/산업/금융", "디지털/뉴미디어"): 0.75,
    ("경제/산업/금융", "논설/해설"): 0.80,
    ("사회/법조", "문화/연예/스포츠"): 0.75,
    ("사회/법조", "논설/해설"): 0.80,
    ("디지털/뉴미디어", "편집/교열/사진"): 0.80
}


def setup_logger(log_file: str = LOG_FILE) -> logging.Logger:
    """콘솔 및 파일 로깅을 설정합니다."""
    logger = logging.getLogger("DailySyncPipeline")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    try:
        fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception:
        pass

    return logger


class MultiSourcePersonnelSync:
    """4개 매체 통합 일일 자동 동기화 및 이직 추적 엔진"""

    def __init__(self, db_path: str = DB_NAME, tracker_db_path: str = TRACKER_DB_NAME, logger: Optional[logging.Logger] = None):
        self.db_path = db_path
        self.tracker_db_path = tracker_db_path
        self.logger = logger or setup_logger()
        self._ensure_db_schema()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_db_schema(self):
        """테이블 및 뷰 스키마가 최신 명세(4대 매체 포함)에 맞추어 존재함을 보장합니다."""
        conn = self._get_connection()
        cur = conn.cursor()

        # 1. 원문 기사 테이블
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

        # 2. 발령 이벤트 테이블
        cur.execute("""
        CREATE TABLE IF NOT EXISTS career_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id TEXT NOT NULL,
            person_name TEXT NOT NULL,
            company_name TEXT NOT NULL,
            department_raw TEXT,
            dept_category TEXT,
            position_raw TEXT,
            rank_score INTEGER NOT NULL,
            action_type TEXT,
            event_date DATE NOT NULL,
            FOREIGN KEY (article_id) REFERENCES raw_articles(article_id)
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_career_person ON career_events(person_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_career_company ON career_events(company_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_career_date ON career_events(event_date);")

        # 3. 통합 인물 마스터 테이블
        cur.execute("""
        CREATE TABLE IF NOT EXISTS person_profiles (
            person_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            current_company TEXT,
            current_department TEXT,
            current_position TEXT,
            last_event_date DATE,
            needs_review BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # 4. 이직(Turnover) 이력 테이블
        cur.execute("""
        CREATE TABLE IF NOT EXISTS turnover_histories (
            turnover_id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            from_company TEXT NOT NULL,
            to_company TEXT NOT NULL,
            from_dept TEXT,
            to_dept TEXT,
            gap_days INTEGER NOT NULL,
            turnover_date DATE NOT NULL,
            confidence_score REAL DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (person_id) REFERENCES person_profiles(person_id)
        );
        """)
        try:
            cur.execute("ALTER TABLE turnover_histories ADD COLUMN confidence_score REAL DEFAULT 1.0;")
        except Exception:
            pass
        cur.execute("CREATE INDEX IF NOT EXISTS idx_turnover_person ON turnover_histories(person_id);")

        # 5. 핵심 SQL View 갱신 (4개 매체 통합)
        cur.execute("DROP VIEW IF EXISTS v_media_only_events;")
        cur.execute("""
        CREATE VIEW v_media_only_events AS
        SELECT 
            c.event_id,
            c.article_id,
            c.person_name,
            c.company_name,
            c.department_raw,
            c.dept_category,
            c.position_raw,
            c.rank_score,
            c.action_type,
            c.event_date,
            r.media_source,
            r.title,
            r.publish_date
        FROM career_events c
        JOIN raw_articles r ON c.article_id = r.article_id
        WHERE r.media_source IN ('연합뉴스', '뉴스1', '중앙일보', '한국기자협회')
          AND (r.title LIKE '%[인사]%' OR r.title LIKE '%<인사>%')
          AND r.is_media_company = 1;
        """)

        conn.commit()
        conn.close()

    # =========================================================================
    # Step 1: 4대 소스 신규 기사 증분 수집 (fetch_incremental)
    # =========================================================================
    def fetch_incremental(self, max_pages: int = 2) -> Dict[str, int]:
        """
        연합뉴스, 뉴스1, 중앙일보, 한국기자협회 4개 매체에서 신규 [인사]/<인사> 기사만 증분 수집합니다.
        이미 등록된 기사를 발견하면 해당 소스 탐색을 즉시 멈추는 Early Stop이 작동합니다.
        """
        self.logger.info("=" * 60)
        self.logger.info("[Step 1: 4대 매체 신규 인사 기사 증분 수집 시작]")
        self.logger.info("대상: 연합뉴스, 뉴스1, 중앙일보, 한국기자협회")
        self.logger.info("=" * 60)

        results = {
            "연합뉴스": 0,
            "뉴스1": 0,
            "중앙일보": 0,
            "한국기자협회": 0,
            "total_new": 0
        }

        # 1. 한국기자협회보 (journalist.or.kr)
        try:
            jou_res = JournalistCrawler.crawl_incremental(
                db_path=self.db_path,
                max_pages=max_pages,
                stop_on_existing=True,
                logger=self.logger
            )
            results["한국기자협회"] = jou_res.get("new_saved", 0)
        except Exception as e:
            self.logger.error(f"[한국기자협회 수집 오류] {e}")

        # 2. 중앙일보 (joongang.co.kr 피플 인사탭)
        try:
            ja_saved = self._crawl_joongang_incremental(max_pages=max_pages)
            results["중앙일보"] = ja_saved
        except Exception as e:
            self.logger.error(f"[중앙일보 수집 오류] {e}")

        # 3. 뉴스1 (news1.kr)
        try:
            n1_saved = self._crawl_portal_wire_incremental(press="뉴스1", max_pages=max_pages)
            results["뉴스1"] = n1_saved
        except Exception as e:
            self.logger.error(f"[뉴스1 수집 오류] {e}")

        # 4. 연합뉴스 (yna.co.kr)
        try:
            yna_saved = self._crawl_portal_wire_incremental(press="연합뉴스", max_pages=max_pages)
            results["연합뉴스"] = yna_saved
        except Exception as e:
            self.logger.error(f"[연합뉴스 수집 오류] {e}")

        results["total_new"] = (
            results["한국기자협회"] + results["중앙일보"] +
            results["뉴스1"] + results["연합뉴스"]
        )
        self.logger.info(
            f"[Step 1 완료] 신규 기사 저장: 총 {results['total_new']}건 "
            f"(연합: {results['연합뉴스']}, 뉴스1: {results['뉴스1']}, "
            f"중앙: {results['중앙일보']}, 기자협회: {results['한국기자협회']})"
        )
        return results

    def _crawl_joongang_incremental(self, max_pages: int = 2) -> int:
        """중앙일보 피플 공식 인사탭 증분 수집"""
        self.logger.info(f"▶ [중앙일보] 증분 수집 시작 (최대 {max_pages}페이지)...")
        conn = self._get_connection()
        cur = conn.cursor()
        saved_count = 0
        stopped_early = False

        for page in range(1, max_pages + 1):
            url = f"https://www.joongang.co.kr/people/personnel?page={page}"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=8, verify=False)
                if resp.status_code != 200:
                    break
                soup = BeautifulSoup(resp.content, "html.parser")
                cards = soup.select("h2.headline a, .card_body a, a[href*='/article/']")
                if not cards:
                    break

                for a in cards:
                    href = a.get("href", "")
                    title = a.get_text(strip=True)
                    if "/article/" not in href or not title or len(title) < 3:
                        continue
                    if not ("[인사]" in title or "<인사>" in title or "인사" in title):
                        continue

                    full_url = href if href.startswith("http") else f"https://www.joongang.co.kr{href}"

                    # Early Stop 검사
                    cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (full_url,))
                    if cur.fetchone():
                        stopped_early = True
                        self.logger.info(f"  [조기 종료] 중앙일보 기등록 기사 발견: {title[:35]} ({full_url})")
                        break

                    # 상세 본문 수집
                    detail_resp = requests.get(full_url, headers=HEADERS, timeout=8, verify=False)
                    if detail_resp.status_code != 200:
                        continue
                    d_soup = BeautifulSoup(detail_resp.content, "html.parser")
                    body = d_soup.select_one(".article_body, #article_body, div[itemprop='articleBody']")
                    content = body.get_text("\n", strip=True) if body else ""
                    if len(content) < 10:
                        continue

                    # 날짜 추출
                    date_elem = d_soup.select_one(".date, .byline, meta[property='article:published_time']")
                    date_str = ""
                    if date_elem:
                        dt_text = date_elem.get("content", "") or date_elem.get_text(strip=True)
                        m = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/](\d{1,2})[.\-/](\d{1,2})", dt_text)
                        if m:
                            date_str = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
                    if not date_str:
                        date_str = time.strftime("%Y-%m-%d")

                    # 대상 언론사 및 언론사 인사 여부 판정
                    target_media = "중앙일보"
                    t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", title)
                    if t_match:
                        target_media = t_match.group(1).strip()

                    # 저장
                    cur.execute("""
                    INSERT OR IGNORE INTO raw_articles (
                        article_id, media_source, title, content, publish_date,
                        target_media, is_media_company
                    ) VALUES (?, '중앙일보', ?, ?, ?, ?, 1)
                    """, (full_url, title, content, date_str, target_media))
                    conn.commit()
                    saved_count += 1
                    self.logger.info(f"  + 중앙일보 신규 저장: [{date_str}] {title[:35]}")
                    time.sleep(random.uniform(0.5, 1.0))

                if stopped_early:
                    break
                time.sleep(random.uniform(0.6, 1.2))

            except Exception as e:
                self.logger.warning(f"  중앙일보 {page}페이지 탐색 중 예외: {e}")
                break

        conn.close()
        return saved_count

    def _crawl_portal_wire_incremental(self, press: str, max_pages: int = 2) -> int:
        """다음 및 네이버 모바일을 통한 통신사(연합뉴스·뉴스1) 최신 인사 기사 증분 수집"""
        self.logger.info(f"▶ [{press}] 포털 증분 수집 시작...")
        conn = self._get_connection()
        cur = conn.cursor()
        saved_count = 0
        stopped_early = False

        query_str = f'"[인사]" {press}'
        # 1. 다음 뉴스 최신순 수집
        try:
            daum_url = f"https://search.daum.net/search?w=news&q={quote(query_str)}&sort=recency"
            resp = requests.get(daum_url, headers=HEADERS, timeout=8, verify=False)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, "html.parser")
                cards = soup.select("div.item-title, strong.tit-doc, a.tit_main")
                for c in cards:
                    a = c if c.name == "a" else c.find("a")
                    if not a:
                        continue
                    title = a.get_text(strip=True)
                    href = a.get("href", "")
                    if not href or not title:
                        continue
                    if not ("[인사]" in title or "<인사>" in title):
                        continue

                    # Early Stop
                    cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (href,))
                    if cur.fetchone():
                        stopped_early = True
                        self.logger.info(f"  [조기 종료] {press} 기등록 기사 발견: {title[:35]} ({href})")
                        break

                    # 본문 상세 수집
                    detail_resp = requests.get(href, headers=HEADERS, timeout=8, verify=False)
                    if detail_resp.status_code != 200:
                        continue
                    d_soup = BeautifulSoup(detail_resp.content, "html.parser")
                    body = d_soup.select_one("#article_body, .article_view, section[itemprop='articleBody'], div.article_content")
                    content = body.get_text("\n", strip=True) if body else ""
                    if len(content) < 10:
                        continue

                    # 날짜 추출
                    m_date = re.search(r"/(19[6-9]\d|20[0-3]\d)(\d{2})(\d{2})", href)
                    if m_date:
                        date_str = f"{m_date.group(1)}-{m_date.group(2)}-{m_date.group(3)}"
                    else:
                        m_txt = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", detail_resp.text[:1200])
                        date_str = f"{m_txt.group(1)}-{int(m_txt.group(2)):02d}-{int(m_txt.group(3)):02d}" if m_txt else time.strftime("%Y-%m-%d")

                    # 언론사명 추출
                    t_media = press
                    t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", title)
                    if t_match:
                        t_media = t_match.group(1).strip()

                    cur.execute("""
                    INSERT OR IGNORE INTO raw_articles (
                        article_id, media_source, title, content, publish_date,
                        target_media, is_media_company
                    ) VALUES (?, ?, ?, ?, ?, ?, 1)
                    """, (href, press, title, content, date_str, t_media))
                    conn.commit()
                    saved_count += 1
                    self.logger.info(f"  + {press} 신규 저장: [{date_str}] {title[:35]}")
                    time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            self.logger.warning(f"  {press} 다음 검색 수집 중 예외: {e}")

        # 2. 공식 언론사 직통 인사 섹션 보조 수집 (포털 캡차 및 차단 방어)
        try:
            direct_items = []
            if press == "연합뉴스":
                for p in range(1, max_pages + 1):
                    y_resp = requests.get(f"https://www.yna.co.kr/people/personnel/{p}", headers=HEADERS, timeout=8, verify=False)
                    if y_resp.status_code == 200:
                        y_soup = BeautifulSoup(y_resp.content, "html.parser")
                        for a in y_soup.select("div.item-box01 a, ul.list a, a[href*='/view/AKR']"):
                            h = a.get("href", "")
                            t = a.get_text(strip=True)
                            if h and "/view/AKR" in h:
                                fu = h if h.startswith("http") else f"https:{h}" if h.startswith("//") else f"https://www.yna.co.kr{h}"
                                direct_items.append((fu, t))
            elif press == "뉴스1":
                for p in range(1, max_pages + 1):
                    n_url = f"https://www.news1.kr/life-culture/press/?page={p}" if p > 1 else "https://www.news1.kr/life-culture/press/"
                    n_resp = requests.get(n_url, headers=HEADERS, timeout=8, verify=False)
                    if n_resp.status_code == 200:
                        n_soup = BeautifulSoup(n_resp.content, "html.parser")
                        nd = n_soup.find("script", id="__NEXT_DATA__")
                        if nd and nd.string:
                            try:
                                import json
                                nd_d = json.loads(nd.string)
                                for it in nd_d.get("props", {}).get("pageProps", {}).get("subsectionData", []):
                                    fu = f"https://www.news1.kr/life-culture/press/{it.get('id')}"
                                    direct_items.append((fu, it.get("title", "")))
                            except Exception:
                                pass

            for fu, t in direct_items:
                cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (fu,))
                if cur.fetchone():
                    continue
                d_resp = requests.get(fu, headers=HEADERS, timeout=8, verify=False)
                if d_resp.status_code != 200:
                    continue
                d_soup = BeautifulSoup(d_resp.content, "html.parser")
                body = d_soup.select_one("article.story-news, #article_body, div.article_content, #articleWrap")
                content = body.get_text("\n", strip=True) if body else ""
                if len(content) < 20:
                    continue

                m_date = re.search(r"AKR(19[6-9]\d|20[0-3]\d)(\d{2})(\d{2})", fu)
                if m_date:
                    date_str = f"{m_date.group(1)}-{m_date.group(2)}-{m_date.group(3)}"
                else:
                    m_txt = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", d_resp.text[:1200])
                    date_str = f"{m_txt.group(1)}-{int(m_txt.group(2)):02d}-{int(m_txt.group(3)):02d}" if m_txt else time.strftime("%Y-%m-%d")

                t_media = press
                t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", t)
                if t_match:
                    t_media = t_match.group(1).strip()

                cur.execute("""
                INSERT OR IGNORE INTO raw_articles (
                    article_id, media_source, title, content, publish_date,
                    target_media, is_media_company
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
                """, (fu, press, t, content, date_str, t_media))
                conn.commit()
                saved_count += 1
                self.logger.info(f"  + {press} 직통 신규 저장: [{date_str}] {t[:35]}")
                time.sleep(random.uniform(0.3, 0.6))
        except Exception as e:
            self.logger.warning(f"  {press} 직통 수집 중 예외: {e}")

        conn.close()
        return saved_count

    # =========================================================================
    # Step 2: 미파싱 기사 일괄 파싱 (parse_unprocessed)
    # =========================================================================
    def parse_unprocessed(self, batch_size: int = 1000) -> int:
        """
        raw_articles 테이블에서 아직 career_events에 등록되지 않은 순수 언론사 인사 기사를 찾아
        개별 발령 레코드(이름, 부서, 직급, 직급서열, 발령일)로 분해하여 적재합니다.
        """
        self.logger.info("=" * 60)
        self.logger.info("[Step 2: 신규/미파싱 기사 발령 데이터 추출 시작]")
        self.logger.info("=" * 60)

        conn = self._get_connection()
        cur = conn.cursor()

        cur.execute("""
        SELECT r.article_id, r.media_source, r.title, r.content, r.publish_date, r.target_media
        FROM raw_articles r
        LEFT JOIN career_events c ON r.article_id = c.article_id
        WHERE r.is_media_company = 1
          AND c.event_id IS NULL
        LIMIT ?;
        """, (batch_size,))
        articles = cur.fetchall()

        if not articles:
            self.logger.info("[Step 2 완료] 새로 파싱할 기사가 없습니다.")
            conn.close()
            return 0

        self.logger.info(f"파싱 대상 기사: 총 {len(articles)}건")
        total_events = 0
        all_tracker_records = []

        for art in articles:
            art_id = art["article_id"]
            title = art["title"]
            content = art["content"]
            pub_date = art["publish_date"]
            target_media = art["target_media"] or "언론사"

            # 1. media_personnel.db career_events 포맷 파싱
            parsed_list = parse_article_content(content, target_media, pub_date)

            for rec in parsed_list:
                cur.execute("""
                INSERT INTO career_events (
                    article_id, person_name, company_name, department_raw, 
                    dept_category, position_raw, rank_score, action_type, event_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    art_id,
                    rec["person_name"],
                    rec["company_name"],
                    rec["department_raw"],
                    rec["dept_category"],
                    rec["position_raw"],
                    rec["rank_score"],
                    rec["action_type"],
                    rec["event_date"]
                ))
                total_events += 1

            # 2. media_tracker.db (UI 연동) personnel_records 포맷 파싱
            regex_recs = PersonnelRegexParser.parse_article_text(
                article_text=content,
                default_media=target_media,
                article_title=title,
                pub_date=pub_date,
                article_url=art_id
            )
            matched_regex_recs = [
                r for r in regex_recs
                if is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", ""))
            ]
            if matched_regex_recs:
                all_tracker_records.extend(matched_regex_recs)

        conn.commit()
        conn.close()

        # UI 연동 DB (media_tracker.db) 동기화
        if all_tracker_records:
            try:
                engine = PersonnelTrackerEngine(self.tracker_db_path)
                engine.process_records(all_tracker_records, append=True)
                self.logger.info(f"[Step 2 동기화] {self.tracker_db_path} 에 {len(all_tracker_records)}건 발령 데이터 즉시 연동 완료.")
                try:
                    db_inst = MediaDatabase(self.tracker_db_path)
                    db_inst.dump_seed_data()
                except Exception:
                    pass
            except Exception as e:
                self.logger.error(f"[Step 2 동기화 오류] {self.tracker_db_path} 연동 실패: {e}")

        self.logger.info(f"[Step 2 완료] {len(articles)}건 기사 파싱 -> {total_events}건 발령 이벤트 적재 완료.")
        return total_events

    # =========================================================================
    # Step 3: 이직(Turnover) 판정 및 인물 프로필 갱신 (track_turnover)
    # =========================================================================
    def track_turnover(self) -> Dict[str, int]:
        """
        신규 발령 데이터를 바탕으로:
        1. 180일(6개월) 이내 윈도우 규칙
        2. 부서 카테고리 유사도 (>= 0.75)
        3. 직급 서열 점수 (동등 이상 유지 및 승진)
        조건을 검증하여 turnover_histories 및 person_profiles 마스터를 실시간 갱신합니다.
        """
        self.logger.info("=" * 60)
        self.logger.info("[Step 3: 언론사 간 이직(Turnover) 추적 및 프로필 갱신 시작]")
        self.logger.info("규칙: 6개월(180일) 윈도우, 부서 유사도 >= 0.75, 직급 서열 유지/상승")
        self.logger.info("=" * 60)

        conn = self._get_connection()
        cur = conn.cursor()

        # 고유 언론인 명단 조회
        cur.execute("SELECT DISTINCT person_name FROM career_events ORDER BY person_name")
        names = [r["person_name"] for r in cur.fetchall()]

        new_turnovers_count = 0
        profiles_updated = 0

        # 기존 등록된 turnover 집합 캐시 (중복 방지: person_id, from, to, date)
        cur.execute("SELECT person_id, from_company, to_company, turnover_date FROM turnover_histories")
        existing_turnovers = {
            (r["person_id"], r["from_company"], r["to_company"], str(r["turnover_date"])[:10])
            for r in cur.fetchall()
        }

        for idx, name in enumerate(names, 1):
            person_id = f"P_{idx:05d}"

            cur.execute("""
            SELECT * FROM career_events 
            WHERE person_name = ? 
            ORDER BY event_date ASC, rank_score ASC
            """, (name,))
            events = [dict(r) for r in cur.fetchall()]

            if not events:
                continue

            has_turnover = False
            for i in range(len(events) - 1):
                prev_ev = events[i]
                curr_ev = events[i + 1]

                is_to, conf, gap = self._evaluate_turnover(prev_ev, curr_ev)
                if is_to:
                    has_turnover = True
                    to_date = str(curr_ev["event_date"])[:10]
                    t_key = (person_id, prev_ev["company_name"], curr_ev["company_name"], to_date)

                    if t_key not in existing_turnovers:
                        cur.execute("""
                        INSERT INTO turnover_histories (
                            person_id, from_company, to_company, from_dept, to_dept,
                            gap_days, turnover_date, confidence_score
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            person_id,
                            prev_ev["company_name"],
                            curr_ev["company_name"],
                            prev_ev["department_raw"],
                            curr_ev["department_raw"],
                            gap,
                            curr_ev["event_date"],
                            conf
                        ))
                        existing_turnovers.add(t_key)
                        new_turnovers_count += 1
                        self.logger.info(
                            f"  [이직 감지] {name}: {prev_ev['company_name']}({prev_ev['department_raw']}) "
                            f"-> {curr_ev['company_name']}({curr_ev['department_raw']}) "
                            f"(공백: {gap}일, 신뢰도: {conf})"
                        )

            # 최신 프로필 마스터 갱신
            last_ev = events[-1]
            needs_review = 1 if len(events) > 8 and not has_turnover else 0

            cur.execute("""
            INSERT OR REPLACE INTO person_profiles (
                person_id, name, current_company, current_department, 
                current_position, last_event_date, needs_review
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                person_id,
                name,
                last_ev["company_name"],
                last_ev["department_raw"],
                last_ev["position_raw"],
                last_ev["event_date"],
                needs_review
            ))
            profiles_updated += 1

        conn.commit()
        conn.close()

        self.logger.info(
            f"[Step 3 완료] 인물 프로필 갱신: {profiles_updated}명, "
            f"신규 이직 감지: {new_turnovers_count}건"
        )
        return {
            "profiles_updated": profiles_updated,
            "new_turnovers": new_turnovers_count
        }

    def _evaluate_turnover(self, prev_ev: Dict[str, Any], curr_ev: Dict[str, Any]) -> Tuple[bool, float, int]:
        """두 발령 이벤트 간 언론사 이동 및 이직 부합 여부를 검증합니다."""
        from_company = prev_ev["company_name"]
        to_company = curr_ev["company_name"]

        # 동일 언론사는 사내 이동(전보/승진)이므로 배제
        if from_company == to_company:
            return False, 0.0, 0

        # 날짜 간격 계산
        try:
            d_prev = datetime.strptime(str(prev_ev["event_date"])[:10], "%Y-%m-%d")
            d_curr = datetime.strptime(str(curr_ev["event_date"])[:10], "%Y-%m-%d")
            gap_days = (d_curr - d_prev).days
        except Exception:
            return False, 0.0, 0

        if gap_days <= 0:
            return False, 0.0, 0

        # 조건 1: 180일(6개월) 윈도우 규칙
        if gap_days > 180:
            return False, 0.0, gap_days

        # 조건 2: 부서 카테고리 유사도 검사 (>= 0.75)
        dept_sim = self._calculate_dept_similarity(
            prev_ev.get("dept_category", ""),
            curr_ev.get("dept_category", "")
        )
        if dept_sim < 0.75:
            return False, 0.0, gap_days

        # 조건 3: 직급 서열 점수 (동등 이상 승진 및 유지)
        prev_rank = prev_ev.get("rank_score", 1)
        curr_rank = curr_ev.get("rank_score", 1)
        if curr_rank < prev_rank:
            return False, 0.0, gap_days

        # 신뢰도 점수 산출
        base_confidence = 0.85
        if dept_sim == 1.00:
            base_confidence += 0.10
        if curr_rank > prev_rank:
            base_confidence += 0.05

        return True, min(round(base_confidence, 2), 1.0), gap_days

    def _calculate_dept_similarity(self, cat1: str, cat2: str) -> float:
        """두 부서 카테고리 간 의미적 유사도를 0.0~1.0 사이로 계산합니다."""
        if not cat1 or not cat2 or cat1 == "미분류" or cat2 == "미분류":
            return 0.75  # 미표기 부서의 경우 기본 임계치 통과 허용

        if cat1 == cat2:
            return 1.00

        pair1 = (cat1, cat2)
        pair2 = (cat2, cat1)
        if pair1 in CATEGORY_SIMILARITY:
            return CATEGORY_SIMILARITY[pair1]
        if pair2 in CATEGORY_SIMILARITY:
            return CATEGORY_SIMILARITY[pair2]

        return 0.20

    # =========================================================================
    # 전체 순차 실행 (Chaining Pipeline)
    # =========================================================================
    def run_full_pipeline(self, max_pages: int = 2) -> Dict[str, Any]:
        """Step 1 -> Step 2 -> Step 3를 하나의 체인으로 순차 실행하고 요약을 반환합니다."""
        start_time = time.time()
        today_str = datetime.now().strftime("%Y-%m-%d")

        self.logger.info("=" * 65)
        self.logger.info(f"[{today_str}] 4대 매체 일일 자동 반영 파이프라인 가동")
        self.logger.info(f"데이터베이스: {self.db_path}")
        self.logger.info("=" * 65)

        # 1. 증분 수집
        fetch_stats = self.fetch_incremental(max_pages=max_pages)

        # 2. 미파싱 기사 파싱
        events_count = self.parse_unprocessed()

        # 3. 이직 추적 및 프로필 갱신
        turnover_stats = self.track_turnover()

        elapsed = time.time() - start_time

        # 요약 콘솔 출력 명세
        summary_msg = (
            f"[{today_str}] 연합: {fetch_stats['연합뉴스']}건, "
            f"뉴스1: {fetch_stats['뉴스1']}건, "
            f"중앙: {fetch_stats['중앙일보']}건, "
            f"기자협회: {fetch_stats['한국기자협회']}건 동기화 완료 "
            f"(신규 발령: {events_count}건, 이직 감지: {turnover_stats['new_turnovers']}건, 소요: {elapsed:.2f}초)"
        )

        print("\n" + "=" * 65)
        print(summary_msg)
        print("=" * 65 + "\n")
        self.logger.info(summary_msg)

        return {
            "date": today_str,
            "fetch": fetch_stats,
            "events_parsed": events_count,
            "turnovers": turnover_stats,
            "elapsed_seconds": elapsed
        }


def print_scheduler_guide(python_exec: str, script_path: str, log_path: str):
    """Windows 작업 스케줄러 및 Linux/macOS cron 설정 가이드를 출력합니다."""
    print("\n" + "=" * 70)
    print("4대 매체 자동 동기화(스케줄러) 설정 가이드")
    print("=" * 70)

    print("\n1. Windows 작업 스케줄러 (Task Scheduler) 등록")
    print("   PowerShell 또는 관리자 권한 명령 프롬프트에서 아래 명령을 실행하세요.")
    print("   (매일 오전 06:30에 자동으로 신규 기사를 수집하고 이직을 반영합니다)")
    print("-" * 70)
    schtasks_cmd = (
        f'schtasks /create /tn "MediaPersonnelDailySync" '
        f'/tr "\\"{python_exec}\\" \\"{script_path}\\" --once" '
        f'/sc daily /st 06:30 /f'
    )
    print(schtasks_cmd)
    print("-" * 70)
    print("   * 등록 확인: schtasks /query /tn \"MediaPersonnelDailySync\"")
    print("   * 즉시 실행: schtasks /run /tn \"MediaPersonnelDailySync\"")
    print("   * 작업 삭제: schtasks /delete /tn \"MediaPersonnelDailySync\" /f")

    print("\n2. Linux / macOS crontab 등록")
    print("   'crontab -e' 실행 후 아래 라인을 추가하세요.")
    print("-" * 70)
    cron_line = f"30 6 * * * \"{python_exec}\" \"{script_path}\" --once >> \"{log_path}\" 2>&1"
    print(cron_line)
    print("-" * 70)
    print("=" * 70 + "\n")


def run_daemon_mode(sync_engine: MultiSourcePersonnelSync, run_at: str = "06:30"):
    """상주형 백그라운드 데몬 모드 (schedule 라이브러리 활용 또는 내장 타이머 루프)"""
    sync_engine.logger.info(f"[데몬 모드] 상주형 스케줄러 가동 (매일 {run_at} 자동 동기화)")

    # 시작 시 1회 즉시 실행
    sync_engine.run_full_pipeline()

    try:
        import schedule
        schedule.every().day.at(run_at).do(sync_engine.run_full_pipeline)
        sync_engine.logger.info(f"'schedule' 라이브러리 스케줄 등록 완료: 매일 {run_at}")
        while True:
            schedule.run_pending()
            time.sleep(30)
    except ImportError:
        sync_engine.logger.info(f"내장 타이머 루프로 동작합니다. 매일 {run_at} 실행 대기 중...")
        last_run_day = None
        while True:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            time_now = now.strftime("%H:%M")
            if time_now == run_at and last_run_day != today:
                last_run_day = today
                sync_engine.run_full_pipeline()
            time.sleep(30)


def main():
    parser = argparse.ArgumentParser(
        description="4대 매체(연합뉴스·뉴스1·중앙일보·한국기자협회) 일일 자동 동기화 파이프라인"
    )
    parser.add_argument("--once", action="store_true", default=True, help="1회 즉시 동기화 실행 후 종료 (기본값)")
    parser.add_argument("--daemon", action="store_true", help="상주형 백그라운드 데몬으로 계속 실행")
    parser.add_argument("--at", type=str, default="06:30", help="데몬 모드 실행 시각 (기본값: 06:30)")
    parser.add_argument("--max-pages", type=int, default=2, help="매체별 증분 탐색 최대 페이지 수 (기본값: 2)")
    parser.add_argument("--backfill", action="store_true", help="한국기자협회 아카이브 전수 백필 실행")
    parser.add_argument("--guide", action="store_true", help="스케줄러 설정 안내 출력")
    parser.add_argument("--db-path", type=str, default=DB_NAME, help="SQLite 데이터베이스 경로")

    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_abs = os.path.join(base_dir, args.db_path) if not os.path.isabs(args.db_path) else args.db_path
    log_abs = os.path.join(base_dir, LOG_FILE)
    script_abs = os.path.abspath(__file__)
    python_exec = sys.executable

    if args.guide:
        print_scheduler_guide(python_exec, script_abs, log_abs)
        return 0

    logger = setup_logger(log_abs)
    sync_engine = MultiSourcePersonnelSync(db_path=db_abs, logger=logger)

    if args.backfill:
        logger.info("한국기자협회 아카이브 전수 백필을 시작합니다.")
        JournalistCrawler.crawl_backfill(db_path=db_abs, reverse=True, logger=logger)
        sync_engine.parse_unprocessed(batch_size=5000)
        sync_engine.track_turnover()
        return 0

    if args.daemon:
        run_daemon_mode(sync_engine, run_at=args.at)
        return 0

    # 기본: 1회 실행
    sync_engine.run_full_pipeline(max_pages=args.max_pages)
    return 0


if __name__ == "__main__":
    sys.exit(main())
