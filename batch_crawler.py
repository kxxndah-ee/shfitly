import sqlite3
import time
import random
import re
import os
import json
from datetime import datetime, timedelta
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup

# --- 1. 설정 및 상수 ---
DB_NAME = "media_personnel.db"
CHECKPOINT_FILE = "crawl_checkpoint.json"

MEDIA_TARGETS = {
    "연합뉴스": "001",
    "뉴스1": "421"
}

# 언론사 화이트리스트 (일반 사기업/관공서 인사 배제용, 더벨 등 주요 매체 포함)
MEDIA_WHITELIST = [
    "조선일보", "중앙일보", "동아일보", "한겨레", "경향신문", "한국일보", "문화일보", "서울신문", "세계일보", "국민일보",
    "매일경제", "한국경제", "서울경제", "이데일리", "머니투데이", "헤럴드경제", "아시아경제", "파이낸셜뉴스", "디지털타임스", "전자신문",
    "KBS", "MBC", "SBS", "YTN", "연합뉴스TV", "JTBC", "TV조선", "채널A", "MBN",
    "연합뉴스", "뉴스1", "뉴시스", "노컷뉴스", "오마이뉴스", "프레시안", "시사IN", "시사저널", "주간조선", "주간동아",
    "더벨", "비즈워치", "비즈니스워치", "뉴스토마토", "데일리안", "아이뉴스24", "뉴데일리", "아시아투데이", "이투데이", "뉴스핌"
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]


# --- 2. 체크포인트 관리 (이어받기 기능) ---
def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # 기본값: 40년 전(1986년)부터 시작
    return {"last_year": 1986, "last_month": 1, "completed": False}

def save_checkpoint(year: int, month: int):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_year": year, "last_month": month, "completed": False}, f, ensure_ascii=False, indent=2)


# --- 3. 데이터베이스 초기화 ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_date ON raw_articles(publish_date);")
    conn.commit()
    conn.close()


# --- 4. 요청 및 재시도 유틸리티 (Exponential Backoff) ---
def fetch_with_retry(url: str, params: Optional[dict] = None, max_retries: int = 5) -> Optional[requests.Response]:
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://search.naver.com/"
        }
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=12)
            if resp.status_code == 200:
                return resp
            elif resp.status_code in [429, 500, 502, 503, 504]:
                print(f"  [경고] HTTP {resp.status_code} 수신 (시도 {attempt}/{max_retries}). {delay:.1f}초 대기 후 재시도...")
                time.sleep(delay)
                delay *= 2  # 지수 백오프
            else:
                print(f"  [오류] HTTP {resp.status_code} 불가: {url}")
                return None
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            print(f"  [네트워크 오류] {e} (시도 {attempt}/{max_retries}). {delay:.1f}초 대기...")
            time.sleep(delay)
            delay *= 2

    print(f"  [실패] 최대 재시도 횟수 초과: {url}")
    return None


# --- 5. 언론사 필터링 및 본문 스크래퍼 ---
def identify_target_media(title: str, content_preview: str) -> Tuple[bool, str]:
    text = f"{title} {content_preview}"
    for media in MEDIA_WHITELIST:
        pattern = rf"(\[|<|◇|▲|\b){re.escape(media)}(\]|>|\s|인사)"
        if re.search(pattern, text):
            return True, media
    return False, ""

def fetch_article_detail(naver_url: str) -> Tuple[str, str]:
    resp = fetch_with_retry(naver_url)
    if not resp:
        return "", ""

    soup = BeautifulSoup(resp.text, "html.parser")
    content_tag = soup.select_one("#newsct_article") or soup.select_one("#articleBodyContents")
    content = content_tag.get_text(separator="\n", strip=True) if content_tag else ""

    date_tag = soup.select_one(".media_end_head_info_datestamp_time") or soup.select_one(".t11")
    date_str = ""
    if date_tag and date_tag.has_attr("data-date-time"):
        date_str = date_tag["data-date-time"].split()[0]
    elif date_tag:
        match = re.search(r"\d{4}\.\d{2}\.\d{2}", date_tag.text)
        if match:
            date_str = match.group().replace(".", "-")

    return content, date_str


# --- 6. 월 단위 수집 배치 코어 ---
def crawl_month_batch(conn: sqlite3.Connection, media_source: str, press_code: str, year: int, month: int):
    # 해당 월의 시작일과 마지막 일 계산
    start_dt = datetime(year, month, 1)
    if month == 12:
        end_dt = datetime(year, 12, 31)
    else:
        end_dt = datetime(year, month + 1, 1) - timedelta(days=1)

    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    cur = conn.cursor()
    start_index = 1
    total_saved = 0

    while True:
        # 네이버 뉴스 검색: [인사] 키워드 & 날짜 기간 지정
        search_url = "https://search.naver.com/search.naver"
        params = {
            "where": "news",
            "query": '"인사"',
            "sort": "1",  # 1: 오래된순 (과거부터 누락없이 차례대로 적재)
            "nso": f"so:dd,p:from{start_str}to{end_str}",
            "start": start_index
        }

        resp = fetch_with_retry(search_url, params=params)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.select("ul.list_news > li")

        if not articles:
            break

        for article in articles:
            # 1) 해당 언론사(연합뉴스/뉴스1) 필터
            press_tag = article.select_one("a.info.press")
            press_url = press_tag["href"] if press_tag and press_tag.has_attr("href") else ""
            if f"/press/{press_code}" not in press_url:
                continue

            # 2) 네이버 인링크 추출
            naver_links = [a["href"] for a in article.select("div.info_group a") if "news.naver.com" in a.get("href", "")]
            if not naver_links:
                continue

            naver_url = naver_links[0]
            article_id = re.sub(r"[^a-zA-Z0-9]", "", naver_url.split("?")[0])[-30:]

            # 중복 체크
            cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (article_id,))
            if cur.fetchone():
                continue

            title_tag = article.select_one("a.news_tit")
            title = title_tag.get_text(strip=True) if title_tag else ""

            # [인사] 또는 <인사> 제목 패턴 엄격 체크
            if not (("[인사]" in title) or ("<인사>" in title)):
                continue

            # 3) 본문 상세 수집
            content, pub_date = fetch_article_detail(naver_url)
            if not content:
                continue
            if not pub_date:
                pub_date = f"{year}-{month:02d}-01"

            # 4) 언론사 인사 기사 판별
            is_media, target_media = identify_target_media(title, content[:300])

            cur.execute("""
            INSERT OR REPLACE INTO raw_articles 
            (article_id, media_source, title, content, publish_date, target_media, is_media_company)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (article_id, media_source, title, content, pub_date, target_media, 1 if is_media else 0))

            total_saved += 1
            media_badge = f"[언론사:{target_media}]" if is_media else "[일반기관/제외]"
            print(f"    ✓ ({pub_date}) {media_badge} {title[:32]}...")

            # 요청 간 레이트 리미트
            time.sleep(random.uniform(0.3, 0.7))

        conn.commit()

        # 다음 페이지 존재 여부 확인
        next_page = soup.select_one("a.btn_next")
        if not next_page or next_page.get("aria-disabled") == "true" or start_index >= 4000:
            break

        start_index += 10
        time.sleep(random.uniform(0.5, 1.0))

    return total_saved


# --- 7. 메인 실행 파이프라인 ---
def run_batch_crawler():
    init_db()
    checkpoint = load_checkpoint()

    start_year = checkpoint["last_year"]
    start_month = checkpoint["last_month"]
    current_year = 2026
    current_month = datetime.now().month

    conn = sqlite3.connect(DB_NAME)

    print(f"=====================================================")
    print(f" 언론사 인사 기사 40년 전수 배치 크롤러 가동")
    print(f" 대상: 연합뉴스(001), 뉴스1(421) | 기간: {start_year}년 {start_month}월 ~ {current_year}년 {current_month}월")
    print(f" 체크포인트: {CHECKPOINT_FILE}")
    print(f"=====================================================\n")

    try:
        for year in range(start_year, current_year + 1):
            month_limit = current_month if year == current_year else 12
            month_begin = start_month if year == start_year else 1

            for month in range(month_begin, month_limit + 1):
                print(f"\n▶ [{year}년 {month:02d}월] 배치 수집 시작...")
                month_total = 0

                for media_source, press_code in MEDIA_TARGETS.items():
                    # 뉴스1은 2011년 설립되었으므로 2011년 이전은 스킵
                    if media_source == "뉴스1" and year < 2011:
                        continue

                    saved = crawl_month_batch(conn, media_source, press_code, year, month)
                    month_total += saved

                print(f"▶ [{year}년 {month:02d}월] 완료: 총 {month_total}건 저장됨.")
                save_checkpoint(year, month)

                # 월 단위 완료 시 휴식
                time.sleep(random.uniform(1.0, 2.0))

        # 전체 완료 시 플래그 갱신
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_year": current_year, "last_month": current_month, "completed": True}, f)
        print("\n최근 40년 치 전수 수집 배치가 모두 성공적으로 완료되었습니다.")

    except KeyboardInterrupt:
        print("\n[사용자 중단] 진행 상황이 체크포인트 파일에 저장되었습니다. 다시 실행하면 이어서 수집합니다.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_batch_crawler()
