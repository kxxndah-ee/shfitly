"""
archive_backfill.py
===================
중앙일보, 연합뉴스, 뉴스1, 한국기자협회 4대 매체의 아카이브 전체 기간 인사 기사를
안정적이고 누락 없이 대량 수집하여 raw_articles에 적재하고,
미파싱 기사를 전수 파싱하여 media_personnel.db 및 media_tracker.db에 즉시 동기화합니다.
"""

import os
import sys
import re
import time
import random
import sqlite3
import html
from datetime import datetime
from typing import List, Dict, Any, Set, Tuple
from urllib.parse import urljoin, quote

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from data_pipeline import (
    MediaDatabase,
    PersonnelTrackerEngine,
    PersonnelRegexParser,
    normalize_media_name,
    is_media_organization,
    is_excluded_department,
    is_personnel_article_title,
    KNOWN_MEDIA_NAMES,
    MEDIA_GROUP_ALIASES
)
from parser_pipeline import parse_article_content

DB_NAME = "media_personnel.db"
TRACKER_DB = "media_tracker.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

session = requests.Session()
session.headers.update(HEADERS)


def init_raw_db():
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_media ON raw_articles(media_source);")
    conn.commit()
    conn.close()


def save_raw_article(conn: sqlite3.Connection, article_id: str, media_source: str, title: str, content: str, pub_date: str, target_media: str) -> bool:
    if not article_id or not content or len(content) < 20:
        return False
    cur = conn.cursor()
    try:
        cur.execute("""
        INSERT INTO raw_articles (article_id, media_source, title, content, publish_date, target_media, is_media_company)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(article_id) DO UPDATE SET
            content = excluded.content,
            publish_date = excluded.publish_date,
            target_media = excluded.target_media,
            is_media_company = 1
        """, (article_id, media_source, title, content, pub_date, target_media))
        conn.commit()
        return True
    except Exception as e:
        return False


# =========================================================================
# 1. 중앙일보 아카이브 수집기 (joongang.co.kr/people/personnel)
# =========================================================================
def crawl_joongang_archive(conn: sqlite3.Connection, max_pages: int = 30) -> int:
    print(f"\n[중앙일보] 아카이브 전체 기간 수집 시작 (최대 {max_pages}페이지)...")
    saved_count = 0
    cur = conn.cursor()

    for page in range(1, max_pages + 1):
        url = f"https://www.joongang.co.kr/people/personnel?page={page}"
        try:
            resp = session.get(url, timeout=8, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            cards = soup.select(".card_body a, h2.headline a, a[href*='/article/']")
            if not cards:
                print(f"  중앙일보 {page}페이지: 기사 없음 (수집 종료)")
                break

            page_saved = 0
            for a in cards:
                href = a.get("href", "")
                title = a.get_text(strip=True)
                if "/article/" not in href or not title or len(title) < 3:
                    continue
                if not ("[인사]" in title or "<인사>" in title or "인사" in title):
                    continue

                full_url = href if href.startswith("http") else f"https://www.joongang.co.kr{href}"

                # 이미 수집되어 본문이 있는 경우 스킵
                cur.execute("SELECT length(content) FROM raw_articles WHERE article_id = ?", (full_url,))
                row = cur.fetchone()
                if row and row[0] and row[0] > 50:
                    continue

                try:
                    d_resp = session.get(full_url, timeout=8, verify=False)
                    if d_resp.status_code != 200:
                        continue
                    d_soup = BeautifulSoup(d_resp.content, "html.parser")
                    body = d_soup.select_one(".article_body, #article_body, div[itemprop='articleBody']")
                    content = body.get_text("\n", strip=True) if body else ""
                    if len(content) < 20:
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
                        date_str = datetime.now().strftime("%Y-%m-%d")

                    # 언론사명 판별
                    target_media = "중앙일보"
                    t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", title)
                    if t_match:
                        target_media = t_match.group(1).strip()

                    if save_raw_article(conn, full_url, "중앙일보", title, content, date_str, target_media):
                        saved_count += 1
                        page_saved += 1
                    time.sleep(0.3)
                except Exception:
                    pass

            print(f"  중앙일보 {page}페이지 완료: {page_saved}건 신규 적재 (누적 {saved_count}건)")
            time.sleep(0.5)
        except Exception as e:
            print(f"  중앙일보 {page}페이지 오류: {e}")
            break

    return saved_count


# =========================================================================
# 2. 연합뉴스 아카이브 수집기 (yna.co.kr/people/personnel)
# =========================================================================
def crawl_yonhap_archive(conn: sqlite3.Connection, max_pages: int = 30) -> int:
    print(f"\n[연합뉴스] 아카이브 전체 기간 수집 시작 (최대 {max_pages}페이지)...")
    saved_count = 0
    cur = conn.cursor()

    for page in range(1, max_pages + 1):
        url = f"https://www.yna.co.kr/people/personnel/{page}"
        try:
            resp = session.get(url, timeout=8, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            cards = soup.select("div.item-box01 a, ul.list a, a[href*='/view/AKR']")
            if not cards:
                print(f"  연합뉴스 {page}페이지: 기사 없음 (수집 종료)")
                break

            seen_page_hrefs = set()
            page_saved = 0
            for a in cards:
                href = a.get("href", "")
                title = a.get_text(strip=True)
                if not href or "/view/AKR" not in href or href in seen_page_hrefs:
                    continue
                seen_page_hrefs.add(href)

                full_url = href if href.startswith("http") else f"https:{href}" if href.startswith("//") else f"https://www.yna.co.kr{href}"

                cur.execute("SELECT length(content) FROM raw_articles WHERE article_id = ?", (full_url,))
                row = cur.fetchone()
                if row and row[0] and row[0] > 50:
                    continue

                try:
                    d_resp = session.get(full_url, timeout=8, verify=False)
                    if d_resp.status_code != 200:
                        continue
                    d_soup = BeautifulSoup(d_resp.content, "html.parser")
                    body = d_soup.select_one("article.story-news, div.story-news, #articleWrap")
                    content = body.get_text("\n", strip=True) if body else ""
                    if len(content) < 20:
                        continue

                    # 기사 제목 재확인
                    h_elem = d_soup.select_one("h1.tit-article, h1.tit")
                    if h_elem:
                        title = h_elem.get_text(strip=True)

                    if not ("[인사]" in title or "<인사>" in title or "인사" in title):
                        continue

                    # 날짜 추출
                    m_date = re.search(r"AKR(19[6-9]\d|20[0-3]\d)(\d{2})(\d{2})", full_url)
                    if m_date:
                        date_str = f"{m_date.group(1)}-{m_date.group(2)}-{m_date.group(3)}"
                    else:
                        date_str = datetime.now().strftime("%Y-%m-%d")

                    target_media = "연합뉴스"
                    t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", title)
                    if t_match:
                        target_media = t_match.group(1).strip()

                    if save_raw_article(conn, full_url, "연합뉴스", title, content, date_str, target_media):
                        saved_count += 1
                        page_saved += 1
                    time.sleep(0.3)
                except Exception:
                    pass

            print(f"  연합뉴스 {page}페이지 완료: {page_saved}건 신규 적재 (누적 {saved_count}건)")
            time.sleep(0.5)
        except Exception as e:
            print(f"  연합뉴스 {page}페이지 오류: {e}")
            break

    return saved_count


# =========================================================================
# 3. 뉴스1 아카이브 수집기 (news1.kr/life-culture/press & wire search)
# =========================================================================
def crawl_news1_archive(conn: sqlite3.Connection, max_pages: int = 15) -> int:
    print(f"\n[뉴스1] 아카이브 수집 시작...")
    saved_count = 0
    cur = conn.cursor()

    # 1. 뉴스1 공식 인사 섹션
    for page in range(1, max_pages + 1):
        url = f"https://www.news1.kr/life-culture/press/?page={page}" if page > 1 else "https://www.news1.kr/life-culture/press/"
        try:
            resp = session.get(url, timeout=8, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            nd_script = soup.find("script", id="__NEXT_DATA__")
            items = []
            if nd_script and nd_script.string:
                import json
                nd_data = json.loads(nd_script.string)
                sub_data = nd_data.get("props", {}).get("pageProps", {}).get("subsectionData", [])
                for it in sub_data:
                    art_id = it.get("id")
                    tit = html.unescape(it.get("title", "")).strip()
                    pub_d = str(it.get("pubdate", ""))[:10]
                    u = f"https://www.news1.kr/life-culture/press/{art_id}"
                    items.append((u, tit, pub_d))

            if not items:
                # fallback HTML
                for a in soup.select("a[href*='/life-culture/press/']"):
                    u = a.get("href", "")
                    tit = a.get_text(strip=True)
                    if u and len(tit) > 3:
                        full_u = u if u.startswith("http") else f"https://www.news1.kr{u}"
                        items.append((full_u, tit, ""))

            if not items:
                break

            page_saved = 0
            for u, tit, pub_d in items:
                cur.execute("SELECT length(content) FROM raw_articles WHERE article_id = ?", (u,))
                row = cur.fetchone()
                if row and row[0] and row[0] > 50:
                    continue

                try:
                    d_resp = session.get(u, timeout=8, verify=False)
                    if d_resp.status_code != 200:
                        continue
                    d_soup = BeautifulSoup(d_resp.content, "html.parser")
                    body = d_soup.select_one("#article_body, .article_content, div.detail")
                    content = body.get_text("\n", strip=True) if body else ""
                    if len(content) < 20:
                        continue

                    if not pub_d:
                        m_dt = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", d_resp.text[:1500])
                        pub_d = f"{m_dt.group(1)}-{int(m_dt.group(2)):02d}-{int(m_dt.group(3)):02d}" if m_dt else datetime.now().strftime("%Y-%m-%d")

                    target_media = "뉴스1"
                    t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", tit)
                    if t_match:
                        target_media = t_match.group(1).strip()

                    if save_raw_article(conn, u, "뉴스1", tit, content, pub_d, target_media):
                        saved_count += 1
                        page_saved += 1
                    time.sleep(0.3)
                except Exception:
                    pass

            print(f"  뉴스1 {page}페이지 완료: {page_saved}건 신규 적재 (누적 {saved_count}건)")
            time.sleep(0.5)
            # Next.js pagination duplicate defense
            if page > 1 and page_saved == 0:
                break
        except Exception as e:
            print(f"  뉴스1 {page}페이지 오류: {e}")
            break

    return saved_count


# =========================================================================
# 4. 한국기자협회보 아카이브 수집기 (sec_no=63)
# =========================================================================
def crawl_journalist_archive(conn: sqlite3.Connection, max_pages: int = 30) -> int:
    print(f"\n[한국기자협회보] 아카이브 수집 시작 (최대 {max_pages}페이지)...")
    saved_count = 0
    cur = conn.cursor()

    for page in range(1, max_pages + 1):
        url = f"https://www.journalist.or.kr/news/section_list_all.html?sec_no=63&page={page}"
        try:
            resp = session.get(url, timeout=8, verify=False)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.content, "html.parser")
            art_links = soup.select("ul.art_list li a.art_tit, td.art_tit a, a.tit")
            if not art_links:
                break

            page_saved = 0
            for a in art_links:
                href = a.get("href", "")
                title = a.get_text(strip=True)
                if not href or not title:
                    continue
                full_url = urljoin(url, href)

                cur.execute("SELECT length(content) FROM raw_articles WHERE article_id = ?", (full_url,))
                row = cur.fetchone()
                if row and row[0] and row[0] > 50:
                    continue

                try:
                    d_resp = session.get(full_url, timeout=8, verify=False)
                    if d_resp.status_code != 200:
                        continue
                    d_soup = BeautifulSoup(d_resp.content, "html.parser")
                    body = d_soup.select_one("div#news_body_area, div.art_txt, div.art_body")
                    content = body.get_text("\n", strip=True) if body else ""
                    if len(content) < 20:
                        continue

                    # 날짜 추출
                    m_d = re.search(r"(19[6-9]\d|20[0-3]\d)[.\-/](\d{1,2})[.\-/](\d{1,2})", d_resp.text[:2000])
                    date_str = f"{m_d.group(1)}-{int(m_d.group(2)):02d}-{int(m_d.group(3)):02d}" if m_d else datetime.now().strftime("%Y-%m-%d")

                    target_media = "한국기자협회"
                    t_match = re.search(r"[\[<]인사[\]>]\s*([가-힣A-Za-z0-9]+)", title)
                    if t_match:
                        target_media = t_match.group(1).strip()

                    if save_raw_article(conn, full_url, "한국기자협회", title, content, date_str, target_media):
                        saved_count += 1
                        page_saved += 1
                    time.sleep(0.3)
                except Exception:
                    pass

            print(f"  한국기자협회 {page}페이지 완료: {page_saved}건 신규 적재 (누적 {saved_count}건)")
            time.sleep(0.5)
        except Exception as e:
            print(f"  한국기자협회 {page}페이지 오류: {e}")
            break

    return saved_count


# =========================================================================
# 5. 미파싱 기사 전수 파싱 및 media_personnel.db & media_tracker.db 듀얼 동기화
# =========================================================================
def sync_and_parse_all():
    print("\n" + "=" * 65)
    print(" [Step 2: 수집된 원문 기사 전수 파싱 및 듀얼 DB 동기화]")
    print("=" * 65)

    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

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
    conn.commit()

    cur.execute("""
    SELECT r.article_id, r.media_source, r.title, r.content, r.publish_date, r.target_media
    FROM raw_articles r
    LEFT JOIN career_events c ON r.article_id = c.article_id
    WHERE r.is_media_company = 1
      AND c.event_id IS NULL
    """)
    unprocessed = cur.fetchall()
    print(f"새로 파싱할 원문 기사: 총 {len(unprocessed)}건")

    if not unprocessed:
        print("모든 기사가 이미 파싱되어 있습니다.")
        conn.close()
        return

    all_tracker_records = []
    total_events = 0

    for idx, art in enumerate(unprocessed, 1):
        art_id = art["article_id"]
        title = art["title"]
        content = art["content"]
        pub_date = str(art["publish_date"])[:10]
        media_src = art["media_source"]
        target_med = art["target_media"] or media_src or "언론사"

        # 1. parser_pipeline (media_personnel.db)
        try:
            evs = parse_article_content(content, target_med, pub_date)
            for ev in evs:
                cur.execute("""
                INSERT INTO career_events (
                    article_id, person_name, company_name, department_raw, 
                    dept_category, position_raw, rank_score, action_type, event_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    art_id,
                    ev["person_name"],
                    ev["company_name"],
                    ev["department_raw"],
                    ev["dept_category"],
                    ev["position_raw"],
                    ev["rank_score"],
                    ev["action_type"],
                    ev["event_date"]
                ))
                total_events += 1
        except Exception:
            pass

        # 2. data_pipeline (media_tracker.db UI용)
        try:
            rec_recs = PersonnelRegexParser.parse_article_text(
                article_text=content,
                default_media=target_med,
                article_title=title,
                pub_date=pub_date,
                article_url=art_id
            )
            for r in rec_recs:
                if is_media_organization(r.get("media", "")) and not is_excluded_department(r.get("dept", "")):
                    all_tracker_records.append(r)
        except Exception:
            pass

        if idx % 100 == 0:
            conn.commit()
            print(f"  진행률: {idx}/{len(unprocessed)} 기사 파싱 완료...")

    conn.commit()
    conn.close()

    print(f"\n[Step 2 완료] career_events 신규 적재: 총 {total_events}건")
    print(f"media_tracker.db 동기화 대상 발령 레코드: 총 {len(all_tracker_records)}건")

    if all_tracker_records:
        try:
            engine = PersonnelTrackerEngine(TRACKER_DB)
            eng_res = engine.process_records(all_tracker_records, append=True)
            print(f"media_tracker.db 동기화 성공: 신규 {eng_res.get('new_records', 0)}건, 누적 언론인 {eng_res.get('unique_reporters', 0)}명, 이직 감지 {eng_res.get('job_changes_detected', 0)}건")
        except Exception as e:
            print(f"media_tracker.db 동기화 오류: {e}")


def main():
    print("=" * 65)
    print(" [4대 매체: 중앙일보·연합뉴스·뉴스1·기자협회 아카이브 전수 수집 및 분석 파이프라인]")
    print("=" * 65)

    init_raw_db()
    conn = sqlite3.connect(DB_NAME)

    # 1. 중앙일보 아카이브 (최대 30페이지, 약 720건)
    crawl_joongang_archive(conn, max_pages=30)

    # 2. 연합뉴스 아카이브 (최대 30페이지, 약 1,200건)
    crawl_yonhap_archive(conn, max_pages=30)

    # 3. 뉴스1 아카이브 (최신 섹션 및 백필)
    crawl_news1_archive(conn, max_pages=15)

    # 4. 한국기자협회보 (전문 언론사 인사 섹션 30페이지)
    crawl_journalist_archive(conn, max_pages=30)

    conn.close()

    # 5. 전수 파싱 및 듀얼 DB 동기화
    sync_and_parse_all()

    print("\n아카이브 전수 수집 및 분석 파이프라인이 성공적으로 완료되었습니다!")


if __name__ == "__main__":
    main()
