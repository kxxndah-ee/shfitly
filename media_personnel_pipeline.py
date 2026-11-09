"""
media_personnel_pipeline.py

연합뉴스 및 뉴스1 인사 기사 전수 수집, 텍스트 파싱,
기자/언론인 이직(Turnover) 판정 알고리즘 및 커리어 프로필 생성 파이프라인.
"""

import os
import sys
import re
import json
import time
import random
import sqlite3
import gc
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# =========================================================================
# 1. 설정 및 상수 정의
# =========================================================================

DB_NAME = "media_personnel.db"

MEDIA_TARGETS = {
    "연합뉴스": "001",
    "뉴스1": "421"
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
}

# 언론사 화이트리스트 (전국 종합일간지, 통신사, 방송사, 경제지, IT/전문지, 주요 인터넷신문)
MEDIA_WHITELIST = [
    # 통신사
    "연합뉴스", "뉴스1", "뉴시스",
    # 종합일간지
    "조선일보", "중앙일보", "동아일보", "한겨레", "경향신문", "한국일보",
    "국민일보", "세계일보", "서울신문", "문화일보",
    # 경제지
    "매일경제", "한국경제", "서울경제", "이데일리", "머니투데이", "헤럴드경제",
    "아시아경제", "파이낸셜뉴스", "디지털타임스", "전자신문", "비즈니스워치",
    "아주경제", "뉴스토마토", "데일리안", "아이뉴스24", "뉴데일리", "아시아투데이",
    # 방송사
    "KBS", "MBC", "SBS", "YTN", "연합뉴스TV", "JTBC", "TV조선", "채널A", "MBN",
    # 인터넷/전문지
    "오마이뉴스", "프레시안", "미디어오늘", "시사인", "더팩트", "지디넷코리아"
]

# 비언론사 제외 키워드 (공기업, 금융, 관공서, 기업 등)
NON_MEDIA_EXCLUSIONS = [
    "은행", "증권", "카드", "캐피탈", "보험", "지주", "공단", "공사", "재단",
    "시청", "구청", "군청", "도청", "경찰청", "검찰청", "법원", "부처", "위원회",
    "병원", "대학", "연구원", "협회", "연맹", "학회", "포스코", "현대", "삼성", "LG", "SK"
]

# 직급별 서열 점수 (Rank Score) 매핑
RANK_SCORE_MAP = {
    "수습기자": 1, "인턴": 1,
    "기자": 2, "평기자": 2, "사원": 2,
    "차장대우": 3, "전문기자": 3, "대리": 3,
    "차장": 4, "과장": 4,
    "부장대우": 5, "팀장": 5, "선임기자": 5,
    "부장": 6, "센터장": 6, "팀장급": 6, "부국장대우": 6,
    "부국장": 7, "에디터": 7, "부실장": 7,
    "국장대우": 8, "국장": 8, "논설위원": 8, "해설위원": 8, "실장": 8,
    "본부장": 9, "이사": 9, "상무": 9, "전무": 9, "주필": 9,
    "부사장": 10, "사장": 10, "대표": 10, "대표이사": 10, "회장": 10
}

# 부서 정규화 카테고리 정의
DEPT_CATEGORIES = {
    "정치": ["정치", "청와대", "대통령실", "국회", "정당", "외교안보"],
    "경제/산업": [
        "경제", "산업", "금융", "증권", "부동산", "건설", "유통", "중기",
        "생활경제", "소비자", "마켓", "비즈니스", "재계", "건설부동산"
    ],
    "사회": ["사회", "사건", "법조", "경찰", "노동", "복지", "환경", "교육", "전국", "지역"],
    "IT/과학": ["IT", "과학", "디지털", "테크", "통신", "게임", "바이오", "AI", "인공지능"],
    "문화/연예": ["문화", "연예", "스포츠", "엔터", "라이프", "출판", "레저", "스타"],
    "국제": ["국제", "글로벌", "해외", "외신", "동북아", "미주"],
    "논설/오피니언": ["논설", "오피니언", "사설", "주필", "칼럼"],
    "편집/제작": ["편집", "교열", "제작", "영상", "사진", "그래픽", "방송", "디자인"],
    "경영/기획": ["기획", "경영", "전략", "총무", "인사", "재무", "회계", "마케팅", "사업", "광고"]
}

# 부서 카테고리 간 유사도 행렬 (기본값: 동일 1.0, 유사 0.8, 기타 0.2)
CATEGORY_SIMILARITY = {
    ("정치", "사회"): 0.80,
    ("정치", "국제"): 0.80,
    ("정치", "논설/오피니언"): 0.85,
    ("경제/산업", "IT/과학"): 0.80,
    ("경제/산업", "논설/오피니언"): 0.80,
    ("사회", "문화/연예"): 0.75,
    ("IT/과학", "디지털/편집"): 0.75,
    ("논설/오피니언", "사회"): 0.80
}


# =========================================================================
# 2. 데이터베이스 매니저 (Database Manager)
# =========================================================================

class DatabaseManager:
    """SQLite 데이터베이스 스키마 및 CRUD 연산을 전담하는 매니저 클래스"""

    def __init__(self, db_path: str = DB_NAME):
        self.db_path = db_path
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """테이블 4종 및 색인 생성 (DDL 명세 준수)"""
        with self.get_connection() as conn:
            cur = conn.cursor()

            # 1. 수집 기사 원본 테이블
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

            # 2. 파싱된 인사 발령 개별 레코드 테이블
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

            # 3. 통합 인물 마스터 테이블
            cur.execute("""
            CREATE TABLE IF NOT EXISTS person_profiles (
                person_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                current_company TEXT,
                current_department TEXT,
                current_position TEXT,
                last_event_date DATE,
                needs_review BOOLEAN DEFAULT 0
            );
            """)

            # 4. 판정된 이직(Turnover) 이력 테이블
            cur.execute("""
            CREATE TABLE IF NOT EXISTS turnover_histories (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id TEXT NOT NULL,
                from_company TEXT NOT NULL,
                to_company TEXT NOT NULL,
                from_dept TEXT,
                to_dept TEXT,
                gap_days INTEGER NOT NULL,
                turnover_date DATE NOT NULL,
                confidence_score REAL DEFAULT 1.0,
                FOREIGN KEY (person_id) REFERENCES person_profiles(person_id)
            );
            """)

            cur.execute("CREATE INDEX IF NOT EXISTS idx_articles_date ON raw_articles(publish_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_lookup ON career_events(person_name, event_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_company ON career_events(company_name, event_date);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_turnover_person ON turnover_histories(person_id);")

            conn.commit()


# =========================================================================
# 3. 언론사 식별 및 정규화 엔진 (Media Classifier)
# =========================================================================

class MediaClassifier:
    """기사 제목 및 본문 첫머리에서 발령 대상 언론사를 식별하고 필터링"""

    @classmethod
    def identify_target_media(cls, title: str, content_preview: str) -> Tuple[bool, str]:
        """
        일반 기업/관공서가 아닌 오직 언론사 본사 및 계열사 인사인지 여부와 언론사명을 판별합니다.
        """
        search_text = f"{title} {content_preview}"

        # 1. 비언론사 제외 키워드 검사 (금융, 공공기관, 대기업 우선 배제)
        for excl in NON_MEDIA_EXCLUSIONS:
            if re.search(rf"\b{re.escape(excl)}\b", title):
                return False, ""

        # 2. 언론사 화이트리스트 매칭 (특수문자 및 괄호 패턴 인식)
        for media in MEDIA_WHITELIST:
            pattern = rf"(\[|<|◇|▲|\b){re.escape(media)}(\]|>|\s|인사)"
            if re.search(pattern, search_text):
                return True, media

        # 3. 추가 패턴: 기사 제목이 '[인사] 언론사명' 형식인 경우 매칭
        title_match = re.search(r"[\[<](?:인사)[\]>]\s*([가-힣A-Za-z0-9]+)", title)
        if title_match:
            candidate = title_match.group(1).strip()
            for media in MEDIA_WHITELIST:
                if candidate.startswith(media) or media.startswith(candidate):
                    return True, media

        return False, ""


# =========================================================================
# 4. 텍스트 파싱 및 엔티티 정규화 엔진 (Parsing & Normalization)
# =========================================================================

class PersonnelTextParser:
    """인사 기사 본문의 다양한 포맷을 정형화된 레코드로 분해"""

    @classmethod
    def categorize_department(cls, dept_raw: str) -> str:
        """원시 부서명을 표준 9대 부서 카테고리로 정규화"""
        if not dept_raw:
            return "기타/일반"

        for cat, keywords in DEPT_CATEGORIES.items():
            for kw in keywords:
                if kw in dept_raw:
                    return cat
        return "기타/일반"

    @classmethod
    def calculate_rank_score(cls, position_raw: str) -> int:
        """직급 문자열을 분석하여 1~10점의 Rank Score를 반환"""
        if not position_raw:
            return 2  # 기본값: 평기자 수준

        matched_score = 2
        for rank_kw, score in RANK_SCORE_MAP.items():
            if rank_kw in position_raw:
                matched_score = max(matched_score, score)

        return matched_score

    @classmethod
    def parse_article(
        cls,
        article_id: str,
        company_name: str,
        content: str,
        event_date: str
    ) -> List[Dict[str, Any]]:
        """
        인사 기사 본문에서 발령 대상자(이름, 부서, 직급, 발령구분)를 전수 추출합니다.
        지원 포맷:
          - ◇ 승진 ▲ 정치부장 홍길동 ▲ 사회부 차장 김철수
          - [편집국] 정치부 차장 김철수
          - ▲ 경제부장(부국장대우) 박지영
        """
        records = []
        if not content:
            return records

        current_action = "발령"
        current_dept_prefix = ""

        # 줄 단위 분해
        lines = content.splitlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 1. 발령 구분 감지 (◇ 승진, 【전보】, <선임> 등)
            action_match = re.search(r"[◇【<\[](승진|전보|보임|선임|신규|파견|복귀|보직|승급|면직)[】>\]]", line)
            if action_match:
                current_action = action_match.group(1)

            # 2. 부서 대분류 접두사 감지 (예: [편집국], [경영총괄본부])
            dept_prefix_match = re.search(r"^[\[【]([가-힣\s]{2,12})[\]】]", line)
            if dept_prefix_match:
                cand_pfx = dept_prefix_match.group(1).strip()
                if cand_pfx not in ["인사", "프로필", "알림"]:
                    current_dept_prefix = cand_pfx

            # 3. 인명 및 발령 단위 항목 분리 (▲, •, - 기호 또는 쉼표 기준)
            items = re.split(r"[▲•■●○▶]\s*", line)
            for item in items:
                item = item.strip()
                if not item or len(item) < 3:
                    continue

                # '부서 및 직급 성명' 형태 파싱
                # 예: '정치부장 홍길동', '사회부 차장 김철수', '건설부동산부 부장 박지영'
                # 정규식: 마지막 2~4글자 한글 이름을 추출
                match = re.search(
                    r"^(?P<dept_pos>[가-힣A-Za-z0-9\s\(\)·/]+?)\s+(?P<name>[가-힣]{2,4})$",
                    item
                )
                if not match:
                    continue

                dept_pos = match.group("dept_pos").strip()
                person_name = match.group("name").strip()

                # 직급 추출 (부장대우, 부장, 차장, 기자, 국장 등)
                position_raw = ""
                dept_raw = dept_pos

                for rk in sorted(RANK_SCORE_MAP.keys(), key=lambda x: -len(x)):
                    if rk in dept_pos:
                        position_raw = rk
                        # 부서명에서 직급 제거
                        dept_raw = dept_pos.replace(rk, "").strip()
                        break

                if not position_raw:
                    position_raw = "기자"

                if current_dept_prefix and current_dept_prefix not in dept_raw:
                    full_dept = f"{current_dept_prefix} {dept_raw}".strip()
                else:
                    full_dept = dept_raw or "본사"

                dept_cat = cls.categorize_department(full_dept)
                rank_score = cls.calculate_rank_score(position_raw)

                records.append({
                    "article_id": article_id,
                    "person_name": person_name,
                    "company_name": company_name,
                    "department_raw": full_dept,
                    "dept_category": dept_cat,
                    "position_raw": position_raw,
                    "rank_score": rank_score,
                    "action_type": current_action,
                    "event_date": event_date
                })

        return records


# =========================================================================
# 5. 핵심 비즈니스 로직: 이직(Turnover) 판정 알고리즘
# =========================================================================

class TurnoverDetector:
    """
    동명이인 문제를 최소화하며 상태 변화, 180일 윈도우, 부서 유사도,
    직급 서열 점수를 기반으로 이직(Turnover)을 판정하는 핵심 엔진.
    """

    @classmethod
    def calculate_dept_similarity(cls, dept_cat1: str, dept_cat2: str) -> float:
        """두 부서 카테고리 간의 의미적 유사도 점수 반환 (0.0 ~ 1.0)"""
        if not dept_cat1 or not dept_cat2:
            return 0.50

        if dept_cat1 == dept_cat2:
            return 1.00

        pair1 = (dept_cat1, dept_cat2)
        pair2 = (dept_cat2, dept_cat1)

        if pair1 in CATEGORY_SIMILARITY:
            return CATEGORY_SIMILARITY[pair1]
        if pair2 in CATEGORY_SIMILARITY:
            return CATEGORY_SIMILARITY[pair2]

        return 0.30

    @classmethod
    def evaluate_turnover(
        cls,
        prev_event: Dict[str, Any],
        curr_event: Dict[str, Any]
    ) -> Tuple[bool, float, int]:
        """
        두 연속된 발령 이벤트 간의 이직 조건 만족 여부를 검증합니다.
        
        조건 1: 이전 언론사와 현재 언론사가 다를 것 (Exclusion & New Appearance)
        조건 2: 이전 발령일 대비 현재 발령일 간격이 180일(6개월) 이내일 것 (Window Condition)
        조건 3: 부서 유사도가 0.75 이상일 것 (Department Similarity)
        조건 4: 직급 서열 점수(Rank Score)가 같거나 높을 것 (Rank Score Condition)
        
        반환: (is_turnover, confidence_score, gap_days)
        """
        from_company = prev_event["company_name"]
        to_company = curr_event["company_name"]

        # 동일 언론사 내의 이동은 이직이 아님 (사내 승진/전보)
        if from_company == to_company:
            return False, 0.0, 0

        # 날짜 간격 계산
        d_prev = datetime.strptime(str(prev_event["event_date"])[:10], "%Y-%m-%d")
        d_curr = datetime.strptime(str(curr_event["event_date"])[:10], "%Y-%m-%d")
        gap_days = (d_curr - d_prev).days

        if gap_days <= 0:
            return False, 0.0, 0

        # 조건 2: 180일(6개월) 이내 윈도우 규칙
        if gap_days > 180:
            return False, 0.0, gap_days

        # 조건 3: 부서 카테고리 유사도 검사 (>= 0.75)
        dept_sim = cls.calculate_dept_similarity(
            prev_event.get("dept_category", ""),
            curr_event.get("dept_category", "")
        )
        if dept_sim < 0.75:
            return False, 0.0, gap_days

        # 조건 4: 직급 서열 점수 (동등 이상 승진 및 유지)
        prev_rank = prev_event.get("rank_score", 1)
        curr_rank = curr_event.get("rank_score", 1)
        if curr_rank < prev_rank:
            return False, 0.0, gap_days

        # 신뢰도 점수 산출
        base_confidence = 0.85
        if dept_sim == 1.00:
            base_confidence += 0.10
        if curr_rank > prev_rank:
            base_confidence += 0.05
        confidence = min(round(base_confidence, 2), 1.0)

        return True, confidence, gap_days


# =========================================================================
# 6. 인물 프로필 빌더 (Person Profile Builder & JSON Schema)
# =========================================================================

class PersonProfileBuilder:
    """수집/파싱된 발령 이벤트와 이직 이력을 바탕으로 인물 마스터 및 JSON 프로필 생성"""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def build_all_profiles(self) -> int:
        """career_events 테이블을 집계하여 person_profiles 및 turnover_histories 구축"""
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # 고유 인물 목록 추출
            cur.execute("SELECT DISTINCT person_name FROM career_events ORDER BY person_name")
            names = [row["person_name"] for row in cur.fetchall()]

            profile_count = 0

            for idx, name in enumerate(names, 1):
                person_id = f"P_{idx:05d}"

                # 해당 인물의 모든 발령 이벤트를 시간순 조회
                cur.execute("""
                SELECT * FROM career_events 
                WHERE person_name = ? 
                ORDER BY event_date ASC, rank_score ASC
                """, (name,))
                events = [dict(r) for r in cur.fetchall()]

                if not events:
                    continue

                # 1. 이직(Turnover) 이벤트 판정 루프
                has_turnover = False
                for i in range(len(events) - 1):
                    prev_ev = events[i]
                    curr_ev = events[i + 1]

                    is_turnover, confidence, gap_days = TurnoverDetector.evaluate_turnover(prev_ev, curr_ev)
                    if is_turnover:
                        has_turnover = True
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
                            gap_days,
                            curr_ev["event_date"],
                            confidence
                        ))

                # 2. 최신 발령 상태 추출
                last_event = events[-1]
                needs_review = 1 if len(events) > 8 and not has_turnover else 0

                cur.execute("""
                INSERT OR REPLACE INTO person_profiles (
                    person_id, name, current_company, current_department, 
                    current_position, last_event_date, needs_review
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    person_id,
                    name,
                    last_event["company_name"],
                    last_event["department_raw"],
                    last_event["position_raw"],
                    last_event["event_date"],
                    needs_review
                ))

                profile_count += 1

            conn.commit()
            return profile_count

    def generate_person_profile_json(self, person_id_or_name: str) -> Optional[Dict[str, Any]]:
        """명세에 부합하는 정형 JSON 프로필 객체를 생성하여 반환합니다."""
        with self.db.get_connection() as conn:
            cur = conn.cursor()

            cur.execute("""
            SELECT * FROM person_profiles 
            WHERE person_id = ? OR name = ?
            LIMIT 1
            """, (person_id_or_name, person_id_or_name))
            p_row = cur.fetchone()

            if not p_row:
                return None

            person = dict(p_row)
            p_id = person["person_id"]

            # 발령 이벤트 조회
            cur.execute("""
            SELECT * FROM career_events 
            WHERE person_name = ? 
            ORDER BY event_date ASC
            """, (person["name"],))
            raw_events = [dict(r) for r in cur.fetchall()]

            # 이직 이력 조회
            cur.execute("""
            SELECT * FROM turnover_histories 
            WHERE person_id = ? 
            ORDER BY turnover_date ASC
            """, (p_id,))
            turnovers = {r["turnover_date"]: dict(r) for r in cur.fetchall()}

            # 타임라인 구성 (APPOINTMENT 및 TURNOVER)
            career_timeline = []
            max_confidence = 0.90

            for ev in raw_events:
                ev_date = str(ev["event_date"])[:10]
                if ev_date in turnovers:
                    to_info = turnovers[ev_date]
                    career_timeline.append({
                        "date": ev_date,
                        "media": ev["company_name"],
                        "department": ev["department_raw"],
                        "position": ev["position_raw"],
                        "event_type": "TURNOVER",
                        "transition_from": to_info["from_company"],
                        "gap_days": to_info["gap_days"]
                    })
                    max_confidence = max(max_confidence, to_info["confidence_score"])
                else:
                    career_timeline.append({
                        "date": ev_date,
                        "media": ev["company_name"],
                        "department": ev["department_raw"],
                        "position": ev["position_raw"],
                        "event_type": "APPOINTMENT"
                    })

            profile_json = {
                "person_id": person["person_id"],
                "name": person["name"],
                "confidence_score": max_confidence,
                "current_status": {
                    "media": person["current_company"],
                    "department": person["current_department"],
                    "position": person["current_position"],
                    "last_updated": person["last_event_date"]
                },
                "career_timeline": career_timeline
            }

            return profile_json


# =========================================================================
# 7. 기사 수집 파이프라인 (Data Collection)
# =========================================================================

class PersonnelCrawler:
    """연합뉴스 및 뉴스1 타깃 네이버 뉴스 검색 수집기"""

    def __init__(self, db: DatabaseManager):
        self.db = db
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=15, pool_maxsize=15)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def fetch_article_detail(self, naver_article_url: str) -> Tuple[str, str]:
        """기사 상세 페이지에서 본문 전문 및 발행 일자를 추출"""
        try:
            resp = self.session.get(naver_article_url, headers=HEADERS, timeout=8, verify=False)
            if resp.status_code != 200:
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
        except Exception as e:
            return "", ""

    def crawl_range(self, start_date_str: str, end_date_str: str) -> int:
        """일자 범위를 순회하며 연합뉴스/뉴스1의 [인사] 기사를 전수 수집"""
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        current_date = start_date

        collected_count = 0

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            while current_date <= end_date:
                date_query = current_date.strftime("%Y%m%d")
                date_hyphen = current_date.strftime("%Y-%m-%d")

                for media_name, press_code in MEDIA_TARGETS.items():
                    start_index = 1
                    while True:
                        search_url = "https://search.naver.com/search.naver"
                        params = {
                            "where": "news",
                            "query": "[인사]",
                            "sort": "0",
                            "nso": f"so:r,p:from{date_query}to{date_query}",
                            "start": start_index
                        }

                        try:
                            resp = self.session.get(search_url, headers=HEADERS, params=params, timeout=8, verify=False)
                            if resp.status_code != 200:
                                break

                            soup = BeautifulSoup(resp.text, "html.parser")
                            articles = soup.select("ul.list_news > li")
                            if not articles:
                                break

                            for art in articles:
                                press_tag = art.select_one("a.info.press")
                                press_url = press_tag["href"] if press_tag and press_tag.has_attr("href") else ""
                                if f"/press/{press_code}" not in press_url:
                                    continue

                                naver_links = [
                                    a["href"] for a in art.select("div.info_group a") 
                                    if "news.naver.com" in a.get("href", "")
                                ]
                                if not naver_links:
                                    continue

                                naver_url = naver_links[0]
                                article_id = re.sub(r"[^a-zA-Z0-9]", "", naver_url.split("?")[0])[-30:]

                                # 중복 검사
                                cur.execute("SELECT 1 FROM raw_articles WHERE article_id = ?", (article_id,))
                                if cur.fetchone():
                                    continue

                                title_tag = art.select_one("a.news_tit")
                                title = title_tag.get_text(strip=True) if title_tag else ""

                                content, pub_date = self.fetch_article_detail(naver_url)
                                pub_date = pub_date or date_hyphen

                                if not content:
                                    continue

                                is_media, target_media = MediaClassifier.identify_target_media(title, content[:300])

                                cur.execute("""
                                INSERT OR REPLACE INTO raw_articles (
                                    article_id, media_source, title, content, 
                                    publish_date, target_media, is_media_company
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    article_id,
                                    media_name,
                                    title,
                                    content,
                                    pub_date,
                                    target_media,
                                    1 if is_media else 0
                                ))
                                conn.commit()
                                collected_count += 1

                            next_page = soup.select_one("a.btn_next")
                            if not next_page or next_page.get("aria-disabled") == "true":
                                break

                            start_index += 10
                            time.sleep(random.uniform(0.2, 0.4))
                        except Exception:
                            break

                current_date += timedelta(days=1)

        return collected_count


# =========================================================================
# 8. 파이프라인 오케스트레이터 및 실행 진입점
# =========================================================================

class MediaPersonnelPipeline:
    """수집 -> 파싱 -> 이직 판정 -> 프로필 생성을 아우르는 종합 파이프라인"""

    def __init__(self, db_path: str = DB_NAME):
        self.db = DatabaseManager(db_path)
        self.crawler = PersonnelCrawler(self.db)
        self.builder = PersonProfileBuilder(self.db)

    def process_raw_articles(self) -> int:
        """수집된 raw_articles 중 미처리된 언론사 인사 기사를 파싱하여 career_events로 저장"""
        total_events = 0

        with self.db.get_connection() as conn:
            cur = conn.cursor()

            # 오직 언론사 필터를 통과한 기사(is_media_company = 1)만 선별
            cur.execute("""
            SELECT article_id, target_media, content, publish_date 
            FROM raw_articles 
            WHERE is_media_company = 1
            """)
            rows = cur.fetchall()

            for r in rows:
                company = r["target_media"] or "언론사"
                events = PersonnelTextParser.parse_article(
                    article_id=r["article_id"],
                    company_name=company,
                    content=r["content"],
                    event_date=r["publish_date"]
                )

                for ev in events:
                    cur.execute("""
                    INSERT INTO career_events (
                        article_id, person_name, company_name, department_raw,
                        dept_category, position_raw, rank_score, action_type, event_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ev["article_id"],
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

            conn.commit()

        return total_events

    def run_turnover_and_profile_build(self) -> int:
        """이직 판정 알고리즘 구동 및 인물 프로필 전체 구축"""
        return self.builder.build_all_profiles()

    def get_person_profile_json(self, name_or_id: str) -> Optional[Dict[str, Any]]:
        """명세 형식의 인물 프로필 JSON 조회"""
        return self.builder.generate_person_profile_json(name_or_id)


# =========================================================================
# 9. 단위 검증 및 시뮬레이션
# =========================================================================

def run_self_verification_test():
    """파이프라인 전 과정(수집 기사 삽입, 파싱, 180일 이직 판정, 프로필 JSON 생성) 기능 검증"""
    test_db = "test_media_personnel.db"
    if os.path.exists(test_db):
        os.remove(test_db)

    pipeline = MediaPersonnelPipeline(test_db)

    # 1. 가상 언론사 인사 기사 데이터 삽입 (매일경제 -> 한국경제 112일 만의 이직 시나리오)
    sample_article_1 = (
        "art_001",
        "연합뉴스",
        "[인사] 매일경제",
        "◇ 매일경제\n◇ 승진\n▲ 경제부 차장 홍길동\n▲ 금융부 차장 박철수",
        "2022-01-10",
        "매일경제",
        1
    )
    sample_article_2 = (
        "art_002",
        "뉴스1",
        "[인사] 한국경제",
        "◇ 한국경제\n◇ 승진 및 전보\n▲ 산업부 부장 홍길동\n▲ IT과학부 차장 강수진",
        "2022-05-02",  # 2022-01-10 대비 112일 경과 (180일 이내)
        "한국경제",
        1
    )

    with pipeline.db.get_connection() as conn:
        cur = conn.cursor()
        for art in [sample_article_1, sample_article_2]:
            cur.execute("""
            INSERT INTO raw_articles (
                article_id, media_source, title, content, publish_date, target_media, is_media_company
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, art)
        conn.commit()

    # 2. 텍스트 파싱 및 career_events 적재
    events_count = pipeline.process_raw_articles()
    print(f"파싱 완료된 인사 발령 레코드 수: {events_count}")

    # 3. 이직 판정 및 프로필 빌드
    profiles_count = pipeline.run_turnover_and_profile_build()
    print(f"생성된 인물 프로필 수: {profiles_count}")

    # 4. JSON 프로필 생성 결과 검증
    profile_json = pipeline.get_person_profile_json("홍길동")
    print("\n--- '홍길동' 인물 프로필 JSON 출력 결과 ---")
    print(json.dumps(profile_json, ensure_ascii=False, indent=2))

    # 5. 검증 assertion
    assert profile_json is not None, "프로필이 생성되지 않았습니다."
    assert profile_json["name"] == "홍길동"
    assert profile_json["current_status"]["media"] == "한국경제"
    assert profile_json["current_status"]["position"] == "부장"
    assert len(profile_json["career_timeline"]) == 2
    assert profile_json["career_timeline"][1]["event_type"] == "TURNOVER"
    assert profile_json["career_timeline"][1]["transition_from"] == "매일경제"
    assert profile_json["career_timeline"][1]["gap_days"] == 112

    print("\n모든 단위 검증 및 명세 요구사항 테스트가 100% 통과했습니다.")

    del pipeline
    gc.collect()
    time.sleep(0.1)
    if os.path.exists(test_db):
        try:
            os.remove(test_db)
        except Exception:
            pass


if __name__ == "__main__":
    run_self_verification_test()
