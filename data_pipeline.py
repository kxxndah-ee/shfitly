"""
data_pipeline.py
================
언론사 인사 발령 기사 데이터 파이프라인 및 엔지니어링 모듈.
- 빅카인즈(BIG Kinds) 엑셀/CSV 파서
- 한국 언론사 인사 기사 전용 정규표현식(Regex) 고속 추출기
- 직급 체계 및 출입처 도메인 유사도 매핑
- 동명이인 분리 클러스터링 (1순위 이메일 바이라인, 2순위 휴리스틱 스코어링)
- 매체 간 이직(Job Change) 자동 탐지 엔진
- 네이버 뉴스 검색 API 기반 바이라인 이메일 동기화
- SQLite 데이터베이스 영속화
"""

import os
import re
import sqlite3
import datetime
from typing import List, Dict, Tuple, Optional, Any
import pandas as pd
import requests


# ==========================================
# 1. 직급 서열(Rank) 및 부서 도메인(Domain) 사전
# ==========================================

RANK_HIERARCHY: Dict[str, int] = {
    "회장": 10,
    "대표이사": 10,
    "대표": 10,
    "사장": 10,
    "부사장": 9,
    "전무": 9,
    "상무": 9,
    "이사": 9,
    "주필": 9,
    "논설주간": 9,
    "편집국장": 8,
    "보도국장": 8,
    "논설위원실장": 8,
    "국장": 8,
    "본부장": 8,
    "부국장": 7,
    "부국장대우": 7,
    "에디터": 7,
    "논설위원": 7,
    "해설위원": 7,
    "전문위원": 7,
    "부장": 6,
    "팀장(부장급)": 6,
    "데스크": 6,
    "지국장": 6,
    "부장대우": 5,
    "차장": 4,
    "팀장": 4,
    "차장대우": 3,
    "선임기자": 3,
    "전문기자": 3,
    "특파원": 3,
    "기자": 2,
    "평기자": 2,
    "수습기자": 1,
    "인턴": 1,
}

# 정규표현식 매칭을 위한 직급 패턴 (긴 단어부터 매칭되도록 정렬)
SORTED_RANKS = sorted(RANK_HIERARCHY.keys(), key=lambda x: len(x), reverse=True)
RANK_REGEX_PATTERN = r"|".join([re.escape(r) for r in SORTED_RANKS])

DOMAIN_TAXONOMY: Dict[str, List[str]] = {
    "부동산/건설": [
        "건설부동산", "부동산", "건설", "도시", "도시인프라", "주택", "토목", "인프라"
    ],
    "경제/산업/금융": [
        "경제", "산업", "금융", "증권", "유통", "중기", "벤처", "중기벤처", "마켓",
        "자본시장", "재정", "통상", "에너지", "모빌리티", "자동차", "소비자", "생활경제",
        "공기업", "기업"
    ],
    "정치/외교": [
        "정치", "청와대", "대통령실", "국회", "정당", "외교", "안보", "통일", "국방",
        "외교안보", "통일외교"
    ],
    "사회/법조": [
        "사회", "법조", "경찰", "검찰", "법원", "시민사회", "전국", "지방", "사건",
        "기동취재", "안전", "환경", "노동", "복지", "보건"
    ],
    "IT/과학/바이오": [
        "it", "과학", "바이오", "제약", "디지털", "테크", "정보미디어", "통신", "인공지능",
        "ai", "플랫폼", "소프트웨어"
    ],
    "문화/스포츠": [
        "문화", "스포츠", "연예", "엔터", "대중문화", "생활", "여가", "스타일", "종교", "출판"
    ],
    "국제/글로벌": [
        "국제", "글로벌", "해외", "외신", "미국", "중국", "일본", "유럽"
    ],
    "기획/논설/편집": [
        "논설", "기획", "탐사", "편집", "교열", "디자인", "미디어랩", "어젠다"
    ],
}

# 도메인 간 친화도 (이직 시 유사 분야 판단용, 1.0 = 동일, 0.7 = 친화적, 0.1 = 무관)
DOMAIN_AFFINITY: Dict[Tuple[str, str], float] = {
    ("부동산/건설", "경제/산업/금융"): 0.85,
    ("경제/산업/금융", "부동산/건설"): 0.85,
    ("경제/산업/금융", "IT/과학/바이오"): 0.70,
    ("IT/과학/바이오", "경제/산업/금융"): 0.70,
    ("정치/외교", "사회/법조"): 0.75,
    ("사회/법조", "정치/외교"): 0.75,
    ("사회/법조", "경제/산업/금융"): 0.50,
    ("경제/산업/금융", "사회/법조"): 0.50,
    ("문화/스포츠", "IT/과학/바이오"): 0.20,
    ("문화/스포츠", "부동산/건설"): 0.10,
}

STOPWORDS = {
    "인사", "발령", "본사", "지사", "편집국", "보도국", "신문", "방송", "뉴스", "한국", "일보",
    "매일", "경제", "대표", "국장", "부장", "차장", "팀장", "기자", "위원", "실장", "총괄",
    "대우", "승진", "전보", "보임", "신규", "선임", "퇴임", "파견", "내정", "전보자",
    "외", "등", "겸", "및", "간", "후보", "부국장"
}


def classify_domain(dept_name: str) -> str:
    """부서명을 분석하여 상위 도메인 카테고리를 반환합니다."""
    if not dept_name:
        return "일반"
    dept_lower = dept_name.lower().strip()
    for domain, keywords in DOMAIN_TAXONOMY.items():
        for kw in keywords:
            if kw in dept_lower:
                return domain
    return "일반"


def calculate_domain_similarity(d1: str, d2: str) -> float:
    """두 도메인 간의 친화도(0.0 ~ 1.0)를 산출합니다."""
    if not d1 or not d2:
        return 0.3
    if d1 == d2:
        return 1.0
    pair = (d1, d2)
    if pair in DOMAIN_AFFINITY:
        return DOMAIN_AFFINITY[pair]
    rev_pair = (d2, d1)
    if rev_pair in DOMAIN_AFFINITY:
        return DOMAIN_AFFINITY[rev_pair]
    return 0.2


def normalize_media_name(raw_name: str) -> str:
    """언론사명 뒤의 법인명, 신문사, 접미사를 표준 매체명으로 정규화합니다."""
    if not raw_name:
        return "미상"
    name = str(raw_name).strip()
    name = re.sub(r"\(주\)|주식회사|신문사|신문|일보사|방송사|미디어그룹", "", name).strip()
    if name.endswith("사") and len(name) >= 3 and not name.endswith("통신사"):
        name = name[:-1]
    
    # 주요 언론사 매핑 규칙
    if "조선" in name: return "조선일보"
    if "중앙" in name: return "중앙일보"
    if "동아" in name: return "동아일보"
    if "한국경제" in name: return "한국경제"
    if "매일경제" in name: return "매일경제"
    if "서울경제" in name: return "서울경제"
    if "헤럴드" in name: return "헤럴드경제"
    if "연합" in name: return "연합뉴스"
    if "뉴스1" in name or "뉴스원" in name: return "뉴스1"
    if "뉴시스" in name: return "뉴시스"
    if "한국일보" in name: return "한국일보"
    if "경향" in name: return "경향신문"
    if "한겨레" in name: return "한겨레"
    if "문화" in name and "일보" in raw_name: return "문화일보"
    if "세계" in name and "일보" in raw_name: return "세계일보"
    if "국민" in name and "일보" in raw_name: return "국민일보"
    return name


def parse_rank_level(rank_title: str) -> int:
    """직급 텍스트에서 가장 높은 직급 서열(1~10)을 반환합니다."""
    if not rank_title:
        return 2  # 기본값: 평기자 수준
    best_level = 2
    for r, level in RANK_HIERARCHY.items():
        if r in rank_title:
            if level > best_level or best_level == 2:
                best_level = level
    return best_level


# ==========================================
# 2. 한국 언론사 인사 기사 전용 정규표현식 파서
# ==========================================

class PersonnelRegexParser:
    """
    한국 언론사의 전형적인 인사 발령 기사 텍스트에서
    (언론사, 부서, 직급, 이름, 이메일, 발령구분)을 고속 추출하는 정규표현식 엔진.
    """

    # 구분 기호 및 불릿 문자
    BULLET_CHARS = r"[△▲▷▶○●■◆◇◎·\-\*]"
    
    # 2~4글자 한글 인명 패턴
    NAME_PATTERN = r"[가-힣]{2,4}"

    # 이메일 추출 패턴
    EMAIL_PATTERN = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"

    # 발령 액션 헤더 패턴 (예: <승진>, ◇전보, [국장급] 등)
    ACTION_HEADER_PATTERN = re.compile(
        r"(?:<|◇|◆|\[|\(|【)(승진|전보|보직|보임|선임|신규|신규선임|전출|전입|파견|퇴임|위촉)(?:>|◇|◆|\]|\)|】)"
    )

    # 미디어 헤더 패턴 (한 기사 내 여러 언론사 구분용)
    MEDIA_HEADER_PATTERN = re.compile(
        r"(?:◆|◇|■|◎|●|【|\[)([가-힣0-9A-Za-z]+(?:일보|경제|뉴스|신문|타임스|미디어|데일리|헤럴드|통신|방송|TV|mbc|kbs|sbs|ytn|jtbc|뉴스1|뉴시스|연합뉴스))(?:】|\]|◆|◇|■|◎|●)?"
    )

    @classmethod
    def clean_name(cls, name_candidate: str) -> Optional[str]:
        """추출된 이름 후보가 유효한 한글 인명인지 정제 및 필터링."""
        name = name_candidate.strip()
        if len(name) < 2 or len(name) > 4:
            return None
        if not re.fullmatch(r"[가-힣]{2,4}", name):
            return None
        if name in STOPWORDS:
            return None
        return name

    @classmethod
    def parse_article_text(
        cls,
        article_text: str,
        default_media: str = "",
        article_title: str = "",
        pub_date: str = ""
    ) -> List[Dict[str, Any]]:
        """
        기사 본문 전체를 순회하며 인사 발령 레코드 리스트를 추출합니다.
        """
        results: List[Dict[str, Any]] = []
        if not article_text:
            return results

        # 제목에서 언론사 힌트 파악 (예: [인사] 뉴스1)
        current_media = normalize_media_name(default_media)
        title_media_match = re.search(r"\[인사\]\s*([가-힣A-Za-z0-9]+)", article_title)
        if title_media_match:
            cand = title_media_match.group(1).strip()
            if len(cand) >= 2 and cand not in ["정부", "외", "공기업", "언론사", "종합"]:
                current_media = normalize_media_name(cand)

        current_action = "발령"
        current_section_rank = ""

        # 줄 단위 또는 불릿 기호 단위로 분할 처리
        lines = [line.strip() for line in article_text.splitlines() if line.strip()]

        for line in lines:
            # 1. 언론사 변경 감지 (기사 중간에 다른 언론사 헤더가 나올 경우)
            media_match = cls.MEDIA_HEADER_PATTERN.search(line)
            if media_match:
                cand_media = media_match.group(1).strip()
                if len(cand_media) >= 2 and cand_media not in ["승진", "전보", "보임", "언론사", "종합"]:
                    current_media = normalize_media_name(cand_media)

            # 2. 발령 액션 감지 (<승진>, ◇전보 등)
            action_match = cls.ACTION_HEADER_PATTERN.search(line)
            if action_match:
                current_action = action_match.group(1).strip()

            # 3. 섹션 직급 헤더 감지 (예: <부장급>, [국장급], <차장>)
            section_rank_match = re.search(r"(?:<|\[|\()([가-힣]+(?:급|대우)?)(?:>|\]|\))", line)
            if section_rank_match:
                cand_rank = section_rank_match.group(1).replace("급", "").strip()
                if cand_rank in RANK_HIERARCHY:
                    current_section_rank = cand_rank

            # 4. 불릿 기호 기준으로 개별 발령 항목 쪼개기
            # 예: △건설부동산부장 김희준 △정치부 차장 박철수
            items = re.split(cls.BULLET_CHARS, line)
            for item in items:
                item = item.strip()
                if not item or len(item) < 3:
                    continue

                # 이메일 추출
                found_email = ""
                email_match = re.search(cls.EMAIL_PATTERN, item)
                if email_match:
                    found_email = email_match.group(0).strip()
                    item_clean = re.sub(cls.EMAIL_PATTERN, "", item).replace("()", "").strip()
                else:
                    item_clean = item

                # 괄호 내 한자나 부가정보 제거: 예: 김희준(金熙俊) -> 김희준
                item_clean = re.sub(r"\([가-힣\w\s·]+\)", "", item_clean).strip()

                # 서브 아이템 파싱 (쉼표로 나열된 경우: 예: 건설부동산부 부장 김희준, 차장 박영수)
                sub_records = cls._parse_single_bullet(
                    item_clean, current_media, current_action, current_section_rank, found_email
                )
                for rec in sub_records:
                    rec["pub_date"] = pub_date
                    results.append(rec)

        return results

    @classmethod
    def _parse_single_bullet(
        cls,
        text: str,
        media: str,
        action: str,
        section_rank: str,
        email: str
    ) -> List[Dict[str, Any]]:
        """
        단일 불릿 텍스트(예: '건설부동산부장 김희준' 또는 '사회부 차장 박철수, 부장 최영희') 파싱.
        """
        records: List[Dict[str, Any]] = []

        # 콤마로 여러 명이 나열된 경우 분할
        parts = [p.strip() for p in text.split(",") if p.strip()]
        last_dept = ""

        for part in parts:
            tokens = part.split()
            if not tokens:
                continue

            extracted_name = None
            extracted_rank = section_rank or "기자"
            extracted_dept = last_dept or "본사"

            # 케이스 A: 3개 이상 토큰 (예: "건설부동산부 부장 김희준" or "논설위원실 논설위원 이재명")
            if len(tokens) >= 3:
                cand_name = cls.clean_name(tokens[-1])
                if cand_name:
                    extracted_name = cand_name
                    extracted_dept = tokens[0]
                    extracted_rank = " ".join(tokens[1:-1])
                    last_dept = extracted_dept

            # 케이스 B: 2개 토큰 (예: "건설부동산부장 김희준" or "차장 박철수" or "정치부 홍길동")
            elif len(tokens) == 2:
                cand_name = cls.clean_name(tokens[1])
                first_token = tokens[0]

                if cand_name:
                    extracted_name = cand_name
                    # first_token 안에 부서와 직급이 결합된 경우 파싱 (예: "건설부동산부장")
                    rank_match = re.search(f"({RANK_REGEX_PATTERN})$", first_token)
                    if rank_match:
                        extracted_rank = rank_match.group(1)
                        extracted_dept = first_token[:rank_match.start()].strip() or "본사"
                    elif first_token in RANK_HIERARCHY:
                        extracted_rank = first_token
                        extracted_dept = last_dept or "본사"
                    else:
                        extracted_dept = first_token
                        extracted_rank = section_rank or "기자"
                    last_dept = extracted_dept

            # 케이스 C: 1개 토큰 (예: "건설부동산부장김희준" 처럼 붙어있는 경우)
            elif len(tokens) == 1:
                single_str = tokens[0]
                rank_match = re.search(f"({RANK_REGEX_PATTERN})([가-힣]{{2,4}})$", single_str)
                if rank_match:
                    r_text = rank_match.group(1)
                    n_text = rank_match.group(2)
                    cand_name = cls.clean_name(n_text)
                    if cand_name:
                        extracted_name = cand_name
                        extracted_rank = r_text
                        extracted_dept = single_str[:rank_match.start()].strip() or last_dept or "본사"
                        last_dept = extracted_dept

            if extracted_name:
                # 불필요한 수식어구 정리
                clean_dept = re.sub(r"^[△▲◇◆■◎\s]+", "", extracted_dept).strip()
                clean_rank = re.sub(r"^[△▲◇◆■◎\s]+", "", extracted_rank).strip()
                if not clean_rank:
                    clean_rank = "기자"

                rank_level = parse_rank_level(clean_rank)
                domain = classify_domain(clean_dept)

                records.append({
                    "media": media,
                    "dept": clean_dept,
                    "domain": domain,
                    "rank_title": clean_rank,
                    "rank_level": rank_level,
                    "name": extracted_name,
                    "action_type": action,
                    "email": email,
                    "raw_text": part
                })

        return records


# ==========================================
# 3. 동명이인 분리 클러스터링 & 이직 탐지 엔진
# ==========================================

class PersonnelTrackerEngine:
    """
    발령 기록들을 모아 동명이인을 분리하고 고유 식별자(reporter_id)를 부여하며,
    매체 간 이직(Job Change)을 자동 탐지하여 타임라인을 구성하는 통합 엔진.
    """

    def __init__(self, db_path: str = "media_tracker.db"):
        self.db = MediaDatabase(db_path)

    def process_records(self, raw_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        추출된 모든 레코드를 시계열 순서로 정렬한 후,
        1순위 이메일 매칭 및 2순위 알고리즘 휴리스틱을 적용하여 클러스터링을 수행합니다.
        """
        if not raw_records:
            return {"total_records": 0, "unique_reporters": 0, "job_changes_detected": 0}

        # 날짜순 정렬 (과거 -> 현재)
        def parse_date_key(r):
            d_str = str(r.get("pub_date", "1970-01-01")).replace("-", "").replace(".", "")
            if len(d_str) >= 8:
                return d_str[:8]
            return "19700101"

        sorted_records = sorted(raw_records, key=parse_date_key)

        # 동명이인 클러스터 컨테이너: Dict[name, List[ReporterCluster]]
        clusters_by_name: Dict[str, List[Dict[str, Any]]] = {}

        detected_job_changes: List[Dict[str, Any]] = []

        for record in sorted_records:
            name = record["name"]
            if not name:
                continue

            if name not in clusters_by_name:
                clusters_by_name[name] = []

            target_cluster = self._match_or_create_cluster(
                record=record,
                existing_clusters=clusters_by_name[name],
                job_changes_collector=detected_job_changes
            )

            # 레코드에 확정된 reporter_id 및 display_label 부여
            record["reporter_id"] = target_cluster["reporter_id"]

        # DB에 저장 및 동기화
        self.db.save_records_and_clusters(
            raw_records=sorted_records,
            clusters_by_name=clusters_by_name,
            job_changes=detected_job_changes
        )

        total_unique = sum(len(c_list) for c_list in clusters_by_name.values())
        return {
            "total_records": len(sorted_records),
            "unique_reporters": total_unique,
            "job_changes_detected": len(detected_job_changes)
        }

    def _match_or_create_cluster(
        self,
        record: Dict[str, Any],
        existing_clusters: List[Dict[str, Any]],
        job_changes_collector: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        단일 레코드에 대해 기존 클러스터 매칭 여부를 검증하고, 일치하는 클러스터가 없으면 신규 클러스터를 생성합니다.
        """
        name = record["name"]
        rec_media = normalize_media_name(record["media"])
        rec_dept = record["dept"]
        rec_domain = record["domain"]
        rec_rank = record["rank_title"]
        rec_level = record["rank_level"]
        rec_date_str = str(record.get("pub_date", "2020-01-01"))
        rec_email = record.get("email", "").strip()

        rec_email_prefix = ""
        if rec_email and "@" in rec_email:
            rec_email_prefix = rec_email.split("@")[0].lower()

        try:
            rec_date = datetime.datetime.strptime(
                rec_date_str.replace(".", "-").split("T")[0][:10], "%Y-%m-%d"
            ).date()
        except Exception:
            rec_date = datetime.date(2020, 1, 1)

        best_cluster = None
        best_score = -999.0
        is_job_change_candidate = False
        change_metadata = None

        for cluster in existing_clusters:
            # 1. 타임라인 중복 방지 (Timeline Conflict Exclusion)
            # 동일/근접 기간(30일 이내)에 서로 다른 매체에서 활동 기록이 있으면 배제
            conflict = False
            for prev_rec in cluster["records"]:
                p_date = prev_rec["date_obj"]
                p_media = normalize_media_name(prev_rec["media"])
                if abs((rec_date - p_date).days) <= 30 and p_media != rec_media:
                    conflict = True
                    break
            if conflict:
                continue

            score = 0.0
            last_rec = cluster["records"][-1]
            last_media = normalize_media_name(last_rec["media"])
            last_date = last_rec["date_obj"]
            last_level = last_rec["rank_level"]
            last_dept = last_rec["dept"]
            last_domain = last_rec["domain"]

            days_diff = (rec_date - last_date).days
            rank_diff = rec_level - last_level

            # ----------------------------------------------------
            # 1순위: 이메일 바이라인 매칭
            # ----------------------------------------------------
            c_email_prefix = cluster.get("email_prefix", "")
            has_same_email = bool(rec_email_prefix and c_email_prefix and rec_email_prefix == c_email_prefix)
            if has_same_email:
                score += 150.0  # 이메일 접두사 일치 시 절대적 가점

            # ----------------------------------------------------
            # 2순위: 알고리즘 휴리스틱 스코어링
            # ----------------------------------------------------
            # CASE A: 동일 언론사 내의 이동/승진
            if rec_media == last_media:
                score += 50.0
                if -1 <= rank_diff <= 2:
                    score += 30.0  # 정상 유지 또는 승진
                elif rank_diff < -1:
                    score -= 70.0  # 급격한 강등(비정상)
                elif rank_diff > 3:
                    score -= 40.0  # 비정상적 고속 승진

                domain_sim = calculate_domain_similarity(rec_domain, last_domain)
                score += domain_sim * 25.0

            # CASE B: 서로 다른 언론사 간의 이동 (이직 탐지 알고리즘 Step A, B, C)
            else:
                # Step B: 연간 정기인사 텀(450일) 또는 동일 이메일(최대 2년) 적용
                max_window = 730 if has_same_email else 450
                if -180 <= days_diff <= max_window:
                    time_score = max(5.0, 30.0 - (abs(days_diff) / 20.0))
                    score += time_score

                    # Step C: 직급 연속성 검증 (수평 이동 ±1 직급 또는 정상 승진 +2)
                    if -1 <= rank_diff <= 2:
                        score += 35.0
                    elif rank_diff < -1:
                        score -= 80.0
                    else:
                        score -= 40.0

                    domain_sim = calculate_domain_similarity(rec_domain, last_domain)
                    score += domain_sim * 40.0

                    # 이직 판정 임계치 충족 여부 체크
                    if score >= 60.0 or has_same_email:
                        is_job_change = True
                        candidate_change_meta = {
                            "name": name,
                            "from_media": last_media,
                            "from_dept": last_dept,
                            "from_rank": last_rec["rank_title"],
                            "from_date": str(last_date),
                            "to_media": rec_media,
                            "to_dept": rec_dept,
                            "to_rank": rec_rank,
                            "to_date": str(rec_date),
                            "days_gap": days_diff,
                            "confidence_score": round(score, 1),
                            "detected_reason": (
                                f"직급 연속성(Δ={rank_diff}), 분야 유사도({int(domain_sim*100)}%), "
                                f"{'이메일 일치' if has_same_email else '타임라인 부합'}"
                            )
                        }
                    else:
                        is_job_change = False
                        candidate_change_meta = None
                else:
                    score -= 50.0
                    is_job_change = False
                    candidate_change_meta = None

            if score > best_score:
                best_score = score
                best_cluster = cluster
                is_job_change_candidate = is_job_change if rec_media != last_media else False
                change_metadata = candidate_change_meta if rec_media != last_media else None

        # 매칭 임계치 (Threshold = 55.0점)
        if best_cluster is not None and best_score >= 55.0:
            # 기존 클러스터에 편입
            record_entry = {
                "media": rec_media,
                "dept": rec_dept,
                "domain": rec_domain,
                "rank_title": rec_rank,
                "rank_level": rec_level,
                "date_obj": rec_date,
                "raw_text": record.get("raw_text", ""),
                "action_type": record.get("action_type", "발령"),
                "email": rec_email
            }
            best_cluster["records"].append(record_entry)
            best_cluster["last_seen"] = rec_date
            best_cluster["current_media"] = rec_media
            best_cluster["current_dept"] = rec_dept
            best_cluster["current_rank"] = rec_rank
            if not best_cluster.get("email") and rec_email:
                best_cluster["email"] = rec_email
                best_cluster["email_prefix"] = rec_email_prefix

            # 이직 이벤트 발생 시 수집기에 등록
            if is_job_change_candidate and change_metadata:
                change_metadata["reporter_id"] = best_cluster["reporter_id"]
                job_changes_collector.append(change_metadata)
                record["action_type"] = f"이직 ({change_metadata['from_media']} ➔ {rec_media})"

            self._update_display_label(best_cluster)
            return best_cluster

        # 일치하는 클러스터가 없으면 신규 클러스터(동명이인 또는 첫 출현) 생성
        cluster_idx = len(existing_clusters) + 1
        dept_tag = re.sub(r"[^\w]", "", rec_dept)[:4] or "기자"
        email_tag = rec_email_prefix if rec_email_prefix else f"c{cluster_idx}"
        reporter_id = f"{name}_{email_tag}_{rec_media[:3]}"

        new_cluster = {
            "reporter_id": reporter_id,
            "name": name,
            "email": rec_email,
            "email_prefix": rec_email_prefix,
            "first_seen": rec_date,
            "last_seen": rec_date,
            "current_media": rec_media,
            "current_dept": rec_dept,
            "current_rank": rec_rank,
            "records": [{
                "media": rec_media,
                "dept": rec_dept,
                "domain": rec_domain,
                "rank_title": rec_rank,
                "rank_level": rec_level,
                "date_obj": rec_date,
                "raw_text": record.get("raw_text", ""),
                "action_type": record.get("action_type", "발령"),
                "email": rec_email
            }],
            "display_label": ""
        }
        self._update_display_label(new_cluster)
        existing_clusters.append(new_cluster)
        return new_cluster

    def _update_display_label(self, cluster: Dict[str, Any]):
        """드롭다운에서 동명이인을 한눈에 구분할 수 있는 세련된 표시명 생성."""
        name = cluster["name"]
        curr_media = cluster["current_media"]
        curr_dept = cluster["current_dept"]
        email = cluster.get("email", "")
        email_hint = f" / {email}" if email else ""
        first_rec = cluster["records"][0]
        origin_hint = f" ({first_rec['media']} 출신)" if first_rec["media"] != curr_media else ""
        cluster["display_label"] = f"{name} ({curr_media} {curr_dept}{origin_hint}{email_hint})"


# ==========================================
# 4. 네이버 뉴스 검색 API 기반 바이라인 이메일 수집
# ==========================================

class NaverNewsEmailFinder:
    """
    네이버 뉴스 검색 오픈 API(OpenAPI)를 활용해
    특정 기자의 바이라인 이메일을 능동적으로 수집·보완하는 클래스.
    """

    API_URL = "https://openapi.naver.com/v1/search/news.json"

    def __init__(self, client_id: str = "", client_secret: str = ""):
        self.client_id = client_id
        self.client_secret = client_secret

    def search_reporter_email(self, name: str, media: str) -> Optional[str]:
        """
        네이버 뉴스 API로 기자명과 언론사명을 검색하여 바이라인 이메일을 탐색합니다.
        API 키가 없거나 실패할 경우 None을 반환합니다.
        """
        if not self.client_id or not self.client_secret:
            return None

        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret
        }

        query = f'"{media}" "{name} 기자"'
        params = {
            "query": query,
            "display": 10,
            "sort": "date"
        }

        try:
            resp = requests.get(self.API_URL, headers=headers, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("items", [])
                for it in items:
                    content = (it.get("title", "") + " " + it.get("description", ""))
                    # HTML 태그 제거
                    clean_content = re.sub(r"<[^>]+>", "", content)
                    email_match = re.search(PersonnelRegexParser.EMAIL_PATTERN, clean_content)
                    if email_match:
                        email = email_match.group(0).strip()
                        return email
        except Exception as e:
            print(f"[Naver API Error] {e}")

        return None


# ==========================================
# 5. SQLite 데이터베이스 및 영속화 레이어
# ==========================================

class MediaDatabase:
    """
    빅카인즈 기사, 추출된 인사 발령 레코드, 인물 마스터, 이직 히스토리를
    저장하고 조회하는 SQLite 데이터베이스 관리자.
    """

    def __init__(self, db_path: str = "media_tracker.db"):
        self.db_path = db_path
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 1. 빅카인즈 원본 기사 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    news_id TEXT UNIQUE,
                    pub_date TEXT,
                    media TEXT,
                    title TEXT,
                    content TEXT,
                    byline TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 2. 발령 레코드 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS personnel_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pub_date TEXT,
                    media TEXT,
                    dept TEXT,
                    domain TEXT,
                    rank_title TEXT,
                    rank_level INTEGER,
                    name TEXT,
                    action_type TEXT,
                    email TEXT,
                    raw_text TEXT,
                    reporter_id TEXT
                )
            """)

            # 3. 인물 마스터 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reporters (
                    reporter_id TEXT PRIMARY KEY,
                    name TEXT,
                    email TEXT,
                    current_media TEXT,
                    current_dept TEXT,
                    current_rank TEXT,
                    first_seen TEXT,
                    last_seen TEXT,
                    display_label TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 4. 이직(Job Change) 감지 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reporter_id TEXT,
                    name TEXT,
                    from_media TEXT,
                    from_dept TEXT,
                    from_rank TEXT,
                    from_date TEXT,
                    to_media TEXT,
                    to_dept TEXT,
                    to_rank TEXT,
                    to_date TEXT,
                    days_gap INTEGER,
                    confidence_score REAL,
                    detected_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 인덱스 생성
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_name ON personnel_records(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_reporter ON personnel_records(reporter_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_records_media ON personnel_records(media)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reporters_name ON reporters(name)")

            conn.commit()

    def save_records_and_clusters(
        self,
        raw_records: List[Dict[str, Any]],
        clusters_by_name: Dict[str, List[Dict[str, Any]]],
        job_changes: List[Dict[str, Any]]
    ):
        """파싱 및 클러스터링된 데이터를 트랜잭션 단위로 일괄 저장합니다."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # 기존 레코드 및 이직 데이터 덮어쓰기/갱신을 위해 삭제 후 재등록
            cursor.execute("DELETE FROM personnel_records")
            cursor.execute("DELETE FROM reporters")
            cursor.execute("DELETE FROM job_changes")

            # 인물 마스터 등록
            for name, clusters in clusters_by_name.items():
                for c in clusters:
                    cursor.execute("""
                        INSERT OR REPLACE INTO reporters
                        (reporter_id, name, email, current_media, current_dept, current_rank, first_seen, last_seen, display_label)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        c["reporter_id"],
                        c["name"],
                        c.get("email", ""),
                        c["current_media"],
                        c["current_dept"],
                        c["current_rank"],
                        str(c["first_seen"]),
                        str(c["last_seen"]),
                        c["display_label"]
                    ))

            # 발령 레코드 등록
            for r in raw_records:
                cursor.execute("""
                    INSERT INTO personnel_records
                    (pub_date, media, dept, domain, rank_title, rank_level, name, action_type, email, raw_text, reporter_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(r.get("pub_date", "")),
                    r.get("media", ""),
                    r.get("dept", ""),
                    r.get("domain", ""),
                    r.get("rank_title", ""),
                    r.get("rank_level", 2),
                    r.get("name", ""),
                    r.get("action_type", "발령"),
                    r.get("email", ""),
                    r.get("raw_text", ""),
                    r.get("reporter_id", "")
                ))

            # 이직 이벤트 등록
            for jc in job_changes:
                cursor.execute("""
                    INSERT INTO job_changes
                    (reporter_id, name, from_media, from_dept, from_rank, from_date, to_media, to_dept, to_rank, to_date, days_gap, confidence_score, detected_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    jc["reporter_id"],
                    jc["name"],
                    jc["from_media"],
                    jc["from_dept"],
                    jc["from_rank"],
                    jc["from_date"],
                    jc["to_media"],
                    jc["to_dept"],
                    jc["to_rank"],
                    jc["to_date"],
                    jc.get("days_gap", 0),
                    jc.get("confidence_score", 0.0),
                    jc.get("detected_reason", "")
                ))

            conn.commit()

    def get_all_reporters(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM reporters ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    def get_reporter_timeline(self, reporter_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM personnel_records
                WHERE reporter_id = ?
                ORDER BY pub_date ASC, id ASC
            """, (reporter_id,))
            return [dict(row) for row in cursor.fetchall()]

    def search_reporters(self, name: str = "", media: str = "", dept: str = "", rank: str = "") -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT DISTINCT r.* FROM reporters r JOIN personnel_records pr ON r.reporter_id = pr.reporter_id WHERE 1=1"
            params = []
            if name:
                query += " AND r.name LIKE ?"
                params.append(f"%{name}%")
            if media:
                query += " AND pr.media LIKE ?"
                params.append(f"%{media}%")
            if dept:
                query += " AND pr.dept LIKE ?"
                params.append(f"%{dept}%")
            if rank:
                query += " AND pr.rank_title LIKE ?"
                params.append(f"%{rank}%")

            query += " ORDER BY r.last_seen DESC"
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_job_changes(self, limit: int = 100) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM job_changes
                ORDER BY to_date DESC, id DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, int]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM reporters")
            reporters_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM personnel_records")
            records_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM job_changes")
            changes_cnt = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT media) FROM personnel_records")
            media_cnt = cursor.fetchone()[0]
            return {
                "reporters": reporters_cnt,
                "records": records_cnt,
                "changes": changes_cnt,
                "media_count": media_cnt
            }


# ==========================================
# 6. 빅카인즈 파일 로더 및 일괄 처리 인터페이스
# ==========================================

def load_and_process_bigkinds_file(file_path_or_buffer) -> Dict[str, Any]:
    """
    빅카인즈 다운로드 표준 엑셀/CSV 파일을 읽고,
    인사 발령 텍스트를 파싱하여 데이터베이스에 저장합니다.
    """
    if isinstance(file_path_or_buffer, str):
        if file_path_or_buffer.endswith(".csv"):
            df = pd.read_csv(file_path_or_buffer, encoding="utf-8-sig")
        else:
            df = pd.read_excel(file_path_or_buffer)
    else:
        # Streamlit UploadedFile 객체인 경우
        try:
            df = pd.read_excel(file_path_or_buffer)
        except Exception:
            file_path_or_buffer.seek(0)
            df = pd.read_csv(file_path_or_buffer, encoding="utf-8-sig")

    # 빅카인즈 컬럼명 호환 처리
    col_map = {}
    for col in df.columns:
        c_clean = str(col).strip()
        if "일자" in c_clean:
            col_map[col] = "일자"
        elif "언론사" in c_clean:
            col_map[col] = "언론사"
        elif "제목" in c_clean:
            col_map[col] = "제목"
        elif "본문" in c_clean or "기사원문" in c_clean:
            col_map[col] = "본문"
        elif "기고자" in c_clean:
            col_map[col] = "기고자"

    df = df.rename(columns=col_map)
    required = ["일자", "언론사", "본문"]
    for req in required:
        if req not in df.columns:
            raise ValueError(f"빅카인즈 필수 컬럼 '{req}'이 누락되었습니다. (현재 컬럼: {list(df.columns)})")

    all_extracted_records = []
    for _, row in df.iterrows():
        pub_date = str(row["일자"]).strip()
        # YYYYMMDD 포맷 변환
        if len(pub_date) == 8 and pub_date.isdigit():
            pub_date = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:]}"

        media = str(row["언론사"]).strip()
        title = str(row.get("제목", "")).strip()
        body = str(row["본문"]).strip()

        records = PersonnelRegexParser.parse_article_text(
            article_text=body,
            default_media=media,
            article_title=title,
            pub_date=pub_date
        )
        all_extracted_records.extend(records)

    engine = PersonnelTrackerEngine()
    result = engine.process_records(all_extracted_records)
    return result
