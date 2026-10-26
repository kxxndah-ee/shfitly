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
import io
import re
import html
import sqlite3
import datetime
from typing import List, Dict, Tuple, Optional, Any
from urllib.parse import quote
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    "임원급": 9,
    "주필": 9,
    "논설주간": 9,
    "편집국장": 8,
    "보도국장": 8,
    "논설위원실장": 8,
    "국장": 8,
    "국장급": 8,
    "본부장": 8,
    "부국장": 7,
    "부국장급": 7,
    "부국장직대": 7,
    "부국장대우": 7,
    "에디터": 7,
    "논설위원": 7,
    "해설위원": 7,
    "전문위원": 7,
    "부장": 6,
    "부장급": 6,
    "부장직대": 6,
    "팀장(부장급)": 6,
    "데스크": 6,
    "지국장": 6,
    "부장대우": 5,
    "직대": 5,
    "차장": 4,
    "차장급": 4,
    "차장직대": 4,
    "팀장": 4,
    "팀장급": 4,
    "팀장직대": 4,
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
    "외", "등", "겸", "및", "간", "후보", "부국장",
    "카톡", "이메일", "제보", "문의", "게재", "연락처", "포함", "담당자", "저작권",
    "무단전재", "재배포", "신문사", "제보는", "산업국", "중앙신문", "경향신문", "한국정경",
    "서울경제", "한겨레", "조선일보", "동아일보", "연합뉴스", "뉴스1", "뉴시스"
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
    """언론사명에서 <인사>, [인사], 편집국, 보도국 등 부가 설명을 제거하고 순수 언론사명만 표준 표기합니다."""
    if not raw_name:
        return "미상"
    name = str(raw_name).strip()
    
    # 1. <인사>, [인사], 【인사】, (인사), 〈인사〉, «인사» 등 인사 관련 태그 및 단어 제거
    name = re.sub(r"[<\[\(【〈«][\s\w]*인사[\s\w]*[>\]\)】〉»]", "", name)
    name = re.sub(r"[<\[\(【〈«]\s*인사\s*", "", name)
    name = re.sub(r"\s*인사\s*[>\]\)】〉»]", "", name)
    name = re.sub(r"^\s*인사\s*|\s*인사\s*$", "", name)
    name = re.sub(r"^[◇◆■●◎▲△▼▷▶·\-\*:<\[\(【〈«\s]+|[◇◆■●◎▲△▼▷▶·\-\*:>\]\)】〉»\s]+$", "", name)

    # 2. 편집국, 보도국, 논설실 등 부서/국/본부 설명 제거
    name = re.sub(r"\s*(편집국|보도국|보도본부|디지털뉴스국|디지털뉴스본부|디지털국|논설실|논설위원실|신문총괄|경영총괄|미디어본부|방송본부|뉴스총괄)\b.*$", "", name)

    # 3. 법인명, 신문사, 일보사 등 접미사 정리
    name = re.sub(r"\(주\)|주식회사|신문사|일보사|방송사|미디어그룹", "", name).strip()
    if name.endswith("사") and len(name) >= 3 and not name.endswith("통신사"):
        name = name[:-1]

    # 4. 주요 언론사 매핑 및 표준화 규칙
    if "조선일보" in name or ("조선" in name and "일보" in raw_name): return "조선일보"
    if "중앙일보" in name or ("중앙" in name and "일보" in raw_name): return "중앙일보"
    if "동아일보" in name or ("동아" in name and "일보" in raw_name): return "동아일보"
    if "한국경제" in name or "한경" in name: return "한국경제"
    if "매일경제" in name or "매경" in name: return "매일경제"
    if "서울경제" in name or "서경" in name: return "서울경제"
    if "아시아경제" in name or "아경" in name: return "아시아경제"
    if "헤럴드" in name: return "헤럴드경제"
    if "파이낸셜" in name: return "파이낸셜뉴스"
    if "머니투데이" in name or "머투" in name: return "머니투데이"
    if "이데일리" in name: return "이데일리"
    if "디지털타임스" in name: return "디지털타임스"
    if "전자신문" in name: return "전자신문"
    if "브릿지" in name: return "브릿지경제"
    if "아주경제" in name: return "아주경제"
    if "에너지경제" in name: return "에너지경제"
    if "연합뉴스" in name or name == "연합": return "연합뉴스"
    if "뉴스1" in name or "뉴스원" in name: return "뉴스1"
    if "뉴시스" in name: return "뉴시스"
    if "한국일보" in name: return "한국일보"
    if "경향" in name: return "경향신문"
    if "한겨레" in name: return "한겨레"
    if "문화일보" in name or ("문화" in name and "일보" in raw_name): return "문화일보"
    if "세계일보" in name or ("세계" in name and "일보" in raw_name): return "세계일보"
    if "국민일보" in name or ("국민" in name and "일보" in raw_name): return "국민일보"
    if "부산일보" in name: return "부산일보"
    if "국제신문" in name: return "국제신문"
    if "매일신문" in name: return "매일신문"
    if "영남일보" in name: return "영남일보"
    if "광주일보" in name: return "광주일보"
    if "대전일보" in name: return "대전일보"
    if "강원일보" in name: return "강원일보"
    if "강원도민일보" in name: return "강원도민일보"

    # 남아있는 불필요한 기호 및 단어 최종 정제
    name = re.sub(r"^[◇◆■●◎▲△▼▷▶·\-\*:<\[\(【〈«\s]+|[◇◆■●◎▲△▼▷▶·\-\*:>\]\)】〉»\s]+$", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "온라인뉴스"


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


MEDIA_KEYWORDS: List[str] = [
    "신문", "일보", "방송", "뉴스", "통신", "경제", "미디어", "타임스", "타임즈", "저널", "데일리", "헤럴드", "프레스",
    "tv", "kbs", "mbc", "sbs", "ytn", "jtbc", "tv조선", "채널a", "mbn", "ebs", "cbs", "bbs", "tbs",
    "연합뉴스", "뉴스1", "뉴시스", "한겨레", "오마이뉴스", "프레시안", "노컷뉴스", "더팩트", "아이뉴스24", "지디넷",
    "조선비즈", "매경", "한경", "서경", "아경", "머니투데이", "이데일리", "디지털타임스", "전자신문", "조세일보", "스포츠조선",
    "스포츠동아", "스포츠서울", "스포츠경향", "일간스포츠", "시사in", "시사저널", "주간조선", "주간동아", "한겨레21"
]

NON_MEDIA_KEYWORDS: List[str] = [
    "부처", "기획재정부", "교육부", "과기정통부", "외교부", "통일부", "법무부", "국방부", "행안부", "보훈부",
    "문체부", "농림축산", "농식품부", "산업통상", "산업부", "복지부", "환경부", "고용노동부", "노동부", "여가부",
    "국토교통부", "국토부", "해수부", "중기부", "국세청", "관세청", "조달청", "통계청", "검찰청", "검찰", "경찰청", "경찰",
    "소방청", "문화재청", "산림청", "특허청", "기상청", "해경", "감사원", "국정원", "금감원", "금융위", "공정위", "방통위",
    "시청", "도청", "구청", "군청", "공사", "공단", "연구원", "진흥원", "재단", "은행", "금융지주", "증권사", "생명", "화재",
    "카드", "보험", "캐피탈", "대학교", "대학원", "대학", "병원", "의료원", "의학원", "원자력",
    "건설사", "현대건설", "대우건설", "gs건설", "포스코건설", "삼성전자", "lg전자", "전자공업",
    "물산", "중공업", "화학", "모빌리티", "자동차", "항공", "엔지니어링", "제약사", "백화점", "쇼핑", "호텔",
    "서울시", "부산시", "인천시", "대구시", "광주시", "대전시", "울산시", "세종시", "경기도", "강원도",
    "충북도", "충남도", "전북도", "전남도", "경북도", "경남도", "제주도", "안동시", "과기", "과학기술", "재정경제", "행정안전"
]


def is_media_organization(name: str) -> bool:
    """해당 기관/조직명이 언론사(신문/방송/통신/인터넷매체)인지 정밀 판별합니다."""
    if not name:
        return False
    n = name.lower().strip()
    if n in ["2010년", "2011년", "제1", "미상", "온라인뉴스", "뉴스", "언론사", "기자", "본사", "지사", "편집국", "보도국"]:
        return False
    # 직급명이나 직책명이 포함된 경우 언론사명이 아님
    if any(r in n for r in ["부장", "차장", "국장", "팀장", "소장", "실장", "본부장", "위원", "기자", "대표", "이사"]):
        return False
    for nmk in NON_MEDIA_KEYWORDS:
        if nmk in n:
            return False
    for mk in MEDIA_KEYWORDS:
        if mk in n:
            return True
    return False


def is_personnel_article_title(title: str, text: str = "") -> bool:
    """
    기사 제목이 [인사] 또는 <인사> 형태의 공식 인사 기사인지 검증합니다.
    비언론사(정부부처/지자체/금융기관/기업) 인사 제목은 배제합니다.
    """
    if not title:
        return False
    t = html.unescape(str(title)).replace(" ", "")
    is_personnel = (
        ("[인사" in t) or ("<인사" in t) or ("【인사" in t) or ("(인사" in t) or 
        ("〈인사" in t) or ("«인사" in t) or ("인사발령" in t) or
        ("인사종합" in t) or ("오늘의인사" in t) or ("인사]" in t) or ("인사>" in t) or ("인사】" in t) or ("인사〉" in t)
    )
    if not is_personnel:
        return False

    t_lower = html.unescape(str(title)).lower()
    # 1. 언론사/미디어 명칭이 포함되어 있다면 종합 인사 기사이므로 통과
    for mk in MEDIA_KEYWORDS:
        if mk in t_lower:
            return True

    # 2. 제목에 '등', '외', '종합', '오늘의' 등이 포함된 복합 종합 인사 기사는 본문에 언론사 발령이 포함될 수 있으므로 허용
    if any(k in t_lower for k in [" 등", " 외", "/등", "/외", "종합", "오늘의", "外"]):
        return True

    # 3. 언론사명이 없고 단독 비언론사(정부부처/기업 등) 인사 기사인 경우 제외
    for nmk in NON_MEDIA_KEYWORDS:
        if nmk in t_lower:
            return False

    return True


# ==========================================
# 2. 한국 언론사 인사 기사 전용 정규표현식 파서
# ==========================================

class PersonnelRegexParser:
    """
    한국 언론사의 전형적인 인사 발령 기사 텍스트에서
    (언론사, 부서, 직급, 이름, 이메일, 발령구분)을 고속 추출하는 정규표현식 엔진.
    """

    # 구분 기호 및 불릿 문자 (각종 한글 특수문자, 가운뎃점, 아라에아(ㆍ), 네모, 세모, 다이아몬드 등 완벽 대응)
    BULLET_CHARS = r"[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━]"
    
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
        r"(?:◆|◇|■|□|◎|●|【|\[|<)\s*([가-힣0-9A-Za-z]+(?:일보|경제|뉴스|신문|타임스|미디어|데일리|헤럴드|통신|방송|TV|mbc|kbs|sbs|ytn|jtbc|뉴스1|뉴시스|연합뉴스|한겨레))\s*(?:】|\]|>|◆|◇|■|□|◎|●)?"
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
        if is_media_organization(name):
            return None
        return name

    @classmethod
    def parse_article_text(
        cls,
        article_text: str,
        default_media: str = "",
        article_title: str = "",
        pub_date: str = "",
        article_url: str = ""
    ) -> List[Dict[str, Any]]:
        """
        기사 본문 전체를 순회하며 인사 발령 레코드 리스트를 추출합니다.
        """
        results: List[Dict[str, Any]] = []
        if not article_text:
            return results

        # 제목에서 언론사 힌트 파악 (예: [인사] 뉴스1 or <인사> 연합뉴스 or 매일경제 <인사>)
        current_media = normalize_media_name(default_media)
        title_media_match = re.search(r"(?:\[인사\]|<인사>|【인사】|\(인사\))\s*([가-힣A-Za-z0-9]+)", html.unescape(article_title))
        if title_media_match:
            cand = title_media_match.group(1).strip()
            if is_media_organization(cand):
                current_media = normalize_media_name(cand)
        else:
            rev_match = re.search(r"([가-힣A-Za-z0-9]+)\s*(?:\[인사\]|<인사>|【인사】|\(인사\))", html.unescape(article_title))
            if rev_match:
                cand = rev_match.group(1).strip()
                if is_media_organization(cand):
                    current_media = normalize_media_name(cand)

        current_action = "발령"
        current_section_rank = ""

        # 줄 단위 또는 불릿 기호 단위로 분할 처리
        lines = [line.strip() for line in article_text.splitlines() if line.strip()]

        for line in lines:
            # 0. 푸터, 제보 및 문의 안내 문구 스킵
            if any(k in line for k in ["게재 문의", "제보", "저작권", "무단전재", "재배포", "기사문의", "확인용", "카톡", "연락처"]):
                continue

            # 1. 언론사 변경 감지 (기사 중간에 다른 언론사 헤더가 나올 경우)
            media_match = cls.MEDIA_HEADER_PATTERN.search(line)
            if media_match:
                cand_media = media_match.group(1).strip()
                if is_media_organization(cand_media):
                    current_media = normalize_media_name(cand_media)

            # 2. 발령 액션 및 섹션 직급 헤더 감지 (예: ◇부장대우 승진, ◇차장 승진, <승진>, ◇전보 등)
            clean_hdr = re.sub(r"^[◇◆■▲△▼▷▶<\[\(【\s]+", "", line).strip()
            for act_cand in ["승진", "전보", "보임", "보직", "선임", "신규선임", "전출", "전입", "파견", "퇴임", "위촉"]:
                if act_cand in clean_hdr:
                    current_action = act_cand
                    break
            for r_cand in SORTED_RANKS:
                if clean_hdr.startswith(r_cand) or f"{r_cand} 승진" in clean_hdr or f"{r_cand}승진" in clean_hdr or f"{r_cand} 전보" in clean_hdr or f"<{r_cand}>" in line or f"[{r_cand}]" in line or f"({r_cand})" in line:
                    current_section_rank = r_cand
                    break

            # 4. 불릿 기호 기준으로 개별 발령 항목 쪼개기
            # 예: △건설부동산부장 김희준 △정치부 차장 박철수
            items = re.split(cls.BULLET_CHARS, line)
            for item in items:
                item = item.strip()
                if not item or len(item) < 2:
                    continue

                # 4-1. 항목 단위 언론사 헤더 변경 감지 (한 줄에 여러 언론사가 연달아 나오는 경우, 예: ■매경미디어그룹 ◇매일경제신문 △... ◇MBN △...)
                cand_media_clean = re.sub(r"^[<\[\(【\s]+|[>\]\)】\s]+$", "", item).strip()
                if len(cand_media_clean) <= 15 and is_media_organization(cand_media_clean) and not any(cls.clean_name(t) for t in item.split()):
                    current_media = normalize_media_name(cand_media_clean)
                    continue

                # 4-2. 항목 단위 발령 액션 및 섹션 직급 헤더 감지 (예: <부장대우 승진>, <승진>, <전보> 등)
                clean_item_hdr = re.sub(r"^[<\[\(【\s]+|[>\]\)】\s]+$", "", item).strip()
                item_has_header = False
                for act_cand in ["승진", "전보", "보임", "보직", "선임", "신규선임", "전출", "전입", "파견", "퇴임", "위촉"]:
                    if act_cand in clean_item_hdr:
                        current_action = act_cand
                        item_has_header = True
                        break
                for r_cand in SORTED_RANKS:
                    if clean_item_hdr.startswith(r_cand) or f"{r_cand} 승진" in clean_item_hdr or f"{r_cand} 전보" in clean_item_hdr:
                        current_section_rank = r_cand
                        item_has_header = True
                        break
                if item_has_header and not any(cls.clean_name(t) for t in item.split()):
                    continue

                # 이메일 추출
                found_email = ""
                email_match = re.search(cls.EMAIL_PATTERN, item)
                if email_match:
                    found_email = email_match.group(0).strip()
                    item_clean = re.sub(cls.EMAIL_PATTERN, "", item).replace("()", "").strip()
                else:
                    item_clean = item

                # 괄호 패턴 사전 분석 (예: 정치부장(홍길동) 또는 홍길동(정치부장))
                parenthesis_match1 = re.search(r"([가-힣]+)\(([가-힣]{2,4})\)", item_clean)
                parenthesis_match2 = re.search(r"([가-힣]{2,4})\(([가-힣]+)\)", item_clean)
                if parenthesis_match1 and cls.clean_name(parenthesis_match1.group(2)):
                    item_clean = f"{parenthesis_match1.group(1)} {parenthesis_match1.group(2)}"
                elif parenthesis_match2 and cls.clean_name(parenthesis_match2.group(1)):
                    item_clean = f"{parenthesis_match2.group(2)} {parenthesis_match2.group(1)}"
                else:
                    item_clean = re.sub(r"\([가-힣\w\s·]+\)", "", item_clean).strip()

                # 서브 아이템 파싱 (쉼표, 슬래시, 세미콜론 등으로 나열된 경우)
                sub_records = cls._parse_single_bullet(
                    item_clean, current_media, current_action, current_section_rank, found_email
                )
                for rec in sub_records:
                    rec["pub_date"] = pub_date or str(datetime.date.today())
                    rec["article_title"] = article_title or ""
                    rec["article_url"] = article_url or ""
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
        단일 발령 텍스트 파싱. 이름의 위치(앞, 뒤, 중간)와 다양한 형태, 등호(=), 복수 인명 나열을 유연하게 지원.
        """
        records: List[Dict[str, Any]] = []

        # 등호(=) 및 콜론(:) 공백 치환 후 쉼표, 슬래시, 세미콜론, 가운데점으로 분할
        clean_text = re.sub(r"\s*(=|:)\s*", " ", text)
        parts = re.split(r"[,/;\·|]", clean_text)
        last_dept = ""
        last_rank = section_rank or ""

        for part in parts:
            part = part.strip()
            tokens = part.split()
            if not tokens:
                continue

            extracted_names: List[str] = []
            extracted_rank = section_rank or last_rank or "기자"
            extracted_dept = last_dept or "본사"

            # 1. 토큰 중 유효한 한글 인명(2~4자) 찾기
            # 뒤쪽에서부터 연속으로 인명인 토큰들을 모두 수집 (예: "편집국 경제부 차장 홍길동 김철수")
            trailing_name_indices: List[int] = []
            for i in range(len(tokens) - 1, -1, -1):
                tok_cand = tokens[i]
                c_name = cls.clean_name(tok_cand)
                if c_name and tok_cand not in RANK_HIERARCHY and not re.search(f"({RANK_REGEX_PATTERN})$", tok_cand):
                    trailing_name_indices.append(i)
                else:
                    break

            if trailing_name_indices:
                split_point = min(trailing_name_indices)
                other_tokens = tokens[:split_point]
                for idx in reversed(trailing_name_indices):
                    c_name = cls.clean_name(tokens[idx])
                    if c_name:
                        extracted_names.append(c_name)
            else:
                # 앞쪽에서 인명이 시작하는 경우 (예: "홍길동 건설부동산부장")
                front_cand = cls.clean_name(tokens[0])
                if front_cand and tokens[0] not in RANK_HIERARCHY and not re.search(f"({RANK_REGEX_PATTERN})$", tokens[0]):
                    extracted_names.append(front_cand)
                    other_tokens = tokens[1:]
                else:
                    other_tokens = tokens

            # 2. 이름 위치에 따라 부서와 직급 추정
            if extracted_names:
                if not other_tokens:
                    # 이름만 있는 경우
                    extracted_rank = section_rank or last_rank or "기자"
                elif len(other_tokens) == 1:
                    first = other_tokens[0]
                    # 〃(반복 부호) 처리: 이전 직급 상속 및 부서명 정제
                    if "〃" in first:
                        dept_clean = first.replace("〃", "").strip()
                        if dept_clean and not dept_clean.endswith("부") and not dept_clean.endswith("팀"):
                            dept_clean = dept_clean + ("팀" if "팀" in last_rank else "부")
                        extracted_dept = dept_clean or last_dept or "본사"
                        extracted_rank = last_rank or section_rank or "기자"
                    else:
                        rank_match = re.search(f"({RANK_REGEX_PATTERN})$", first)
                        if rank_match:
                            extracted_rank = rank_match.group(1)
                            dept_part = first[:rank_match.start()].strip()
                            if dept_part:
                                if (extracted_rank.startswith("부장") or extracted_rank.startswith("차장") or extracted_rank.startswith("팀장")) and not dept_part.endswith("부") and not dept_part.endswith("팀"):
                                    dept_part = dept_part + ("팀" if extracted_rank.startswith("팀장") else "부")
                                extracted_dept = dept_part
                            else:
                                extracted_dept = last_dept or "본사"
                        elif first in RANK_HIERARCHY:
                            extracted_rank = first
                            extracted_dept = last_dept or "본사"
                        else:
                            extracted_dept = first
                            extracted_rank = section_rank or last_rank or "기자"
                    last_dept = extracted_dept
                    if extracted_rank and extracted_rank not in ["기자", "평기자"]:
                        last_rank = extracted_rank
                else:
                    # 2개 이상 토큰이 남은 경우 (예: "건설부동산부" "부장", "편집국" "오피니언부", "편집국" "경제부" "차장")
                    found_rank_idx = -1
                    for r_i, tok in enumerate(other_tokens):
                        if tok in RANK_HIERARCHY or re.search(f"({RANK_REGEX_PATTERN})$", tok):
                            found_rank_idx = r_i
                            break
                    if found_rank_idx != -1:
                        rank_tok = other_tokens[found_rank_idx]
                        rank_m = re.search(f"({RANK_REGEX_PATTERN})$", rank_tok)
                        if rank_m:
                            extracted_rank = rank_m.group(1)
                            prefix_dept = rank_tok[:rank_m.start()].strip()
                            dept_tokens = list(other_tokens[:found_rank_idx])
                            if prefix_dept:
                                dept_tokens.append(prefix_dept)
                            extracted_dept = " ".join(dept_tokens) if dept_tokens else (last_dept or "본사")
                        else:
                            extracted_rank = rank_tok
                            dept_tokens = other_tokens[:found_rank_idx]
                            extracted_dept = " ".join(dept_tokens) if dept_tokens else (last_dept or "본사")
                    else:
                        # 직급 토큰이 없는 경우 (예: "편집국" "오피니언부") -> 모두 부서로 간주, 직급은 section_rank 또는 직전 직급
                        extracted_dept = " ".join(other_tokens)
                        extracted_rank = section_rank or last_rank or "기자"
                    last_dept = extracted_dept
                    if extracted_rank and extracted_rank not in ["기자", "평기자"]:
                        last_rank = extracted_rank

            # 3. 토큰이 1개로 붙어있는 경우 (예: "건설부동산부장김희준" or "홍길동정치부장")
            elif len(tokens) == 1:
                single_str = tokens[0]
                # 직급+이름 형태
                match1 = re.search(f"({RANK_REGEX_PATTERN})([가-힣]{{2,4}})$", single_str)
                # 이름+직급 형태
                match2 = re.search(f"^([가-힣]{{2,4}})({RANK_REGEX_PATTERN})", single_str)

                if match1 and cls.clean_name(match1.group(2)):
                    extracted_names = [match1.group(2)]
                    extracted_rank = match1.group(1)
                    extracted_dept = single_str[:match1.start()].strip() or last_dept or "본사"
                    last_dept = extracted_dept
                elif match2 and cls.clean_name(match2.group(1)):
                    extracted_names = [match2.group(1)]
                    extracted_rank = match2.group(2)
                    extracted_dept = single_str[match2.end():].strip() or last_dept or "본사"
                    last_dept = extracted_dept

            for extracted_name in extracted_names:
                final_media = normalize_media_name(media) if media else "미상"
                if not is_media_organization(final_media):
                    continue  # 언론사 인사소식만 정리 (비언론사 배제)

                clean_dept = re.sub(r"^[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━\s]+", "", extracted_dept).strip()
                clean_rank = re.sub(r"^[△▲▷▶○●■□◆◇◈▣◎⊙◐◑·ㆍ\-\*―━\s]+", "", extracted_rank).strip()
                for bureau in ["편집국 ", "보도국 ", "논설실 ", "디지털뉴스국 ", "신문총괄 "]:
                    if clean_dept.startswith(bureau) and len(clean_dept) > len(bureau):
                        clean_dept = clean_dept[len(bureau):].strip()
                if not clean_rank:
                    clean_rank = "기자"

                rank_level = parse_rank_level(clean_rank)
                domain = classify_domain(clean_dept)

                records.append({
                    "media": final_media,
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

    def process_records(self, raw_records: List[Dict[str, Any]], append: bool = True) -> Dict[str, Any]:
        """
        추출된 모든 레코드를 시계열 순서로 정렬한 후,
        1순위 이메일 매칭 및 2순위 알고리즘 휴리스틱을 적용하여 클러스터링을 수행합니다.
        append=True인 경우 기존 DB의 레코드들과 병합(Merge)하여 누적 처리합니다.
        """
        if append:
            existing_records = self.db.get_all_raw_records()
            seen_keys = set()
            combined_records = []
            
            for r in existing_records:
                key = (str(r.get("pub_date", "")), str(r.get("media", "")), str(r.get("name", "")), str(r.get("dept", "")), str(r.get("rank_title", "")))
                if key not in seen_keys:
                    seen_keys.add(key)
                    combined_records.append(dict(r))

            new_added_count = 0
            for r in raw_records:
                key = (str(r.get("pub_date", "")), str(r.get("media", "")), str(r.get("name", "")), str(r.get("dept", "")), str(r.get("rank_title", "")))
                if key not in seen_keys:
                    seen_keys.add(key)
                    combined_records.append(r)
                    new_added_count += 1
            
            effective_records = combined_records
        else:
            effective_records = list(raw_records)
            new_added_count = len(raw_records)

        # 언론사 인사소식만 엄격 필터링
        effective_records = [
            r for r in effective_records 
            if is_media_organization(r.get("media", "")) and (not r.get("article_title") or is_personnel_article_title(r.get("article_title", "")))
        ]

        if not effective_records:
            return {"total_records": 0, "new_records": 0, "unique_reporters": 0, "job_changes_detected": 0}

        # 날짜순 정렬 (과거 -> 현재)
        def parse_date_key(r):
            d_str = str(r.get("pub_date", "1970-01-01")).replace("-", "").replace(".", "")
            if len(d_str) >= 8:
                return d_str[:8]
            return "19700101"

        sorted_records = sorted(effective_records, key=parse_date_key)

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
            "new_records": new_added_count,
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

            # CASE B: 서로 다른 언론사 간의 이동 (이직 탐지 알고리즘)
            else:
                # 16번 요청 조건 반영:
                # 1) A매체 이전 인사 후 6개월(180일) 안에 타사 인사리스트에 등장할 시 이직으로 감지
                # 2) 직급이 강등될 경우 이직에서 제외 (Demotion Filter)
                # 3) 동명이인 필터링: 유사한 부서/도메인일 경우에만 이직으로 인정
                
                is_within_6_months = (0 < days_diff <= 180) or (has_same_email and 0 < days_diff <= 365)
                is_demoted = (rank_diff < 0)
                
                # 부서 및 도메인 유사도 검증
                domain_sim = calculate_domain_similarity(rec_domain, last_domain)
                last_kw = set(re.findall(r"[가-힣]{2,}", last_dept)) - STOPWORDS
                rec_kw = set(re.findall(r"[가-힣]{2,}", rec_dept)) - STOPWORDS
                kw_match = bool(last_kw and rec_kw and (last_kw & rec_kw))
                is_similar_dept = (domain_sim >= 0.5) or kw_match or (rec_domain == last_domain and rec_domain != "일반") or has_same_email

                if is_within_6_months and not is_demoted and is_similar_dept:
                    # 정상적인 매체 간 이직 판정
                    score += 65.0 + domain_sim * 30.0 + max(0.0, 25.0 - (days_diff / 10.0))
                    if -1 <= rank_diff <= 2:
                        score += 20.0
                    
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
                            f"6개월 내 타사 이동, 부서 유사도({int(domain_sim*100)}%), "
                            f"{'이메일 일치' if has_same_email else '직급 승진 및 유지'}"
                        )
                    }
                else:
                    # 6개월 초과, 직급 강등, 또는 비유사 부서: 이직에서 제외 및 동명이인 분리 가중치
                    is_job_change = False
                    candidate_change_meta = None
                    if is_demoted:
                        score -= 90.0  # 직급 강등 시 동명이인으로 분리
                    if not is_similar_dept and not has_same_email:
                        score -= 90.0  # 부서 무관 시 동명이인으로 분리
                    if not is_within_6_months and not has_same_email:
                        score -= 50.0  # 6개월 초과 시 이직 미인정

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
                record["action_type"] = f"이직: {change_metadata['from_media']} ➔ {rec_media}"

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
        """드롭다운에서 동명이인을 한눈에 구분할 수 있는 표시명 생성."""
        name = cluster["name"]
        curr_media = cluster["current_media"]
        curr_dept = cluster["current_dept"]
        email = cluster.get("email", "")
        email_hint = f" · {email}" if email else ""
        first_rec = cluster["records"][0]
        origin_hint = f" · {first_rec['media']} 출신" if first_rec["media"] != curr_media else ""
        cluster["display_label"] = f"{name} : {curr_media} {curr_dept}{origin_hint}{email_hint}"


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
# 4-2. 실시간 온라인 뉴스 크롤러 및 발령 트래커
# ==========================================

class OnlineNewsTracker:
    """
    네이버/다음/구글 뉴스 온라인 기사를 실시간 수집 및 스크래핑하여,
    인사 발령 기사 및 기자 바이라인을 자동 분석하고 DB에 즉시 등록하는 온라인 트래커.
    최근 30개년(1995~2026)의 포털 기사를 실시간 탐색하여 연대기 커리어 프로필을 구축합니다.
    """

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Upgrade-Insecure-Requests": "1"
    }

    @classmethod
    def extract_pub_date(cls, url: str, soup: BeautifulSoup, raw_text: str = "") -> Optional[str]:
        """기사 URL 또는 본문/HTML 메타데이터에서 실제 기사 발행 일자(YYYY-MM-DD)를 정밀 추출합니다."""
        # 1. 다음 뉴스 URL 패턴 (예: v.daum.net/v/20240324143000999)
        daum_match = re.search(r"v\.daum\.net/v/(\d{4})(\d{2})(\d{2})", url)
        if daum_match:
            return f"{daum_match.group(1)}-{daum_match.group(2)}-{daum_match.group(3)}"

        # 2. 네이버 뉴스 메타데이터 및 속성 (._ARTICLE_DATE_TIME, span[data-date-time])
        naver_date = soup.select_one("._ARTICLE_DATE_TIME, .media_end_head_info_datestamp_time, span[data-date-time]")
        if naver_date:
            dt_val = naver_date.get("data-date-time", "") or naver_date.get("data-modify-date-time", "")
            if dt_val and len(dt_val) >= 10:
                m_iso = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", dt_val[:10])
                if m_iso:
                    return f"{m_iso.group(1)}-{int(m_iso.group(2)):02d}-{int(m_iso.group(3)):02d}"
            t_text = naver_date.get_text(strip=True)
            m = re.search(r"(199[0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", t_text)
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 3. 표준 오픈그래프 및 발행일 메타 태그
        for meta_name in ["article:published_time", "og:regDate", "pubdate", "sailthru.date", "date", "publish-date"]:
            meta = soup.select_one(f"meta[property='{meta_name}'], meta[name='{meta_name}']")
            if meta and meta.get("content"):
                c = meta["content"]
                m = re.search(r"(199[0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", c)
                if m:
                    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 4. 언론사 공통 날짜 표시 태그 (.txt_info, .num_date, .info_view, .date)
        for tag in soup.select(".txt_info, .num_date, .info_view, .date, .byline_date, .article_info"):
            text = tag.get_text(strip=True)
            m = re.search(r"(199[0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", text)
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 5. 본문 시작부 1,200자 내의 정규식 탐색
        if raw_text:
            m = re.search(r"(199[0-9]|20[0-3][0-9])[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})", raw_text[:1200])
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 6. 일반 URL 경로 내의 날짜 (/2024/05/23/ 등)
        url_m = re.search(r"/(199[0-9]|20[0-3][0-9])/?(\d{2})/?(\d{2})", url)
        if url_m:
            return f"{url_m.group(1)}-{url_m.group(2)}-{url_m.group(3)}"

        return None

    @classmethod
    def search_online_articles(
        cls, 
        query: str, 
        max_items: int = 25,
        client_id: str = "",
        client_secret: str = "",
        start_year: int = 1995,
        end_year: int = 2026,
        sort_first: str = "date"
    ) -> List[Dict[str, Any]]:
        """
        포털(다음·네이버·구글 뉴스 RSS) 및 네이버 공식 검색 API를 통해 기사 목록을 수집합니다.
        최신 기사를 누락 없이 신속하게 가져오기 위해 sort_first='date' 기본값으로 최신순 검색을 우선 수행합니다.
        """
        encoded_query = quote(query)
        articles = []
        seen_urls = set()

        # 0. 네이버 공식 검색 API (API 키가 유효하게 제공된 경우)
        if client_id and client_secret:
            api_sort_opts = ["date", "sim"] if sort_first == "date" else ["sim", "date"]
            for s_opt in api_sort_opts:
                if len(articles) >= max_items:
                    break
                try:
                    api_url = "https://openapi.naver.com/v1/search/news.json"
                    api_headers = {
                        "X-Naver-Client-Id": client_id,
                        "X-Naver-Client-Secret": client_secret,
                        **cls.HEADERS
                    }
                    api_params = {"query": query, "display": min(max_items, 30), "sort": s_opt}
                    api_resp = requests.get(api_url, headers=api_headers, params=api_params, timeout=5, verify=False)
                    if api_resp.status_code == 200:
                        api_data = api_resp.json()
                        for it in api_data.get("items", []):
                            raw_tit = re.sub(r"<[^>]+>", "", it.get("title", "")).strip()
                            raw_tit = raw_tit.replace("&quot;", '"').replace("&amp;", '&').replace("&lt;", '<').replace("&gt;", '>')
                            target_url = it.get("link", "") or it.get("originallink", "")
                            if target_url and target_url not in seen_urls:
                                seen_urls.add(target_url)
                                # pubDate 파싱
                                pub_d = it.get("pubDate", "")
                                date_str = ""
                                if pub_d:
                                    dm = re.search(r"(\d{4})", pub_d)
                                    if dm:
                                        try:
                                            import email.utils
                                            dt = email.utils.parsedate_to_datetime(pub_d)
                                            date_str = dt.strftime("%Y-%m-%d")
                                        except Exception:
                                            pass
                                articles.append({
                                    "title": raw_tit,
                                    "url": target_url,
                                    "media": "온라인뉴스",
                                    "date_text": date_str
                                })
                                if len(articles) >= max_items:
                                    break
                except Exception as e:
                    print(f"[Naver API Search Error] {e}")

        # 1. 다음 뉴스 검색 (최신순 우선 + 관련도순)
        daum_sort_opts = ["recency", "accuracy"] if sort_first == "date" else ["accuracy", "recency"]
        for daum_sort in daum_sort_opts:
            if len(articles) >= max_items:
                break
            try:
                daum_url = f"https://search.daum.net/search?w=news&q={encoded_query}&sort={daum_sort}&period=u&sd={start_year}0101000000&ed={end_year}1231235959"
                resp = requests.get(daum_url, headers=cls.HEADERS, timeout=6, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    card_items = soup.select("div.c-item-content, li.bx, div.item-title, ul.list_news > li")
                    for it in card_items:
                        tit_a = it.select_one("strong.tit-doc a, .item-title a, a.tit_main, a.c-tit-doc")
                        if not tit_a:
                            continue
                        # 다음뉴스 인링크(v.daum.net) 우선 선택 -> 본문 텍스트 완전 보장 및 차단 방지
                        daum_inlink = it.select_one("a[href*='v.daum.net/v/'], a[href*='news.v.daum.net/v/']")
                        href = daum_inlink.get("href", "") if daum_inlink else tit_a.get("href", "")
                        if not href or href in seen_urls:
                            continue
                        title = html.unescape(tit_a.get_text(strip=True))
                        if len(title) < 3 or not is_personnel_article_title(title):
                            continue

                        press_elem = it.select_one(".item-subinfo .txt_info, a.f_nb, span.c-tit-info, .gem-subinfo a, span.info")
                        press_name = press_elem.get_text(strip=True) if press_elem else "온라인뉴스"

                        seen_urls.add(href)
                        d_match = re.search(r"v\.daum\.net/v/(\d{4})(\d{2})(\d{2})", href)
                        d_text = f"{d_match.group(1)}-{d_match.group(2)}-{d_match.group(3)}" if d_match else ""
                        articles.append({
                            "title": title,
                            "url": href,
                            "media": press_name,
                            "date_text": d_text
                        })
                        if len(articles) >= max_items:
                            break
            except Exception as e:
                print(f"[Daum Search Error] {e}")

        # 2. 네이버 뉴스 검색 (최신순 우선 + 관련도순)
        n_sort_opts = ["1", "0"] if sort_first == "date" else ["0", "1"]
        for n_sort in n_sort_opts:
            if len(articles) >= max_items:
                break
            try:
                nso_sort = "so:dd" if n_sort == "1" else "so:r"
                naver_url = f"https://m.search.naver.com/search.naver?where=m_news&query={encoded_query}&sort={n_sort}&ds={start_year}.01.01&de={end_year}.12.31&nso={nso_sort},p:from{start_year}0101to{end_year}1231,a:all"
                resp = requests.get(naver_url, headers=cls.HEADERS, timeout=6, verify=False)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.content, "html.parser")
                    for it in soup.select("div.news_wrap, div.news_area, li.bx, div.bx"):
                        tit_a = it.select_one("a.news_tit, a.api_txt_lines, a.tit")
                        if not tit_a:
                            continue
                        title = html.unescape(tit_a.get_text(strip=True))
                        if len(title) < 3 or not is_personnel_article_title(title):
                            continue

                        # 네이버뉴스 인링크(n.news.naver.com) 우선 선택 -> 403 차단 및 JS 미렌더링 원천 방지
                        naver_inlink = it.select_one("a.info[href*='news.naver.com'], a[href*='n.news.naver.com'], a[href*='m.news.naver.com']")
                        href = naver_inlink.get("href", "") if naver_inlink else tit_a.get("href", "")
                        if not href or href in seen_urls:
                            continue

                        press_elem = it.select_one("a.info.press, span.info, span.press")
                        press_name = press_elem.get_text(strip=True) if press_elem else "온라인뉴스"
                        seen_urls.add(href)
                        articles.append({
                            "title": title,
                            "url": href,
                            "media": press_name,
                            "date_text": ""
                        })
                        if len(articles) >= max_items:
                            break
            except Exception as e:
                print(f"[Naver Search Error] {e}")

        # 3. 구글 뉴스 RSS (최신 연도 검색 시에만 fallback 활성화하여 과거 검색 오염 방지)
        if end_year >= 2025 and len(articles) < max_items:
            try:
                g_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=ko&gl=KR&ceid=KR:ko"
                g_resp = requests.get(g_url, headers=cls.HEADERS, timeout=5, verify=False)
                if g_resp.status_code == 200:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(g_resp.content)
                    for item in root.findall(".//item"):
                        tit_elem = item.find("title")
                        link_elem = item.find("link")
                        pub_elem = item.find("pubDate")
                        if tit_elem is not None and link_elem is not None:
                            raw_title = tit_elem.text or ""
                            link = link_elem.text or ""
                            date_str = ""
                            if pub_elem is not None and pub_elem.text:
                                try:
                                    import email.utils
                                    dt = email.utils.parsedate_to_datetime(pub_elem.text)
                                    date_str = dt.strftime("%Y-%m-%d")
                                    if dt.year < start_year or dt.year > end_year:
                                        continue
                                except Exception:
                                    pass

                            if link and link not in seen_urls:
                                seen_urls.add(link)
                                media = "온라인뉴스"
                                if " - " in raw_title:
                                    parts = raw_title.rsplit(" - ", 1)
                                    title = parts[0].strip()
                                    media = normalize_media_name(parts[1].strip())
                                else:
                                    title = raw_title.strip()

                                articles.append({
                                    "title": title,
                                    "url": link,
                                    "media": media,
                                    "date_text": date_str
                                })
                                if len(articles) >= max_items:
                                    break
            except Exception as e:
                print(f"[Google RSS Search Error] {e}")

        return articles

    @classmethod
    def fetch_full_text(cls, url: str) -> Dict[str, Any]:
        """기사 URL에 직접 접속하여 100% 원문 본문, 언론사명, 실제 발행 일자, 이메일을 추출합니다."""
        result = {"text": "", "media": "온라인뉴스", "email": None, "pub_date": None}
        if not url or not str(url).startswith("http"):
            return result

        try:
            resp = requests.get(url, headers=cls.HEADERS, timeout=8, verify=False)
            if resp.status_code == 200:
                # resp.content 전달로 BeautifulSoup이 UTF-8, EUC-KR, CP949를 자동 완벽 판별하도록 보장
                soup = BeautifulSoup(resp.content, "html.parser")

                # 언론사명 파악
                press_meta = (
                    soup.select_one("meta[property='og:article:author']") or
                    soup.select_one("meta[property='og:site_name']") or
                    soup.select_one("meta[name='twitter:creator']") or
                    soup.select_one("meta[name='dable:author']") or
                    soup.select_one(".info_cp .tit_cp, .link_cp, .media_end_head_top_logo img, .press_logo img")
                )
                if press_meta:
                    if press_meta.name == "img" and press_meta.get("alt"):
                        result["media"] = normalize_media_name(press_meta.get("alt"))
                    elif press_meta.has_attr("content"):
                        result["media"] = normalize_media_name(press_meta.get("content"))
                    else:
                        result["media"] = normalize_media_name(press_meta.get_text(strip=True))

                # 본문 영역 탐색 (네이버·다음 및 주요 언론사 최신 CMS 레이아웃 전면 지원)
                content_area = (
                    soup.select_one("#dic_area") or
                    soup.select_one("#newsct_article") or
                    soup.select_one("article#dic_area") or
                    soup.select_one("div#newsct_article") or
                    soup.select_one("section.article-view") or
                    soup.select_one("#harmonyContainer") or
                    soup.select_one("div.article_view") or
                    soup.select_one("div#article_body") or
                    soup.select_one("div.article-body") or
                    soup.select_one("div.article_body") or
                    soup.select_one("div#articeBody") or
                    soup.select_one("div.view_con") or
                    soup.select_one("div.cnt_view") or
                    soup.select_one("div.news_cnt") or
                    soup.select_one("div#article-view-content-div") or
                    soup.select_one("#articleBodyContents") or
                    soup.select_one("article") or
                    soup.select_one("div.article_body_contents")
                )

                if content_area:
                    # 필요 없는 스크립트, 광고, 캡션 제거
                    for tag in content_area(["script", "style", "figure", "figcaption", "aside", "nav", "header", "footer"]):
                        tag.decompose()
                    full_text = content_area.get_text(separator="\n", strip=True)
                else:
                    for tag in soup(["script", "style", "nav", "header", "footer", "form", "aside"]):
                        tag.decompose()
                    full_text = soup.get_text(separator="\n", strip=True)

                result["text"] = full_text

                # 실제 기사 발행 일자 추출 (30개년 연대기 타임라인 핵심)
                result["pub_date"] = cls.extract_pub_date(url, soup, full_text)

                # 이메일 추출
                email_match = re.search(PersonnelRegexParser.EMAIL_PATTERN, full_text)
                if email_match:
                    result["email"] = email_match.group(0).strip()

        except Exception as e:
            print(f"[Fetch Body Error] {e}")

        return result

    @classmethod
    def track_reporter_online(
        cls, 
        reporter_name: str,
        client_id: str = "",
        client_secret: str = "",
        stop_event: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        특정 기자의 30개년(1995~2026) 온라인 인사 발령 기사 및 바이라인 활동 기사를 실시간 검색하여
        소속 언론사, 직급/부서, 이메일, 연대기 타임라인을 파싱하고 DB에 등록합니다.
        최신 기사 정보를 놓치지 않도록 최근 연도(2024~2026) 구간은 최신순 정렬을 최우선으로 탐색합니다.
        """
        name = reporter_name.strip()
        if not name:
            return {"found_records": 0, "articles_searched": 0, "message": "기자 이름을 입력하세요."}

        # 30개년(1995~2026) 연대기 분할 구간 설정:
        intervals = [
            (2024, 2026),
            (2020, 2023),
            (2010, 2019),
            (1995, 2009)
        ]

        all_records = []
        searched_urls = set()
        article_count = 0
        known_medias = set()

        for sy, ey in intervals:
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break

            interval_queries = [
                f'[인사] "{name}"',
                f'<인사> "{name}"',
                f'[인사] 신문 "{name}"',
                f'[인사] 경제 "{name}"',
            ]
            # 이미 발견된 소속 언론사가 있다면 과거 기사 탐색 쿼리 맨 앞에 우선 배치하여 정확도 극대화
            for km in list(known_medias):
                interval_queries.insert(0, f'[인사] {km} "{name}"')

            # 최근 연도(2024~2026)는 최신순(sort_first='date')을 우선 실행하여 최신 인사 정보를 누락 없이 확보
            sort_order_for_interval = "date" if ey >= 2024 else "sim"

            for q in interval_queries:
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                arts = cls.search_online_articles(
                    q, 
                    max_items=8, 
                    client_id=client_id, 
                    client_secret=client_secret, 
                    start_year=sy, 
                    end_year=ey,
                    sort_first=sort_order_for_interval
                )
                for art in arts:
                    if stop_event and getattr(stop_event, "is_set", lambda: False)():
                        break
                    # 기사 제목이 [인사] 또는 <인사>인 인사기사만 엄격 필터링
                    if not is_personnel_article_title(art.get("title", "")):
                        continue

                    u = art["url"]
                    if u in searched_urls:
                        continue
                    searched_urls.add(u)
                    article_count += 1

                    fetched = cls.fetch_full_text(u)
                    body = fetched["text"]
                    if not body or len(body) < 30:
                        continue
                    # 기자 이름이 본문에 없는 기사는 즉시 건너뜀 (고속 최적화)
                    if name not in body:
                        continue

                    media = fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"]
                    article_pub_date = fetched.get("pub_date") or art.get("date_text") or f"{sy}-01-01"

                    # 인사 발령문 파싱
                    recs = PersonnelRegexParser.parse_article_text(
                        article_text=body,
                        default_media=media,
                        article_title=art["title"],
                        pub_date=article_pub_date,
                        article_url=u
                    )
                    matched_recs = [
                        r for r in recs 
                        if r["name"] == name and is_media_organization(r.get("media", ""))
                    ]
                    if matched_recs:
                        for mr in matched_recs:
                            if fetched["email"] and not mr.get("email"):
                                mr["email"] = fetched["email"]
                            if is_media_organization(mr.get("media", "")):
                                known_medias.add(mr["media"])
                        all_records.extend(matched_recs)

                if len(all_records) >= 30:
                    break
            if len(all_records) >= 30:
                break

        is_stopped = bool(stop_event and getattr(stop_event, "is_set", lambda: False)())
        if all_records:
            engine = PersonnelTrackerEngine()
            res = engine.process_records(all_records, append=True)
            msg_suffix = " (중단 전까지 수집된 내역 반영)" if is_stopped else ""
            return {
                "found_records": len(all_records),
                "articles_searched": article_count,
                "unique_reporters": res["unique_reporters"],
                "message": f"30개년 공식 [인사]/<인사> 기사 {article_count}건을 탐색하여 '{name}' 기자의 발령 기록 {len(all_records)}건을 실시간 수집했습니다!{msg_suffix}"
            }
        else:
            if is_stopped:
                return {
                    "found_records": 0,
                    "articles_searched": article_count,
                    "message": "검색이 중단되었습니다."
                }
            return {
                "found_records": 0,
                "articles_searched": article_count,
                "message": f"30개년 온라인 기사를 탐색했으나 제목에 [인사] 또는 <인사>가 포함된 기사에서 '{name}' 기자의 발령 기록을 찾지 못했습니다."
            }

    @classmethod
    def track_media_dept_online(
        cls,
        media: str = "",
        dept: str = "",
        rank: str = "",
        client_id: str = "",
        client_secret: str = "",
        stop_event: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        특정 매체명 및 부서명의 30개년 온라인 [인사]/<인사> 기사를 실시간 검색하여
        발령 레코드와 언론인 명단을 수집하고 DB에 등록합니다.
        """
        media_clean = media.strip()
        dept_clean = dept.strip()
        rank_clean = rank.strip()

        # 검색 쿼리 구성 ([인사] 및 <인사> 기사 대상)
        query_parts = []
        if media_clean:
            query_parts.append(media_clean)
        if dept_clean:
            query_parts.append(dept_clean)
        if rank_clean:
            query_parts.append(rank_clean)

        main_kw = " ".join(query_parts) if query_parts else "언론사"
        queries = [
            f'[인사] {main_kw}',
            f'<인사> {main_kw}',
        ]
        if media_clean:
            queries.append(f'[인사] {media_clean}')

        all_records = []
        searched_urls = set()
        article_count = 0

        for q in queries:
            if stop_event and getattr(stop_event, "is_set", lambda: False)():
                break
            arts = cls.search_online_articles(
                q, 
                max_items=12, 
                client_id=client_id, 
                client_secret=client_secret, 
                start_year=1995, 
                end_year=2026,
                sort_first="date"
            )
            for art in arts:
                if stop_event and getattr(stop_event, "is_set", lambda: False)():
                    break
                if not is_personnel_article_title(art.get("title", "")):
                    continue
                u = art["url"]
                if u in searched_urls:
                    continue
                searched_urls.add(u)
                article_count += 1

                fetched = cls.fetch_full_text(u)
                body = fetched.get("text", "")
                if not body or len(body) < 30:
                    continue

                art_media = fetched.get("media") if fetched.get("media") != "온라인뉴스" else (media_clean or art.get("media"))
                pub_date = fetched.get("pub_date") or art.get("date_text") or "2020-01-01"

                recs = PersonnelRegexParser.parse_article_text(
                    article_text=body,
                    default_media=art_media,
                    article_title=art["title"],
                    pub_date=pub_date,
                    article_url=u
                )
                for r in recs:
                    if not is_media_organization(r.get("media", "")):
                        continue
                    if media_clean and media_clean not in r.get("media", ""):
                        if media_clean in art["title"]:
                            r["media"] = normalize_media_name(media_clean)
                        else:
                            continue
                    if dept_clean and dept_clean not in r.get("dept", ""):
                        continue
                    if rank_clean and rank_clean not in r.get("rank_title", ""):
                        continue
                    all_records.append(r)

            if len(all_records) >= 20:
                break

        is_stopped = bool(stop_event and getattr(stop_event, "is_set", lambda: False)())
        if all_records:
            engine = PersonnelTrackerEngine()
            res = engine.process_records(all_records, append=True)
            msg_suffix = " (중단 전까지 수집된 내역 반영)" if is_stopped else ""
            return {
                "found_records": len(all_records),
                "articles_searched": article_count,
                "unique_reporters": res["unique_reporters"],
                "message": f"온라인 [인사]/<인사> 기사 {article_count}건에서 {len(all_records)}건의 발령 기록(언론인 {res['unique_reporters']}명)을 실시간 수집했습니다!{msg_suffix}"
            }
        else:
            if is_stopped:
                return {
                    "found_records": 0,
                    "articles_searched": article_count,
                    "message": "검색이 중단되었습니다."
                }
            return {
                "found_records": 0,
                "articles_searched": article_count,
                "message": f"온라인 인사 기사 {article_count}건을 탐색했으나 해당 조건의 발령 기록을 찾지 못했습니다."
            }

    @classmethod
    def fetch_latest_online_personnel(
        cls, 
        max_articles: int = 15,
        client_id: str = "",
        client_secret: str = ""
    ) -> Dict[str, Any]:
        """최신 온라인 인사 기사 전문을 대량 수집하여 파싱 및 DB에 등록합니다."""
        arts = cls.search_online_articles("[인사]", max_items=max_articles, client_id=client_id, client_secret=client_secret)
        all_records = []
        for art in arts:
            fetched = cls.fetch_full_text(art["url"])
            body = fetched["text"]
            media = fetched["media"] if fetched["media"] != "온라인뉴스" else art["media"]
            if body and len(body) >= 50:
                recs = PersonnelRegexParser.parse_article_text(
                    article_text=body,
                    default_media=media,
                    article_title=art["title"],
                    pub_date=art.get("date_text", str(datetime.date.today()))
                )
                for r in recs:
                    if fetched["email"] and not r.get("email"):
                        r["email"] = fetched["email"]
                all_records.extend(recs)

        if all_records:
            engine = PersonnelTrackerEngine()
            res = engine.process_records(all_records, append=True)
            return {
                "articles_count": len(arts),
                "records_extracted": len(all_records),
                "unique_reporters": res["unique_reporters"],
                "job_changes": res["job_changes_detected"],
                "message": f"최신 온라인 기사 {len(arts)}건에서 {len(all_records)}건의 발령 기록(총 언론인 {res['unique_reporters']}명)을 실시간 수집 및 등록 완료했습니다!"
            }
        else:
            return {
                "articles_count": len(arts),
                "records_extracted": 0,
                "message": "온라인 기사를 가져왔으나 발령 명단을 추출하지 못했습니다."
            }

    @classmethod
    def fetch_daily_personnel_news(
        cls,
        target_date: str = "",
        max_articles: int = 25,
        client_id: str = "",
        client_secret: str = ""
    ) -> Dict[str, Any]:
        """
        당일(또는 지정된 일자)의 공식 [인사]/<인사> 기사를 실시간 크롤링하여
        언론사별, 발령구분(승진/전보/보임/선임)별로 구조화된 일일 인사 소식 브리핑 데이터를 생성하고
        DB에 영속화합니다.
        """
        if not target_date:
            target_date = datetime.date.today().strftime("%Y-%m-%d")

        search_queries = [
            '"[인사]"',
            '"<인사>"',
            '[인사] 신문',
            '<인사> 신문',
            '[인사] 방송',
            '<인사> 방송',
            '[인사] 통신',
            '<인사> 통신',
            '[인사] 일보',
            '<인사> 일보',
            '[인사] 경제',
            '<인사> 경제',
            '[인사] 미디어',
            '<인사> 미디어',
            '[인사] 뉴스',
            '<인사> 뉴스',
            '[인사]',
            '<인사>'
        ]

        seen_urls = set()
        collected_articles = []

        for q in search_queries:
            if len(collected_articles) >= max_articles * 2:
                break
            arts = cls.search_online_articles(
                q, 
                max_items=12, 
                client_id=client_id, 
                client_secret=client_secret,
                start_year=2024,
                end_year=2026,
                sort_first="date"
            )
            for a in arts:
                tit = a.get("title", "")
                if not is_personnel_article_title(tit):
                    continue
                u = a.get("url", "")
                if not u or u in seen_urls:
                    continue
                seen_urls.add(u)
                collected_articles.append(a)

        all_records = []
        media_news_map = {}

        for a in collected_articles[:max_articles]:
            fetched = cls.fetch_full_text(a["url"])
            body = fetched.get("text", "")
            if not body or len(body) < 30:
                continue

            pub_date = fetched.get("pub_date") or a.get("date_text") or target_date
            art_media = fetched.get("media") if fetched.get("media") != "온라인뉴스" else a.get("media")

            recs = PersonnelRegexParser.parse_article_text(
                article_text=body,
                default_media=art_media,
                article_title=a["title"],
                pub_date=pub_date,
                article_url=a["url"]
            )
            clean_recs = [
                r for r in recs 
                if is_personnel_article_title(r.get("article_title", ""))
                and is_media_organization(r.get("media", ""))
                and r["name"] not in STOPWORDS and len(r["name"]) >= 2
            ]
            if not clean_recs:
                continue

            # 개별 발령 레코드의 매체별로 올바르게 그룹화 (종합 인사 기사 내 복수 매체 지원)
            for r in clean_recs:
                r_media = r.get("media", art_media)
                if r_media not in media_news_map:
                    media_news_map[r_media] = {
                        "media": r_media,
                        "articles": [],
                        "records": [],
                        "by_action": {},
                        "latest_date": pub_date,
                        "article_title": a["title"],
                        "article_url": a["url"]
                    }
                media_news_map[r_media]["records"].append(r)
                act = r.get("action_type", "발령")
                if act not in media_news_map[r_media]["by_action"]:
                    media_news_map[r_media]["by_action"][act] = []
                media_news_map[r_media]["by_action"][act].append(r)

            all_records.extend(clean_recs)

        job_changes_detected = []
        if all_records:
            engine = PersonnelTrackerEngine()
            res = engine.process_records(all_records, append=True)
            job_changes_detected = engine.db.get_job_changes(limit=50)

        return {
            "target_date": target_date,
            "total_articles": len([a for m in media_news_map.values() for a in m["articles"]]),
            "total_media": len(media_news_map),
            "total_records": len(all_records),
            "media_news": list(media_news_map.values()),
            "all_records": all_records,
            "job_changes": job_changes_detected,
            "message": f"당일 및 최신 인사 기사에서 {len(media_news_map)}개 언론사, 총 {len(all_records)}명의 발령 소식을 성공적으로 정리했습니다."
        }



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
                    reporter_id TEXT,
                    article_title TEXT,
                    article_url TEXT
                )
            """)
            try:
                cursor.execute("ALTER TABLE personnel_records ADD COLUMN article_title TEXT")
            except Exception:
                pass
            try:
                cursor.execute("ALTER TABLE personnel_records ADD COLUMN article_url TEXT")
            except Exception:
                pass

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

    def clear_all_data(self):
        """데이터베이스의 모든 인사 발령 및 기자 데이터를 완전히 삭제합니다."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM personnel_records")
            cursor.execute("DELETE FROM reporters")
            cursor.execute("DELETE FROM job_changes")
            cursor.execute("DELETE FROM articles")
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
                    (pub_date, media, dept, domain, rank_title, rank_level, name, action_type, email, raw_text, reporter_id, article_title, article_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    r.get("reporter_id", ""),
                    r.get("article_title", ""),
                    r.get("article_url", "")
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

    def get_all_raw_records(self) -> List[Dict[str, Any]]:
        """DB에 저장된 기존 모든 발령 레코드를 가져옵니다."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM personnel_records ORDER BY pub_date ASC, id ASC")
            return [dict(row) for row in cursor.fetchall()]

    def get_all_reporters(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM reporters ORDER BY name ASC")
            return [dict(row) for row in cursor.fetchall()]

    @staticmethod
    def deduplicate_timeline_records(records: List[Dict[str, Any]], window_days: int = 90, sort_desc: bool = False) -> List[Dict[str, Any]]:
        """
        일정 기간(기본 90일) 동안 동일한 발령(동일 인물, 동일 매체, 부서, 직급)이 여러 기사로 보도되어
        중복 수집된 경우, 가장 빠른 기사 발행 일자(Earliest Article Date)를 기준으로 단일 이력으로 병합합니다.
        """
        if not records:
            return []

        def _dept_key(d: str) -> str:
            s = re.sub(r"[〃\s]+", "", str(d).strip())
            if s.endswith("부") and len(s) > 2:
                return s[:-1]
            return s
        
        # 날짜 오름차순 정렬 (과거 -> 현재)
        sorted_records = sorted(records, key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10])
        merged: List[Dict[str, Any]] = []

        for rec in sorted_records:
            rec_copy = dict(rec)
            r_date_str = str(rec_copy.get("pub_date", ""))[:10]
            try:
                r_date = datetime.datetime.strptime(r_date_str, "%Y-%m-%d").date()
            except Exception:
                r_date = None

            matched_idx = -1
            for idx, m in enumerate(merged):
                same_name = (not m.get("name") or not rec_copy.get("name") or m.get("name", "").strip() == rec_copy.get("name", "").strip())
                same_media = normalize_media_name(m.get("media", "")) == normalize_media_name(rec_copy.get("media", ""))
                same_dept = _dept_key(m.get("dept", "")) == _dept_key(rec_copy.get("dept", ""))
                same_rank = m.get("rank_title", "").strip() == rec_copy.get("rank_title", "").strip()

                if same_name and same_media and same_dept and same_rank:
                    m_date_str = str(m.get("pub_date", ""))[:10]
                    try:
                        m_date = datetime.datetime.strptime(m_date_str, "%Y-%m-%d").date()
                    except Exception:
                        m_date = None

                    if r_date and m_date:
                        if abs((r_date - m_date).days) <= window_days:
                            matched_idx = idx
                            break
                    else:
                        matched_idx = idx
                        break

            if matched_idx >= 0:
                existing = merged[matched_idx]
                e_date_str = str(existing.get("pub_date", ""))[:10]
                # 동일한 인사는 가장 빠른 기사 일자(Earliest Date) 적용
                if r_date_str and (not e_date_str or r_date_str < e_date_str):
                    existing["pub_date"] = r_date_str
                    if rec_copy.get("article_url"):
                        existing["article_url"] = rec_copy["article_url"]
                    if rec_copy.get("article_title"):
                        existing["article_title"] = rec_copy["article_title"]
                elif not existing.get("article_url") and rec_copy.get("article_url"):
                    existing["article_url"] = rec_copy["article_url"]
                    existing["article_title"] = rec_copy.get("article_title", "")
                
                # 부서명이 더 상세하고 정돈된 경우 갱신
                cur_dept = str(existing.get("dept", ""))
                new_dept = str(rec_copy.get("dept", ""))
                if "부" in new_dept and "부" not in cur_dept:
                    existing["dept"] = new_dept
                elif len(new_dept) > len(cur_dept) and "〃" not in new_dept:
                    existing["dept"] = new_dept

                # 발령구분이 더 구체적인 경우 갱신
                if existing.get("action_type") in ["발령", ""] and rec_copy.get("action_type") not in ["발령", ""]:
                    existing["action_type"] = rec_copy["action_type"]
            else:
                merged.append(rec_copy)

        if sort_desc:
            merged.sort(key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10], reverse=True)
        else:
            merged.sort(key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10])
        return merged

    def get_reporter_timeline(self, reporter_id: str) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM personnel_records
                WHERE reporter_id = ?
                ORDER BY pub_date ASC, id ASC
            """, (reporter_id,))
            rows = [dict(row) for row in cursor.fetchall()]
            return self.deduplicate_timeline_records(rows, sort_desc=True)

    def search_reporters(self, name: str = "", media: str = "", dept: str = "", rank: str = "") -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = """
                SELECT DISTINCT r.* FROM reporters r
                LEFT JOIN personnel_records pr ON r.reporter_id = pr.reporter_id
                WHERE 1=1
            """
            params = []
            if name:
                query += " AND r.name LIKE ?"
                params.append(f"%{name}%")
            if media:
                query += " AND (pr.media LIKE ? OR r.current_media LIKE ?)"
                params.extend([f"%{media}%", f"%{media}%"])
            if dept:
                query += " AND (pr.dept LIKE ? OR r.current_dept LIKE ?)"
                params.extend([f"%{dept}%", f"%{dept}%"])
            if rank:
                query += " AND (pr.rank_title LIKE ? OR r.current_rank LIKE ?)"
                params.extend([f"%{rank}%", f"%{rank}%"])

            query += " ORDER BY r.last_seen DESC"
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def search_records(self, name: str = "", media: str = "", dept: str = "", rank: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM personnel_records WHERE 1=1"
            params = []
            if name:
                query += " AND name LIKE ?"
                params.append(f"%{name}%")
            if media:
                query += " AND media LIKE ?"
                params.append(f"%{media}%")
            if dept:
                query += " AND dept LIKE ?"
                params.append(f"%{dept}%")
            if rank:
                query += " AND rank_title LIKE ?"
                params.append(f"%{rank}%")
            query += " ORDER BY pub_date DESC, id DESC LIMIT ?"
            params.append(limit)
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

    def get_daily_personnel_summary(self, target_date: str = "") -> Dict[str, Any]:
        """
        DB에 저장된 발령 레코드 중 당일(또는 지정된 일자/최근 일자)의 데이터를
        언론사별 및 발령구분(승진/전보/보임 등)별로 구조화하여 반환합니다.
        제목에 [인사] 또는 <인사>가 포함된 공식 인사 기사만 엄격 필터링하여 분석합니다.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if target_date:
                cursor.execute("""
                    SELECT * FROM personnel_records
                    WHERE pub_date LIKE ?
                    ORDER BY pub_date DESC, media ASC, id ASC
                """, (f"{target_date}%",))
            else:
                cursor.execute("""
                    SELECT * FROM personnel_records
                    ORDER BY pub_date DESC, media ASC, id ASC
                    LIMIT 500
                """)
            raw_rows = [dict(row) for row in cursor.fetchall()]

        # [인사] 또는 <인사> 공식 기사이자 언론사 발령 레코드만 엄격 필터링
        rows = [
            r for r in raw_rows 
            if is_media_organization(r.get("media", "")) and (not r.get("article_title") or is_personnel_article_title(r.get("article_title", "")))
        ]

        grouped = {}
        for r in rows:
            med = normalize_media_name(r.get("media", "미상"))
            r["media"] = med
            if med not in grouped:
                grouped[med] = {
                    "media": med,
                    "records": [],
                    "by_action": {},
                    "latest_date": r.get("pub_date", ""),
                    "article_title": r.get("article_title", ""),
                    "article_url": r.get("article_url", "")
                }
            grouped[med]["records"].append(r)
            act = r.get("action_type", "발령")
            if act not in grouped[med]["by_action"]:
                grouped[med]["by_action"][act] = []
            grouped[med]["by_action"][act].append(r)
            if not grouped[med]["article_url"] and r.get("article_url"):
                grouped[med]["article_url"] = r.get("article_url", "")
                grouped[med]["article_title"] = r.get("article_title", "")

        return {
            "total_records": len(rows),
            "media_count": len(grouped),
            "media_news": list(grouped.values()),
            "raw_records": rows
        }



# ==========================================
# 6. 빅카인즈 파일 로더 및 일괄 처리 인터페이스
# ==========================================

def _fetch_full_text_from_url(url: str) -> Optional[str]:
    """기사 URL에서 원문 본문을 자동 추출하여 200자 제한을 보완합니다."""
    if not url or not str(url).startswith("http"):
        return None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(str(url).strip(), headers=headers, timeout=4)
        if resp.status_code == 200:
            html = resp.text
            clean = re.sub(r"<(script|style|header|footer|nav).*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
            clean = re.sub(r"<[^>]+>", " ", clean)
            clean = re.sub(r"&[a-z]+;", " ", clean)
            clean = re.sub(r"\s+", " ", clean).strip()
            if any(k in clean for k in ["승진", "전보", "부장", "차장", "국장", "논설위원"]):
                return clean[:10000]
    except Exception:
        pass
    return None


def load_and_process_bigkinds_file(file_path_or_buffer, append: bool = True) -> Dict[str, Any]:
    """
    빅카인즈 다운로드 표준 엑셀/CSV 파일을 읽고,
    인사 발령 텍스트를 파싱하여 데이터베이스에 저장(또는 누적)합니다.
    """
    df = None

    # 1. 파일 읽기
    if isinstance(file_path_or_buffer, str):
        if file_path_or_buffer.endswith(".csv"):
            df = pd.read_csv(file_path_or_buffer, encoding="utf-8-sig")
        else:
            try:
                df = pd.read_excel(file_path_or_buffer)
            except Exception:
                df = pd.read_excel(file_path_or_buffer, engine="openpyxl")
    else:
        content_bytes = file_path_or_buffer.read()
        file_path_or_buffer.seek(0)
        try:
            df = pd.read_excel(io.BytesIO(content_bytes))
        except Exception:
            try:
                df = pd.read_csv(io.BytesIO(content_bytes), encoding="utf-8-sig")
            except Exception:
                df = pd.read_csv(io.BytesIO(content_bytes), encoding="cp949")

    if df is None or df.empty:
        raise ValueError("업로드된 파일이 비어있거나 읽을 수 없는 형식입니다.")

    # 2. 헤더 행 자동 탐색 (상단 메타데이터 행이 있는 경우)
    keywords = ["일자", "언론사", "본문", "기사원문", "제목"]
    has_valid_header = any(any(kw in str(c) for kw in keywords) for c in df.columns)

    if not has_valid_header:
        for row_idx in range(min(10, len(df))):
            row_vals = [str(v) for v in df.iloc[row_idx].values]
            match_count = sum(1 for kw in keywords if any(kw in v for v in row_vals))
            if match_count >= 2:
                df.columns = df.iloc[row_idx].values
                df = df.iloc[row_idx + 1:].reset_index(drop=True)
                break

    # 3. 빅카인즈 컬럼명 유연한 매핑
    col_map = {}
    for col in df.columns:
        c_clean = str(col).strip()
        if any(k in c_clean for k in ["일자", "날짜", "date", "등록일"]):
            col_map[col] = "일자"
        elif any(k in c_clean for k in ["언론사", "매체", "출처", "media", "언론사명"]):
            col_map[col] = "언론사"
        elif any(k in c_clean for k in ["제목", "기사제목", "title", "헤드라인"]):
            col_map[col] = "제목"
        elif any(k in c_clean for k in ["본문", "기사원문", "내용", "본문(200자)", "기사본문", "요약"]):
            col_map[col] = "본문"
        elif any(k in c_clean for k in ["URL", "url", "링크", "link", "기사링크"]):
            col_map[col] = "URL"

    df = df.rename(columns=col_map)
    required = ["일자", "언론사", "본문"]
    for req in required:
        if req not in df.columns:
            raise ValueError(f"빅카인즈 필수 컬럼 '{req}'을 찾을 수 없습니다. (현재 인식된 컬럼: {list(df.columns)})")

    # 4. 행별 인사 발령 레코드 추출
    all_extracted_records = []
    for _, row in df.iterrows():
        pub_date = str(row["일자"]).strip()
        if len(pub_date) == 8 and pub_date.isdigit():
            pub_date = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:]}"

        media = str(row["언론사"]).strip()
        title = str(row.get("제목", "")).strip()
        # [인사] 또는 <인사> 기사만 엄격 필터링
        if title and not is_personnel_article_title(title):
            continue

        body = str(row["본문"]).strip()
        url = str(row.get("URL", "")).strip()

        # 200자 제한 보완: 본문이 짧고 URL이 있는 경우 원문 스크랩 시도
        if len(body) < 250 and url.startswith("http"):
            full_body = _fetch_full_text_from_url(url)
            if full_body and len(full_body) > len(body):
                body = full_body

        records = PersonnelRegexParser.parse_article_text(
            article_text=body,
            default_media=media,
            article_title=title,
            pub_date=pub_date,
            article_url=url
        )
        all_extracted_records.extend(records)

    if not all_extracted_records:
        raise ValueError("파일 내 기사 본문에서 [부서명] [직책] [이름] 형태의 인사 발령 텍스트를 찾지 못했습니다. 기사 본문에 발령 내용이 포함되어 있는지 확인해주세요.")

    engine = PersonnelTrackerEngine()
    result = engine.process_records(all_extracted_records, append=append)
    return result


# ==========================================
# 7. 30개년 커리어 프로필 매트릭스 및 엑셀 리포트 빌더
# ==========================================

def generate_career_profile_metrics(timeline: List[Dict[str, Any]], reporter_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    30개년 시계열 발령/활동 데이터를 기반으로 종합 커리어 프로필 매트릭스를 분석합니다:
    - 활동 기간 및 연차 산정 (Career Span)
    - 연도별 소속 매체 이동 로드맵 (Affiliation Journey)
    - 직급 승진 사다리 (Progression Ladder)
    - 출입처 전문 도메인 분포 (Domain Expertise)
    - 주요 발령 마일스톤 집계
    """
    if not timeline:
        return {}

    # 1. 시계열 정렬 (오름차순)
    sorted_items = sorted(
        timeline,
        key=lambda x: str(x.get("pub_date", "1970-01-01"))
    )

    first_date_str = str(sorted_items[0].get("pub_date", ""))[:10]
    last_date_str = str(sorted_items[-1].get("pub_date", ""))[:10]

    try:
        first_year = int(first_date_str[:4])
        last_year = int(last_date_str[:4])
        career_years = max(1, last_year - first_year + 1)
    except Exception:
        first_year = 1995
        last_year = 2026
        career_years = 1

    # 2. 소속 매체 이동 로드맵 (Affiliation Journey)
    # 연속된 동일 매체는 기간(start_year ~ end_year)으로 묶음
    affiliation_journey = []
    current_aff = None
    for item in sorted_items:
        m = item.get("media", "").strip()
        y_str = str(item.get("pub_date", ""))[:4]
        try:
            y = int(y_str)
        except Exception:
            y = first_year

        if not current_aff or current_aff["media"] != m:
            if current_aff:
                affiliation_journey.append(current_aff)
            current_aff = {
                "media": m,
                "start_year": y,
                "end_year": y,
                "last_rank": item.get("rank_title", "기자"),
                "last_dept": item.get("dept", "본사")
            }
        else:
            current_aff["end_year"] = max(current_aff["end_year"], y)
            current_aff["last_rank"] = item.get("rank_title", current_aff["last_rank"])
            current_aff["last_dept"] = item.get("dept", current_aff["last_dept"])
    if current_aff:
        affiliation_journey.append(current_aff)

    # 3. 직급 승진 사다리 (Progression Ladder)
    promotion_ladder = []
    seen_ranks = set()
    for item in sorted_items:
        r_title = item.get("rank_title", "").strip()
        if not r_title:
            continue
        r_level = item.get("rank_level", 2)
        y_str = str(item.get("pub_date", ""))[:4]
        if r_title not in seen_ranks:
            seen_ranks.add(r_title)
            promotion_ladder.append({
                "rank_title": r_title,
                "rank_level": r_level,
                "year": y_str,
                "media": item.get("media", ""),
                "dept": item.get("dept", "")
            })
    promotion_ladder = sorted(promotion_ladder, key=lambda x: (x["year"], x["rank_level"]))

    # 4. 출입처 도메인 전문성 분석 (Domain Expertise Breakdown)
    domain_counts: Dict[str, int] = {}
    for item in sorted_items:
        d = item.get("domain", "일반").strip()
        domain_counts[d] = domain_counts.get(d, 0) + 1

    total_cnt = len(sorted_items)
    domain_breakdown = [
        {"domain": d, "count": c, "percentage": round((c / total_cnt) * 100, 1)}
        for d, c in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    primary_domain = domain_breakdown[0]["domain"] if domain_breakdown else "일반"

    # 5. 이직 및 발령 통계
    job_change_count = sum(1 for item in sorted_items if "이직" in item.get("action_type", ""))
    promotion_count = sum(1 for item in sorted_items if "승진" in item.get("action_type", ""))

    return {
        "first_year": first_year,
        "last_year": last_year,
        "career_years": career_years,
        "first_date": first_date_str,
        "last_date": last_date_str,
        "total_records": total_cnt,
        "job_change_count": job_change_count,
        "promotion_count": promotion_count,
        "affiliation_journey": affiliation_journey,
        "promotion_ladder": promotion_ladder,
        "domain_breakdown": domain_breakdown,
        "primary_domain": primary_domain
    }


def generate_career_excel(
    reporter_obj: Dict[str, Any],
    timeline: List[Dict[str, Any]],
    job_changes: List[Dict[str, Any]]
) -> bytes:
    """
    인물의 30개년 커리어 프로필, 연대기 타임라인, 이직 히스토리를
    정돈된 서식의 다중 시트 Excel(.xlsx) 바이트 스트림으로 생성합니다.
    """
    output = io.BytesIO()
    metrics = generate_career_profile_metrics(timeline, reporter_obj)

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Sheet 1: 프로필 요약
        summary_data = [
            {"항목": "언론인 성명", "내용": reporter_obj.get("name", "")},
            {"항목": "현재 소속 언론사", "내용": reporter_obj.get("current_media", "")},
            {"항목": "현재 부서", "내용": reporter_obj.get("current_dept", "")},
            {"항목": "현재 직급", "내용": reporter_obj.get("current_rank", "")},
            {"항목": "추적 활동 기간", "내용": f"{metrics.get('first_date', '')} ~ {metrics.get('last_date', '')}"},
            {"항목": "총 경력 연차", "내용": f"총 {metrics.get('career_years', 1)}년차 ({metrics.get('first_year', '')}년 ~ {metrics.get('last_year', '')}년)"},
            {"항목": "주요 출입처 전문분야", "내용": metrics.get("primary_domain", "일반")},
            {"항목": "바이라인 이메일", "내용": reporter_obj.get("email", "") or "미등록"},
            {"항목": "소속 매체 이동 경로", "내용": " ➔ ".join([f"{a['media']}({a['start_year']}~{a['end_year']})" for a in metrics.get("affiliation_journey", [])])},
            {"항목": "매체 간 이직 횟수", "내용": f"{metrics.get('job_change_count', 0)}회"},
            {"항목": "승진 발령 횟수", "내용": f"{metrics.get('promotion_count', 0)}회"},
            {"항목": "총 수집 발령/활동 건수", "내용": f"{metrics.get('total_records', len(timeline))}건"},
            {"항목": "고유 식별자(ID)", "내용": reporter_obj.get("reporter_id", "")},
            {"항목": "프로필 생성일시", "내용": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        ]
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name="1_커리어_프로필_요약", index=False)

        # Sheet 2: 30개년 발령 및 활동 연대기
        timeline_rows = []
        for t in timeline:
            p_date = str(t.get("pub_date", ""))
            y_str = p_date[:4] if len(p_date) >= 4 else ""
            timeline_rows.append({
                "발령일자": p_date,
                "연도": y_str,
                "언론사": t.get("media", ""),
                "부서": t.get("dept", ""),
                "직급": t.get("rank_title", ""),
                "직급서열": t.get("rank_level", 2),
                "발령구분": t.get("action_type", "발령"),
                "출입처도메인": t.get("domain", "일반"),
                "기사제목": t.get("article_title", ""),
                "기사원문링크": t.get("article_url", ""),
                "원문텍스트": t.get("raw_text", "")
            })
        df_timeline = pd.DataFrame(timeline_rows)
        if not df_timeline.empty:
            df_timeline = df_timeline.sort_values(by="발령일자", ascending=True)
        df_timeline.to_excel(writer, sheet_name="2_30개년_발령_연대기", index=False)

        # Sheet 3: 이직 히스토리
        if job_changes:
            jc_rows = []
            for jc in job_changes:
                jc_rows.append({
                    "이직발령일자": jc.get("to_date", ""),
                    "이전 언론사": jc.get("from_media", ""),
                    "이전 부서": jc.get("from_dept", ""),
                    "이전 직급": jc.get("from_rank", ""),
                    "이직후 언론사": jc.get("to_media", ""),
                    "이직후 부서": jc.get("to_dept", ""),
                    "이직후 직급": jc.get("to_rank", ""),
                    "경과일수": jc.get("days_gap", 0),
                    "알고리즘신뢰도": jc.get("confidence_score", 0.0),
                    "탐지근거": jc.get("detected_reason", "")
                })
            df_jc = pd.DataFrame(jc_rows)
            df_jc.to_excel(writer, sheet_name="3_매체간_이직_기록", index=False)

    return output.getvalue()
