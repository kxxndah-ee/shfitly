import sqlite3
import re
from typing import List, Dict, Any, Optional

DB_NAME = "media_personnel.db"

# ---------------------------------------------------------
# 1. 직급 서열 점수 매핑 (이직 시 직급 유지/상승 판정 기준)
# ---------------------------------------------------------
RANK_SCORE_MAP = {
    # 수습 / 평기자 (1점)
    "수습기자": 1, "수습": 1, "기자": 1, "업무직": 1, "업무원": 1, "사원": 1,
    # 대우 / 차장급 (2~3점)
    "차장대우": 2, "선임기자": 2, "전문기자": 2,
    "차장": 3, "팀장대우": 3,
    # 부장급 (4~5점)
    "부장대우": 4, "부팀장": 4,
    "부장": 5, "팀장": 5, "데스크": 5, "에디터": 5,
    # 국장급 (6~7점)
    "부국장대우": 6, "부국장": 6, "에디터국장": 6,
    "국장": 7, "편집국장": 7, "보도국장": 7, "본부장": 7, "실장": 7, "논설위원": 7,
    # 경영진 (8~9점)
    "논설주간": 8, "상무": 8, "전무": 8, "이사": 8, "주필": 8,
    "부사장": 9, "사장": 9, "대표이사": 9, "발행인": 9, "회장": 9
}

# ---------------------------------------------------------
# 2. 부서 카테고리 매핑 (유사 부서 판별 기준)
# ---------------------------------------------------------
DEPT_CATEGORIES = {
    "정치/행정": ["정치", "청와대", "대통령실", "국회", "통일", "외교", "안보", "행정"],
    "경제/산업/금융": ["경제", "산업", "금융", "증권", "부동산", "재계", "유통", "중기", "벤처", "IT", "과학", "테크"],
    "사회/법조": ["사회", "법조", "검찰", "경찰", "법원", "시민사회", "노동", "환경", "교육"],
    "문화/연예/스포츠": ["문화", "연예", "스포츠", "엔터", "대중문화", "학술", "출판"],
    "국제/외신": ["국제", "외신", "글로벌", "워싱턴", "베이징", "도쿄"],
    "디지털/뉴미디어": ["디지털", "뉴미디어", "온라인", "영상", "플랫폼", "콘텐츠"],
    "논설/해설": ["논설", "해설", "사설", "오피니언"],
    "편집/교열/사진": ["편집", "교열", "사진", "포토", "그래픽", "미디어제작"]
}

# 조직/부서 단위 제외 키워드 (인명 오인식 방지)
ORG_UNIT_SUFFIXES = ["팀", "실", "국", "부", "처", "센터", "단", "소", "파트", "그룹"]
INVALID_NAME_WORDS = {"감사팀", "감사실", "법무실", "기획실", "비서실", "총무팀", "인사팀", "인사", "동정", "알림", "본사", "지사", "편집장"}


def normalize_rank_score(position_raw: str) -> int:
    """직급 문자열을 기반으로 서열 점수 계산 (매칭 안 될 시 기본 1점)"""
    pos = position_raw.strip()
    for rank_title, score in sorted(RANK_SCORE_MAP.items(), key=lambda x: -len(x[0])):
        if rank_title in pos:
            return score
    return 1


def categorize_dept(dept_raw: str) -> str:
    """부서 문자열을 표준 카테고리로 분류"""
    if not dept_raw:
        return "미분류"
    dept = dept_raw.strip()
    for cat, keywords in DEPT_CATEGORIES.items():
        if any(kw in dept for kw in keywords):
            return cat
    return "기타"


def clean_name(name_raw: str) -> str:
    """괄호, 공백, 특수문자 제거 후 2~4글자 한글 이름 정규화 (조직명 배제)"""
    name = re.sub(r"\(.*?\)|\[.*?\]", "", name_raw).strip()
    name = re.sub(r"[^가-힣]", "", name)
    if name in INVALID_NAME_WORDS:
        return ""
    if any(name.endswith(sfx) for sfx in ORG_UNIT_SUFFIXES):
        return ""
    if 2 <= len(name) <= 4:
        return name
    return ""


# ---------------------------------------------------------
# 3. 인사 기사 본문 파서 엔진
# ---------------------------------------------------------
def parse_article_content(content: str, default_company: str, event_date: str) -> List[Dict[str, Any]]:
    """
    언론사 인사 기사의 전형적인 계층 구조 파싱:
    대분류(발령 구분/언론사): ◇ 승진, ◇ 조선일보, [전보]
    중분류(부서/직급): ▲ 정치부장, ◎ 사회부
    소분류(성명): 홍길동 김철수
    """
    records = []
    lines = [line.strip() for line in content.split("\n") if line.strip()]

    current_action = "발령"
    current_dept = ""
    current_pos = ""
    target_company = default_company

    for line in lines:
        # 1) 최상위 헤더 감지 (예: ◇ 조선일보, ◇ 승진, 【전보】)
        top_match = re.match(r"^[◇◆■□【\[<](.*?)[】\]>◇◆■□]?$", line)
        if top_match:
            header_text = top_match.group(1).strip()
            
            # 발령 구분 키워드 검사
            if any(act in header_text for act in ["승진", "전보", "보직", "전입", "파견", "복귀", "전출"]):
                current_action = header_text
                current_dept = ""
                current_pos = ""
                continue
            
            # 언론사명이 포함된 경우 타깃 언론사 갱신
            if "일보" in header_text or "신문" in header_text or "경제" in header_text or "방송" in header_text or "뉴스" in header_text or "미디어" in header_text or "그룹" in header_text or "홀딩스" in header_text:
                clean_target = re.sub(r"[^\w]", "", header_text.split("인사")[0])
                if clean_target:
                    target_company = clean_target
                current_dept = ""
                current_pos = ""
                continue

        # 2) 본문 항목 줄 파싱 (기호: ▲, △, ◎, ▷, ● 또는 텍스트)
        item_text = re.sub(r"^[▲△◎▷●○•·-]\s*", "", line).strip()
        if not item_text:
            continue

        # '직급/부서'와 '이름 목록' 분리
        tokens = item_text.split()
        if len(tokens) < 2:
            continue

        # 뒤에서부터 이름을 탐색
        names_found = []
        split_idx = len(tokens)

        for i in range(len(tokens) - 1, -1, -1):
            cand_name = clean_name(tokens[i])
            if cand_name and not any(kw in cand_name for kw in ["부장", "차장", "기자", "국장", "위원", "팀장", "실장", "본부장", "대표", "주간", "주필"]):
                names_found.insert(0, cand_name)
                split_idx = i
            else:
                break

        if not names_found or split_idx == 0:
            continue

        role_desc = " ".join(tokens[:split_idx]).strip()

        # 직급 및 부서 추출 정규식
        pos_match = re.search(r"(수습기자|기자|차장대우|부장대우|부국장대우|차장|부장|부국장|국장|팀장|에디터|데스크|논설위원|논설주간|주필|본부장|실장|상무|전무|사장)", role_desc)
        
        if pos_match:
            pos = pos_match.group(1)
            dept = role_desc[:pos_match.start()].strip()
            # 부서명이 비어있을 경우 직전 부서 컨텍스트 유지
            if not dept and current_dept:
                dept = current_dept
            elif dept:
                current_dept = dept
            current_pos = pos
        else:
            # 부장, 차장 등 직급 언급이 없으면 모두 기자로 분류
            pos = "기자"
            dept = role_desc if role_desc else current_dept

        rank_score = normalize_rank_score(pos)
        dept_cat = categorize_dept(dept)

        for name in names_found:
            records.append({
                "person_name": name,
                "company_name": target_company,
                "department_raw": dept,
                "dept_category": dept_cat,
                "position_raw": pos,
                "rank_score": rank_score,
                "action_type": current_action,
                "event_date": event_date
            })

    return records


# ---------------------------------------------------------
# 4. 일괄 파싱 및 DB 적재 파이프라인
# ---------------------------------------------------------
def process_unparsed_articles(batch_size: int = 500):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # 필요 테이블 생성 보장
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
        event_date DATE NOT NULL
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_career_person ON career_events(person_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_career_company ON career_events(company_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_career_date ON career_events(event_date);")

    # 이미 파싱된 기사(article_id)는 제외하고 언론사 기사만 조회
    query = """
    SELECT r.article_id, r.media_source, r.title, r.content, r.publish_date, r.target_media
    FROM raw_articles r
    LEFT JOIN career_events c ON r.article_id = c.article_id
    WHERE r.is_media_company = 1
      AND c.event_id IS NULL
    LIMIT ?;
    """
    cur.execute(query, (batch_size,))
    articles = cur.fetchall()

    if not articles:
        print("[완료] 새로 파싱할 언론사 인사 기사가 없습니다.")
        conn.close()
        return 0

    print(f"총 {len(articles)}건의 기사 파싱 시작...")
    total_events_saved = 0

    for art in articles:
        art_id, source, title, content, pub_date, target_media = art
        fallback_company = target_media if target_media else "미상언론사"

        parsed_records = parse_article_content(content, fallback_company, pub_date)

        for rec in parsed_records:
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
            total_events_saved += 1

    conn.commit()
    conn.close()

    print(f"파싱 완료: {len(articles)}건 기사에서 {total_events_saved}건의 개별 인사 발령 레코드 생성.")
    return total_events_saved


if __name__ == "__main__":
    process_unparsed_articles(batch_size=1000)
