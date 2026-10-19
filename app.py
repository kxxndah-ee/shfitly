"""
app.py
======
Streamlit 기반 '언론사 인사 트래커 (Media Personnel Tracker)'
- 빅카인즈 대량 인사 발령 기사 데이터 연동
- 이메일 바이라인 및 알고리즘 휴리스틱 기반 동명이인 완전 분리
- 매체 간 이직(Job Change) 자동 탐지 및 인터랙티브 타임라인 시각화
- 비밀번호 기반 모바일/PC 세션 보안 인증
"""

import os
import io
import time
import datetime
import pandas as pd
import streamlit as st

from data_pipeline import (
    MediaDatabase,
    PersonnelTrackerEngine,
    PersonnelRegexParser,
    OnlineNewsTracker,
    NaverNewsEmailFinder,
    load_and_process_bigkinds_file,
    normalize_media_name,
    generate_career_profile_metrics,
    generate_career_excel,
    DOMAIN_TAXONOMY,
    RANK_HIERARCHY
)


# ==========================================
# 1. 페이지 설정 및 모던 카드 스타일 CSS
# ==========================================

st.set_page_config(
    page_title="Shiftly",
    layout="wide",
    initial_sidebar_state="expanded"
)

# dahi-screener 스타일의 모던 카드형 UI CSS 주입
st.markdown("""
<style>
    @import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css');
    
    html, body, [class*="css"] {
        font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, system-ui, Roboto, sans-serif;
    }
    
    /* 메인 배경 및 레이아웃 */
    .main .block-container {
        padding-top: 1.8rem;
        padding-bottom: 3rem;
        max-width: 1400px;
    }

    /* 카드 컨테이너 */
    .metric-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 14px;
        padding: 20px 24px;
        box-shadow: 0 4px 12px rgba(15, 23, 42, 0.04);
        margin-bottom: 16px;
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
    }
    
    .metric-label {
        color: #64748B;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-value {
        color: #0F172A;
        font-size: 1.85rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-sub {
        color: #10B981;
        font-size: 0.8rem;
        font-weight: 500;
        margin-top: 6px;
    }

    /* 프로필 요약 카드 */
    .profile-card {
        background: linear-gradient(135deg, #F8FAFC 0%, #FFFFFF 100%);
        border: 1px solid #CBD5E1;
        border-radius: 16px;
        padding: 24px 28px;
        margin-bottom: 24px;
        box-shadow: 0 6px 16px rgba(15, 23, 42, 0.05);
    }
    .profile-name {
        font-size: 1.6rem;
        font-weight: 800;
        color: #0F172A;
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 8px;
    }
    .profile-meta {
        font-size: 0.95rem;
        color: #475569;
        line-height: 1.6;
    }

    /* 뱃지 스타일 */
    .badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        vertical-align: middle;
    }
    .badge-promotion {
        background-color: #DEF7EC;
        color: #03543F;
    }
    .badge-transfer {
        background-color: #E1EFFE;
        color: #1E429F;
    }
    .badge-jobchange {
        background-color: #EDEBFE;
        color: #5521B5;
        border: 1px solid #A78BFA;
    }
    .badge-career {
        background-color: #0F172A;
        color: #F8FAFC;
        border: 1px solid #334155;
    }
    .badge-normal {
        background-color: #F1F5F9;
        color: #475569;
    }

    /* 30개년 소속 매체 로드맵 및 사다리 */
    .roadmap-container {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 10px;
        margin: 14px 0 20px 0;
        padding: 16px 20px;
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
    }
    .roadmap-node {
        background: #FFFFFF;
        border: 1px solid #CBD5E1;
        border-radius: 10px;
        padding: 10px 14px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.03);
    }
    .roadmap-media {
        font-size: 0.95rem;
        font-weight: 800;
        color: #0F172A;
    }
    .roadmap-period {
        font-size: 0.78rem;
        color: #64748B;
        font-weight: 600;
        margin-top: 2px;
    }
    .roadmap-arrow {
        color: #94A3B8;
        font-size: 1.2rem;
        font-weight: 800;
    }

    /* 타임라인 컴포넌트 */
    .timeline-container {
        position: relative;
        padding-left: 32px;
        margin-top: 20px;
        border-left: 2px solid #E2E8F0;
    }
    .timeline-item {
        position: relative;
        margin-bottom: 28px;
    }
    .timeline-item:last-child {
        margin-bottom: 0;
    }
    .timeline-dot {
        position: absolute;
        left: -40px;
        top: 4px;
        width: 16px;
        height: 16px;
        border-radius: 50%;
        background-color: #3B82F6;
        border: 3px solid #FFFFFF;
        box-shadow: 0 0 0 2px #3B82F6;
    }
    .timeline-dot-jobchange {
        background-color: #8B5CF6;
        box-shadow: 0 0 0 2px #8B5CF6;
    }
    .timeline-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.03);
    }
    .timeline-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 8px;
    }
    .timeline-date {
        font-size: 0.85rem;
        font-weight: 600;
        color: #64748B;
    }
    .timeline-title {
        font-size: 1.1rem;
        font-weight: 700;
        color: #1E293B;
    }
    .timeline-body {
        font-size: 0.9rem;
        color: #475569;
        line-height: 1.5;
        margin-top: 6px;
    }

    /* 이직 카드 스타일 */
    .jobchange-card {
        background: #FFFFFF;
        border-left: 4px solid #8B5CF6;
        border-top: 1px solid #E2E8F0;
        border-right: 1px solid #E2E8F0;
        border-bottom: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 18px 22px;
        margin-bottom: 14px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.03);
    }

    /* 모바일 반응형 폰트 조절 */
    @media (max-width: 768px) {
        .metric-value { font-size: 1.4rem; }
        .profile-name { font-size: 1.3rem; }
        .timeline-container { padding-left: 24px; }
        .timeline-dot { left: -31px; width: 14px; height: 14px; }
    }

    /* ===================================================
       버튼 스타일: 모든 빨간색 기본 스타일을 차분한 그레이(Slate/Gray) 톤으로 재정의
       =================================================== */
    button,
    .stButton > button,
    button[kind="primary"],
    button[data-testid="baseButton-primary"],
    div.stButton > button:first-child[kind="primary"],
    .stFormSubmitButton > button {
        background-color: #475569 !important;
        color: #FFFFFF !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        transition: all 0.15s ease !important;
    }

    button:hover,
    .stButton > button:hover,
    button[kind="primary"]:hover,
    button[data-testid="baseButton-primary"]:hover,
    div.stButton > button:first-child[kind="primary"]:hover,
    .stFormSubmitButton > button:hover {
        background-color: #334155 !important;
        border-color: #1E293B !important;
        color: #FFFFFF !important;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15) !important;
    }

    button:active,
    button:focus,
    button[kind="primary"]:active,
    button[kind="primary"]:focus,
    .stButton > button:active,
    .stButton > button:focus {
        background-color: #1E293B !important;
        border-color: #0F172A !important;
        color: #FFFFFF !important;
        box-shadow: none !important;
    }

    /* 보조 버튼 스타일 (연회색 모노톤) */
    button[kind="secondary"],
    button[data-testid="baseButton-secondary"],
    .stButton > button[kind="secondary"] {
        background-color: #F1F5F9 !important;
        color: #334155 !important;
        border: 1px solid #CBD5E1 !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
    }

    button[kind="secondary"]:hover,
    button[data-testid="baseButton-secondary"]:hover,
    .stButton > button[kind="secondary"]:hover {
        background-color: #E2E8F0 !important;
        color: #0F172A !important;
        border-color: #94A3B8 !important;
    }

    /* 검색 폼 내부 입력창 및 버튼 높이 완벽 정렬 */
    div[data-testid="stForm"] {
        border: none !important;
        padding: 0 !important;
    }
    div[data-testid="stForm"] div[data-testid="stTextInput"] {
        margin-bottom: 0 !important;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================
# 2. 세션 기반 개인 전용 보안 인증 및 UI 렌더러
# ==========================================

def get_secret(key: str, default: str = "") -> str:
    """Streamlit Secrets 또는 환경 변수에서 값을 안전하게 조회합니다."""
    try:
        return st.secrets.get(key, default)
    except Exception:
        return os.environ.get(key, default)


def render_html(html_content: str):
    """
    HTML 문자열의 모든 선행 공백과 들여쓰기를 제거하여 
    마크다운 4스페이스 들여쓰기 코드블록 변환 버그를 완벽 방지하고 순수 HTML로 안전하게 렌더링합니다.
    """
    clean_lines = [line.strip() for line in html_content.strip().splitlines() if line.strip()]
    st.markdown("\n".join(clean_lines), unsafe_allow_html=True)


# ==========================================
# 3. 데이터베이스 및 서비스 초기화
# ==========================================

DB_FILE = "media_tracker.db"
db = MediaDatabase(DB_FILE)

stats = db.get_stats()


# ==========================================
# 4. 사이드바 및 네비게이션
# ==========================================

with st.sidebar:
    st.markdown("""
    <div style="padding: 10px 0 16px 0;">
        <span style="font-size: 1.5rem; font-weight: 800; color: #1E293B;">Shiftly</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    
    # DB 현황 요약 미니 위젯
    st.markdown("**데이터베이스 현황**")
    st.write(f"• 등록 언론인: **{stats['reporters']:,}명**")
    st.write(f"• 인사 발령 기록: **{stats['records']:,}건**")
    st.write(f"• 탐지된 이직: **{stats['changes']:,}건**")
    st.write(f"• 수집 언론사: **{stats['media_count']:,}개사**")

    st.markdown("---")
    st.markdown("**빠른 온라인 실행**")
    if st.button("최신 발령 실시간 수집", use_container_width=True, help="인터넷 뉴스에서 최신 인사 발령 기사 15건을 실시간 수집하여 등록합니다."):
        with st.spinner("온라인 뉴스에서 최신 인사 발령 기사 전문을 수집 및 분석 중..."):
            nid = get_secret("naver_client_id", "")
            nsec = get_secret("naver_client_secret", "")
            res = OnlineNewsTracker.fetch_latest_online_personnel(max_articles=15, client_id=nid, client_secret=nsec)
            st.session_state["upload_banner"] = res["message"]
            st.rerun()

    if st.button("전체 데이터 삭제", use_container_width=True, type="secondary"):
        db.clear_all_data()
        st.session_state["upload_banner"] = "데이터베이스의 모든 발령 및 기자 데이터가 삭제되었습니다."
        st.rerun()


# ==========================================
# 5. 메인 대시보드 탭 레이아웃
# ==========================================

st.markdown("""
<div style="margin-bottom: 20px;">
    <h2 style="font-weight: 800; color: #0F172A; margin-bottom: 4px;">Shiftly</h2>
</div>
""", unsafe_allow_html=True)

if "upload_banner" in st.session_state and st.session_state["upload_banner"]:
    st.success(st.session_state["upload_banner"])

tab_search, tab_jobchanges, tab_analytics, tab_data = st.tabs([
    "1. 검색",
    "2. 이직 모니터링",
    "3. 이동 통계 및 네트워크",
    "4. 데이터 관리 및 수집"
])


# =========================================================================
# TAB 1: 검색
# =========================================================================
with tab_search:
    col_title, col_quick_btn = st.columns([3, 1])
    with col_title:
        st.markdown("### 검색")
    with col_quick_btn:
        st.write("")
        if st.button("최신 발령 실시간 수집", use_container_width=True, type="secondary", help="인터넷 뉴스에서 최신 인사 발령 기사 15건을 실시간 수집하여 등록합니다."):
            with st.spinner("온라인 포털에서 최신 인사 기사 전문을 수집 및 분석 중..."):
                nid = get_secret("naver_client_id", "")
                nsec = get_secret("naver_client_secret", "")
                res = OnlineNewsTracker.fetch_latest_online_personnel(max_articles=15, client_id=nid, client_secret=nsec)
                st.session_state["upload_banner"] = res["message"]
                st.rerun()

    # 검색창 길이 1/3로 축소 배치 및 예시 삭제
    col_search_box, col_search_empty = st.columns([1, 2])
    with col_search_box:
        with st.form(key="realtime_search_form", clear_on_submit=False):
            col_in, col_btn = st.columns([3, 1])
            with col_in:
                search_name_input = st.text_input(
                    "기자 이름",
                    value=st.session_state.get("active_search_name", ""),
                    placeholder="기자 이름 입력",
                    label_visibility="collapsed"
                )
            with col_btn:
                search_submitted = st.form_submit_button("검색", type="primary", use_container_width=True)

    if search_submitted:
        name_clean = search_name_input.strip()
        if name_clean:
            with st.spinner(f"포털 온라인 뉴스에서 '{name_clean}' 기자의 최신 기사 및 발령문을 실시간 검색 중..."):
                nid = get_secret("naver_client_id", "")
                nsec = get_secret("naver_client_secret", "")
                res = OnlineNewsTracker.track_reporter_online(name_clean, client_id=nid, client_secret=nsec)
                st.session_state["upload_banner"] = res["message"]
                st.session_state["active_search_name"] = name_clean
                st.rerun()
        else:
            st.warning("검색할 기자 이름을 입력해주세요.")

    # 8. 매체 및 부서별 필터 검색 (등록된 언론인 목록 위로 위치 이동 및 항상 펼침, 예시 삭제, 괄호/영어 삭제)
    st.markdown("##### 매체 및 부서별 필터")
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        media_input = st.text_input("매체명", key="f_media")
    with fcol2:
        dept_input = st.text_input("부서명", key="f_dept")
    with fcol3:
        rank_input = st.text_input("직급명", key="f_rank")

    all_reporters = db.get_all_reporters()
    active_name = st.session_state.get("active_search_name", search_name_input.strip())
    selected_reporter_id = None

    filtered_reporters = all_reporters
    if media_input or dept_input or rank_input:
        filter_results = db.search_reporters(media=media_input, dept=dept_input, rank=rank_input)
        if filter_results:
            filtered_reporters = filter_results
        else:
            filtered_reporters = []
            st.info("조건에 부합하는 언론인 기록이 없습니다.")

    # 등록된 언론인 목록 / 인물 선택
    if active_name:
        matched_reporters = [r for r in filtered_reporters if active_name in r["name"]] if filtered_reporters else [r for r in all_reporters if active_name in r["name"]]
        if matched_reporters:
            if len(matched_reporters) > 1:
                options_dict = {r["display_label"]: r["reporter_id"] for r in matched_reporters}
                selected_label = st.selectbox(
                    f"인물 선택 : 검색된 기자 {len(matched_reporters)}명",
                    options=list(options_dict.keys())
                )
                selected_reporter_id = options_dict[selected_label]
            else:
                selected_reporter_id = matched_reporters[0]["reporter_id"]
        else:
            st.info(f"온라인 기사에서 '{active_name}' 기자의 발령문 또는 최근 기사를 찾지 못했습니다. 다른 이름으로 검색해보세요.")
    elif filtered_reporters:
        options_dict = {r["display_label"]: r["reporter_id"] for r in filtered_reporters}
        selected_label = st.selectbox(
            f"등록된 언론인 목록 : 총 {len(filtered_reporters)}명",
            options=list(options_dict.keys())
        )
        selected_reporter_id = options_dict[selected_label]
    elif all_reporters:
        pass
    else:
        st.write("")
        st.info("상단 검색창에 기자 이름을 입력하고 검색 버튼을 누르면 실시간 온라인 기사를 크롤링하여 프로필을 생성합니다.")

    # 선택된 기자의 상세 프로필 및 타임라인 렌더링
    if selected_reporter_id:
        reporter_obj = next((r for r in all_reporters if r["reporter_id"] == selected_reporter_id), None)
        timeline = db.get_reporter_timeline(selected_reporter_id)

        if reporter_obj and timeline:
            # 30개년 커리어 지표 및 로드맵 산출
            metrics = generate_career_profile_metrics(timeline, reporter_obj)
            all_jc = db.get_job_changes(limit=500)
            rep_jc = [jc for jc in all_jc if jc.get("reporter_id") == selected_reporter_id or jc.get("name") == reporter_obj["name"]]

            # 엑셀 및 CSV 바이트 버퍼 생성
            excel_bytes = generate_career_excel(reporter_obj, timeline, rep_jc)
            df_timeline = pd.DataFrame(timeline)
            csv_bytes = df_timeline.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

            # 1. 상단 프로필 헤더 및 다운로드 액션 바
            col_phead, col_pdl1, col_pdl2 = st.columns([3, 1.2, 1.2])
            with col_phead:
                st.markdown(f"### {reporter_obj['name']} 프로필")
            with col_pdl1:
                st.download_button(
                    "프로필 엑셀 다운로드",
                    data=excel_bytes,
                    file_name=f"{reporter_obj['name']}_커리어_프로필.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            with col_pdl2:
                st.download_button(
                    "발령 연대기 다운로드",
                    data=csv_bytes,
                    file_name=f"{reporter_obj['name']}_발령연대기.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            current_media = reporter_obj["current_media"]
            current_dept = reporter_obj["current_dept"]
            current_rank = reporter_obj["current_rank"]
            email = reporter_obj.get("email", "")

            # 2. 프로필 요약 카드
            render_html(f"""
            <div class="profile-card">
                <div class="profile-name">
                    <span>{reporter_obj['name']}</span>
                    <span class="badge badge-transfer">{current_media}</span>
                    <span class="badge badge-normal">{current_dept}</span>
                    <span class="badge badge-promotion">{current_rank}</span>
                    <span class="badge badge-career">총 {metrics.get('career_years', 1)}년차 : {metrics.get('first_year', 1995)}년 ~ {metrics.get('last_year', 2026)}년</span>
                    {'<span class="badge badge-jobchange">이직 ' + str(metrics.get('job_change_count', 0)) + '회</span>' if metrics.get('job_change_count', 0) > 0 else ''}
                </div>
                <div class="profile-meta">
                    • <b>고유 식별자:</b> <code>{reporter_obj['reporter_id']}</code><br>
                    • <b>바이라인 이메일:</b> {f'<a href="mailto:{email}">{email}</a>' if email else '<span style="color:#94A3B8;">미등록</span>'}<br>
                    • <b>활동 추적 기간:</b> {metrics.get('first_date')} ~ {metrics.get('last_date')} : 총 {metrics.get('total_records', len(timeline))}건 발령 및 활동 수집, 승진 {metrics.get('promotion_count', 0)}회, 이직 {metrics.get('job_change_count', 0)}회<br>
                    • <b>주요 출입처 전문분야:</b> <b style="color:#0F172A;">{metrics.get('primary_domain', '일반')}</b>
                </div>
            </div>
            """)

            # 3. 4대 핵심 커리어 KPI 지표 카드
            kcol1, kcol2, kcol3, kcol4 = st.columns(4)
            with kcol1:
                render_html(f"""
                <div class="metric-card">
                    <div class="metric-label">총 경력 연차</div>
                    <div class="metric-value">{metrics.get('career_years', 1)}년차</div>
                    <div class="metric-sub">{metrics.get('first_year', 1995)}년 ~ {metrics.get('last_year', 2026)}년</div>
                </div>
                """)
            with kcol2:
                render_html(f"""
                <div class="metric-card">
                    <div class="metric-label">현재 소속 및 직급</div>
                    <div class="metric-value" style="font-size:1.4rem;">{current_media}</div>
                    <div class="metric-sub" style="color:#475569;">{current_dept} {current_rank}</div>
                </div>
                """)
            with kcol3:
                render_html(f"""
                <div class="metric-card">
                    <div class="metric-label">주요 출입처 전문분야</div>
                    <div class="metric-value" style="font-size:1.4rem;">{metrics.get('primary_domain', '일반')}</div>
                    <div class="metric-sub">발령 및 활동 기사 기반</div>
                </div>
                """)
            with kcol4:
                render_html(f"""
                <div class="metric-card">
                    <div class="metric-label">매체 간 이직 횟수</div>
                    <div class="metric-value">{metrics.get('job_change_count', 0)}회</div>
                    <div class="metric-sub">총 {len(metrics.get('affiliation_journey', []))}개 언론사 활동</div>
                </div>
                """)

            # 4. 소속 매체 이동 로드맵 (12. 이직)
            aff_list = metrics.get("affiliation_journey", [])
            if aff_list:
                st.markdown("#### 이직")
                roadmap_html = '<div class="roadmap-container">'
                for idx, aff in enumerate(aff_list):
                    roadmap_html += f"""
                    <div class="roadmap-node">
                        <div class="roadmap-media">{aff['media']}</div>
                        <div class="roadmap-period">{aff['start_year']}년 ~ {aff['end_year']}년 : {aff['last_dept']} {aff['last_rank']}</div>
                    </div>
                    """
                    if idx < len(aff_list) - 1:
                        roadmap_html += '<div class="roadmap-arrow">➔</div>'
                roadmap_html += '</div>'
                render_html(roadmap_html)

            # 5. 직급 성장 (13. 승진) & 출입처 도메인 분석
            l_col, d_col = st.columns([1.2, 1])
            with l_col:
                st.markdown("#### 승진")
                l_items = metrics.get("promotion_ladder", [])
                if l_items:
                    ladder_html = '<div style="display:flex; flex-wrap:wrap; align-items:center; gap:8px; margin-top:8px;">'
                    for idx, l in enumerate(l_items):
                        ladder_html += f"""
                        <div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:6px 12px; font-size:0.85rem;">
                            <b>{l['rank_title']}</b> <span style="color:#64748B;">{l['year']}년 {l['media']}</span>
                        </div>
                        """
                        if idx < len(l_items) - 1:
                            ladder_html += '<span style="color:#94A3B8; font-weight:700;">➔</span>'
                    ladder_html += '</div>'
                    render_html(ladder_html)
                else:
                    st.write("단일 직급 활동 기록이 확인되었습니다.")

            with d_col:
                st.markdown("#### 출입처 도메인 전문성")
                d_breakdown = metrics.get("domain_breakdown", [])
                if d_breakdown:
                    d_html = '<div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:8px;">'
                    for db_item in d_breakdown[:4]:
                        d_html += f"""
                        <span class="badge badge-normal" style="padding:6px 10px; font-size:0.8rem;">
                            <b>{db_item['domain']}</b> {db_item['percentage']}% : {db_item['count']}건
                        </span>
                        """
                    d_html += '</div>'
                    render_html(d_html)

            st.markdown("<br>", unsafe_allow_html=True)

            # 6. 연대기 타임라인 (14. 타임라인)
            st.markdown("#### 타임라인")
            st.caption("기사 원문 링크를 클릭하면 해당 시점의 실제 뉴스 기사 웹페이지로 이동합니다.")

            # 타임라인 카드 렌더링
            timeline_html = '<div class="timeline-container">'
            for item in timeline:
                action = item.get("action_type", "발령")
                is_job_change = "이직" in action
                dot_class = "timeline-dot timeline-dot-jobchange" if is_job_change else "timeline-dot"

                if is_job_change:
                    badge_html = f'<span class="badge badge-jobchange">{action}</span>'
                elif "승진" in action:
                    badge_html = f'<span class="badge badge-promotion">{action}</span>'
                elif "전보" in action or "보직" in action:
                    badge_html = f'<span class="badge badge-transfer">{action}</span>'
                else:
                    badge_html = f'<span class="badge badge-normal">{action}</span>'

                art_url = item.get("article_url", "")
                art_title = item.get("article_title", "")
                if art_url:
                    link_html = f'<div style="margin-top:6px;"><a href="{art_url}" target="_blank" style="font-size:0.85rem; font-weight:600; color:#2563EB; text-decoration:underline;">기사 원문: {art_title or "온라인 기사 보기"}</a></div>'
                elif art_title:
                    link_html = f'<div style="margin-top:6px; font-size:0.85rem; color:#475569;">기사 제목: <b>{art_title}</b></div>'
                else:
                    link_html = ''

                raw_snippet = item.get('raw_text', '')
                timeline_html += f"""
                <div class="timeline-item">
                    <div class="{dot_class}"></div>
                    <div class="timeline-card">
                        <div class="timeline-header">
                            <span class="timeline-title">{item['media']} · {item['dept']} {item['rank_title']}</span>
                            <span class="timeline-date">{item['pub_date']}</span>
                        </div>
                        <div>
                            {badge_html}
                            <span style="font-size:0.85rem; color:#64748B; margin-left:8px;">출입처 도메인: {item.get('domain', '일반')}</span>
                        </div>
                        {link_html}
                        <div class="timeline-body">
                            <b>발령 원문:</b> <code>{raw_snippet}</code>
                        </div>
                    </div>
                </div>
                """
            timeline_html += '</div>'
            render_html(timeline_html)

            # 7. 하단 다운로드 액션 바
            st.markdown("<br>", unsafe_allow_html=True)
            st.download_button(
                "프로필 엑셀 다운로드",
                data=excel_bytes,
                file_name=f"{reporter_obj['name']}_커리어_프로필.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="btn_dl_bottom"
            )


# =========================================================================
# =========================================================================
# TAB 2: 이직 모니터링
# =========================================================================
with tab_jobchanges:
    st.markdown("### 이직 모니터링")
    st.markdown("정규표현식 파싱 및 알고리즘 휴리스틱을 통해 탐지된 언론인 매체 이동 내역입니다.")

    changes = db.get_job_changes(limit=200)

    if not changes:
        st.info("탐지된 매체 간 이직 데이터가 없습니다. 상단 '데이터 관리 및 수집' 탭에서 데이터를 로드하세요.")
    else:
        # 상단 KPI 지표 카드 4종
        df_changes = pd.DataFrame(changes)
        total_jc = len(df_changes)
        
        # 최다 유입 언론사, 유출 언론사
        top_in = df_changes["to_media"].mode()[0] if not df_changes.empty else "-"
        top_out = df_changes["from_media"].mode()[0] if not df_changes.empty else "-"
        avg_score = round(df_changes["confidence_score"].mean(), 1) if not df_changes.empty else 0.0

        mcol1, mcol2, mcol3, mcol4 = st.columns(4)
        with mcol1:
            render_html(f"""
            <div class="metric-card">
                <div class="metric-label">총 이직 탐지 건수</div>
                <div class="metric-value">{total_jc}건</div>
                <div class="metric-sub">매체 간 이동 감지</div>
            </div>
            """)
        with mcol2:
            render_html(f"""
            <div class="metric-card">
                <div class="metric-label">최다 유입 언론사</div>
                <div class="metric-value">{top_in}</div>
                <div class="metric-sub">외부 기자 적극 영입</div>
            </div>
            """)
        with mcol3:
            render_html(f"""
            <div class="metric-card">
                <div class="metric-label">최다 유출 언론사</div>
                <div class="metric-value">{top_out}</div>
                <div class="metric-sub">타사 이직 발생 매체</div>
            </div>
            """)
        with mcol4:
            render_html(f"""
            <div class="metric-card">
                <div class="metric-label">평균 알고리즘 신뢰도</div>
                <div class="metric-value">{avg_score}점</div>
                <div class="metric-sub">연속성 및 도메인 일치도</div>
            </div>
            """)

        # 필터링 컨트롤
        st.markdown("##### 이직 내역 필터")
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            media_filter = st.selectbox("언론사 필터", ["전체"] + sorted(list(set(df_changes["from_media"].tolist() + df_changes["to_media"].tolist()))))
        with fcol2:
            period_filter = st.selectbox("조회 기간", ["전체 기간", "최근 1년 이내", "최근 6개월 이내"])
        with fcol3:
            sort_by = st.selectbox("정렬 기준", ["최신 발령일순", "알고리즘 신뢰도 점수순"])

        # 필터 적용
        filtered_df = df_changes.copy()
        if media_filter != "전체":
            filtered_df = filtered_df[(filtered_df["from_media"] == media_filter) | (filtered_df["to_media"] == media_filter)]
        
        if sort_by == "최신 발령일순":
            filtered_df = filtered_df.sort_values(by="to_date", ascending=False)
        else:
            filtered_df = filtered_df.sort_values(by="confidence_score", ascending=False)

        # 이직 카드 피드 렌더링
        st.markdown(f"**총 {len(filtered_df)}건의 이직 이벤트가 조회되었습니다.**")
        for _, row in filtered_df.iterrows():
            render_html(f"""
            <div class="jobchange-card">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                    <div style="font-size: 1.15rem; font-weight: 800; color: #0F172A;">
                        <span style="color: #7C3AED;">이직</span> {row['name']} 기자
                    </div>
                    <div style="font-size: 0.85rem; color: #64748B; font-weight: 600;">
                        발령일: {row['to_date']} : 이전 발령 후 {row['days_gap']}일 경과
                    </div>
                </div>
                <div style="font-size: 1.05rem; font-weight: 700; color: #1E293B; margin: 8px 0;">
                    <span style="color: #64748B;">{row['from_media']} {row['from_dept']}</span> {row['from_rank']}
                    &nbsp;➔&nbsp;
                    <span style="color: #2563EB;">{row['to_media']} {row['to_dept']}</span> {row['to_rank']}
                </div>
                <div style="font-size: 0.85rem; color: #475569; display: flex; gap: 8px; align-items: center; margin-top: 6px;">
                    <span class="badge badge-jobchange">신뢰도: {row['confidence_score']}점</span>
                    <span>탐지 근거: <b>{row['detected_reason']}</b></span>
                </div>
            </div>
            """)


# =========================================================================
# TAB 3: 이동 통계 및 네트워크
# =========================================================================
with tab_analytics:
    st.markdown("### 이동 통계 및 네트워크")

    with db.get_connection() as conn:
        df_all_rec = pd.read_sql_query("SELECT * FROM personnel_records", conn)

    if df_all_rec.empty:
        st.info("통계 분석을 위한 데이터가 부족합니다.")
    else:
        acol1, acol2 = st.columns(2)
        with acol1:
            st.markdown("##### 주요 언론사별 인사 발령 건수")
            media_counts = df_all_rec["media"].value_counts().reset_index()
            media_counts.columns = ["언론사", "발령 건수"]
            st.dataframe(media_counts, use_container_width=True, hide_index=True)

        with acol2:
            st.markdown("##### 출입처 도메인별 인사 분포")
            domain_counts = df_all_rec["domain"].value_counts().reset_index()
            domain_counts.columns = ["도메인", "발령 건수"]
            st.dataframe(domain_counts, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("##### 직급별 발령 분포")
        rank_counts = df_all_rec["rank_title"].value_counts().head(10).reset_index()
        rank_counts.columns = ["직급", "건수"]
        st.bar_chart(data=rank_counts.set_index("직급"), use_container_width=True)


# =========================================================================
# TAB 4: 데이터 관리 및 수집
# =========================================================================
with tab_data:
    st.markdown("### 데이터 관리 및 수집")

    dcol1, dcol2 = st.columns(2)

    with dcol1:
        st.markdown("#### 최신 온라인 기사 실시간 수집")
        st.write("네이버 뉴스에서 최근 인사 발령 기사 전문을 실시간으로 가져와 자동 분석 및 등록합니다.")
        col_on1, col_on2 = st.columns([2, 1])
        with col_on1:
            online_limit = st.slider("수집할 최신 기사 수", min_value=5, max_value=25, value=15, step=5)
        with col_on2:
            st.write("")
            st.write("")
            if st.button("온라인 최신 발령 즉시 수집", use_container_width=True, type="primary"):
                with st.spinner(f"온라인 뉴스에서 최신 인사 발령 기사 {online_limit}건의 전문을 수집 및 분석 중..."):
                    nid = get_secret("naver_client_id", "")
                    nsec = get_secret("naver_client_secret", "")
                    res = OnlineNewsTracker.fetch_latest_online_personnel(max_articles=online_limit, client_id=nid, client_secret=nsec)
                    st.session_state["upload_banner"] = res["message"]
                    st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 빅카인즈 파일 업로드")
        st.write("빅카인즈에서 다운로드한 엑셀 또는 CSV 파일을 업로드하세요.")
        
        upload_mode = st.radio(
            "반영 방식 선택",
            ["기존 데이터에 누적하여 추가", "기존 데이터 비우고 새로 교체"],
            horizontal=True,
            help="기존에 업로드한 기자 명단에 새 파일의 발령 명단을 덧붙여 누적할지, 아니면 초기화하고 교체할지 선택합니다."
        )

        uploaded_file = st.file_uploader(
            "빅카인즈 파일 선택",
            type=["xlsx", "xls", "csv"],
            help="일자, 언론사, 제목, 본문 컬럼이 포함되어 있어야 합니다."
        )

        if uploaded_file is not None:
            if st.button("업로드 파일 처리 및 데이터베이스 반영", use_container_width=True):
                with st.spinner("빅카인즈 기사 본문 정규식 파싱, 동명이인 클러스터링 및 누적 처리 중..."):
                    try:
                        is_append = (upload_mode == "기존 데이터에 누적하여 추가")
                        res = load_and_process_bigkinds_file(uploaded_file, append=is_append)
                        new_cnt = res.get('new_records', res['total_records'])
                        st.session_state["upload_banner"] = (
                            f"'{uploaded_file.name}' 반영 완료 : "
                            f"신규 발령 {new_cnt}건 추가 반영, 현재 총 언론인 {res['unique_reporters']}명, "
                            f"총 발령 {res['total_records']}건, 탐지된 이직 {res['job_changes_detected']}건"
                        )
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"파일 처리 중 오류가 발생했습니다: {e}")
                        st.info("엑셀 파일 안에 일자, 언론사, 본문 컬럼과 부서명, 직책, 이름 형태의 발령 내용이 들어있는지 확인해주세요.")

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 기본 샘플 로드 및 데이터 초기화")
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("샘플 데이터 로드", use_container_width=True):
                sample_path = "sample_data/bigkinds_personnel_sample.xlsx"
                if os.path.exists(sample_path):
                    with st.spinner("샘플 데이터 파싱 중..."):
                        res = load_and_process_bigkinds_file(sample_path, append=False)
                        st.session_state["upload_banner"] = f"샘플 데이터로 초기화되었습니다. : 총 레코드 {res['total_records']}건"
                        st.rerun()
                else:
                    st.error("샘플 데이터 파일을 찾을 수 없습니다.")
        with col_btn2:
            if st.button("전체 데이터 완전히 비우기", use_container_width=True):
                db.clear_all_data()
                st.session_state["upload_banner"] = "모든 데이터가 삭제되었습니다. 이제 온라인 기사 검색을 시작해보세요."
                st.rerun()

    with dcol2:
        st.markdown("#### 기사 텍스트 직접 붙여넣기")
        st.write("인터넷 기사의 인사 발령 텍스트를 그대로 복사해 붙여넣으면 즉시 파싱하여 등록합니다.")

        with st.expander("텍스트 직접 입력창 열기", expanded=True):
            pcol1, pcol2 = st.columns(2)
            with pcol1:
                paste_media = st.text_input("언론사명")
            with pcol2:
                paste_date = st.date_input("발령 일자", value=datetime.date.today())
            
            paste_title = st.text_input("기사 제목")
            paste_text = st.text_area(
                "발령 기사 본문 붙여넣기",
                height=140
            )

            if st.button("붙여넣은 본문 즉시 분석 및 데이터베이스 등록", use_container_width=True):
                if not paste_text.strip():
                    st.warning("발령 기사 본문을 입력해주세요.")
                else:
                    parsed_records = PersonnelRegexParser.parse_article_text(
                        article_text=paste_text,
                        default_media=paste_media or "미상",
                        article_title=paste_title or "",
                        pub_date=str(paste_date)
                    )
                    if not parsed_records:
                        st.error("입력된 텍스트에서 부서명, 직책, 이름 형태의 발령 대상을 추출하지 못했습니다.")
                    else:
                        engine = PersonnelTrackerEngine()
                        res = engine.process_records(parsed_records, append=True)
                        st.session_state["upload_banner"] = (
                            f"직접 입력한 기사에서 {len(parsed_records)}명의 발령 기록을 등록했습니다. : "
                            f"총 누적 언론인 {res['unique_reporters']}명"
                        )
                        st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 네이버 뉴스 검색 연동")
        st.write("기자의 이름과 언론사명을 검색해 본문 바이라인에서 이메일을 추출하고 고유 식별자를 보완합니다.")

        naver_id = st.text_input("클라이언트 아이디", value=get_secret("naver_client_id", ""), type="password")
        naver_secret = st.text_input("클라이언트 시크릿", value=get_secret("naver_client_secret", ""), type="password")

        if st.button("네이버 바이라인 이메일 동기화 실행", use_container_width=True):
            if not naver_id or not naver_secret:
                st.warning("네이버 개발자 센터에서 발급받은 인증 정보를 입력해주세요.")
            else:
                with st.spinner("네이버 뉴스 검색을 통해 기자별 이메일 바이라인 탐색 중..."):
                    finder = NaverNewsEmailFinder(client_id=naver_id, client_secret=naver_secret)
                    reporters = db.get_all_reporters()
                    updated_cnt = 0
                    for r in reporters[:30]:
                        if not r.get("email"):
                            found = finder.search_reporter_email(name=r["name"], media=r["current_media"])
                            if found:
                                with db.get_connection() as conn:
                                    conn.cursor().execute(
                                        "UPDATE reporters SET email = ? WHERE reporter_id = ?",
                                        (found, r["reporter_id"])
                                    )
                                    conn.commit()
                                updated_cnt += 1
                    st.success(f"이메일 동기화 완료 : 총 {updated_cnt}명의 바이라인 이메일을 보완 등록했습니다.")
                    time.sleep(1)
                    st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 전체 데이터 내보내기")
        with db.get_connection() as conn:
            export_df = pd.read_sql_query("SELECT * FROM reporters", conn)
            rec_df = pd.read_sql_query("SELECT * FROM personnel_records", conn)
            jc_df = pd.read_sql_query("SELECT * FROM job_changes", conn)

            col_ex1, col_ex2 = st.columns(2)
            with col_ex1:
                xl_buf = io.BytesIO()
                with pd.ExcelWriter(xl_buf, engine="openpyxl") as writer:
                    export_df.to_excel(writer, sheet_name="기자_마스터", index=False)
                    rec_df.to_excel(writer, sheet_name="발령_레코드", index=False)
                    jc_df.to_excel(writer, sheet_name="이직_기록", index=False)
                st.download_button(
                    "전체 데이터 엑셀 다운로드",
                    data=xl_buf.getvalue(),
                    file_name="media_personnel_master.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            with col_ex2:
                csv_bytes = export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button(
                    "등록 기자 명단 다운로드",
                    data=csv_bytes,
                    file_name="media_reporters_master.csv",
                    mime="text/csv",
                    use_container_width=True
                )
