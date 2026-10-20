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
import threading
import base64
from pathlib import Path
from PIL import Image
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

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
    is_personnel_article_title,
    is_media_organization,
    DOMAIN_TAXONOMY,
    RANK_HIERARCHY
)


# ==========================================
# 1. 페이지 설정 및 모던 카드 스타일 CSS
# ==========================================

FAVICON_PATH = Path(__file__).parent / "assets" / "favicon.png"
fav_icon = "S"
_b64_fav = ""

if FAVICON_PATH.exists():
    try:
        fav_icon = Image.open(FAVICON_PATH)
        with open(FAVICON_PATH, "rb") as _f:
            _b64_fav = base64.b64encode(_f.read()).decode("utf-8")
    except Exception:
        fav_icon = "S"

st.set_page_config(
    page_title="Shiftly",
    page_icon=fav_icon,
    layout="wide",
    initial_sidebar_state="expanded"
)

# 크롬 탭 고정 및 브라우저 파비콘 강제 동기화 (원형 S 아이콘)
if _b64_fav:
    components.html(f"""
    <script>
        try {{
            const head = window.parent.document.head;
            const existingLinks = head.querySelectorAll("link[rel*='icon']");
            existingLinks.forEach(el => el.remove());
            
            const newLink = window.parent.document.createElement('link');
            newLink.type = 'image/png';
            newLink.rel = 'icon';
            newLink.href = 'data:image/png;base64,{_b64_fav}';
            head.appendChild(newLink);
            
            const appleLink = window.parent.document.createElement('link');
            appleLink.rel = 'apple-touch-icon';
            appleLink.href = 'data:image/png;base64,{_b64_fav}';
            head.appendChild(appleLink);
        }} catch(e) {{
            // Ignore in iframes if any
        }}
    </script>
    """, height=0, width=0)

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
        top: 2px;
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
    .timeline-date-above {
        font-size: 0.95rem;
        font-weight: 700;
        color: #0F172A;
        margin-bottom: 8px;
        line-height: 1.2;
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
    div[data-testid="stForm"] div[data-testid="stHorizontalBlock"],
    div[data-testid="stHorizontalBlock"] {
        align-items: flex-end !important;
    }
    div[data-testid="stForm"] div[data-testid="stTextInput"],
    div[data-testid="stTextInput"] {
        margin-bottom: 0 !important;
    }
    div[data-testid="stTextInput"] input {
        height: 42px !important;
    }
    div[data-testid="stFormSubmitButton"],
    div.stButton {
        margin-bottom: 0 !important;
    }
    div[data-testid="stFormSubmitButton"] button,
    div.stButton > button {
        margin-bottom: 0 !important;
        height: 42px !important;
        min-height: 42px !important;
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

tab_search, tab_monitoring = st.tabs([
    "1. 검색",
    "2. 인사 모니터링"
])



# =========================================================================
# TAB 1: 검색
# =========================================================================
with tab_search:
    # 3. 검색 모드 선택 (이름 검색 vs 매체 및 부서별 검색)
    col_mode, col_quick_btn = st.columns([3, 1], vertical_alignment="bottom")
    with col_mode:
        search_mode = st.radio(
            "검색 모드 선택",
            options=["이름 검색", "매체 및 부서별 검색"],
            horizontal=True,
            index=0 if st.session_state.get("search_mode_selection", "이름 검색") == "이름 검색" else 1,
            key="search_mode_radio"
        )
        st.session_state["search_mode_selection"] = search_mode
    with col_quick_btn:
        if st.button("최신 발령 실시간 수집", use_container_width=True, type="secondary", help="인터넷 뉴스에서 최신 인사 발령 기사 15건을 실시간 수집하여 등록합니다."):
            with st.spinner("온라인 포털에서 최신 인사 기사 전문을 수집 및 분석 중..."):
                nid = get_secret("naver_client_id", "")
                nsec = get_secret("naver_client_secret", "")
                res = OnlineNewsTracker.fetch_latest_online_personnel(max_articles=15, client_id=nid, client_secret=nsec)
                st.session_state["upload_banner"] = res["message"]
                st.rerun()

    all_reporters = [r for r in db.get_all_reporters() if not is_media_organization(r["name"])]

    # 1. 이름 검색 (폰트 및 사이즈는 매체 및 부서별 검색과 동일)
    if search_mode == "이름 검색":
        st.markdown("##### 이름 검색")
        col_search_box, col_search_empty = st.columns([1, 2])
        is_searching_name = st.session_state.get("is_searching_name", False)

        with col_search_box:
            if is_searching_name:
                col_in, col_btn, col_stop = st.columns([2.5, 1, 1], vertical_alignment="bottom")
                with col_in:
                    st.text_input(
                        "기자 이름",
                        value=st.session_state.get("active_search_name", ""),
                        placeholder="기자 이름 입력",
                        label_visibility="collapsed",
                        disabled=True,
                        key="search_name_dis"
                    )
                with col_btn:
                    st.button("검색", type="primary", disabled=True, use_container_width=True, key="btn_name_searching")
                with col_stop:
                    if st.button("검색 중단", type="secondary", use_container_width=True, key="btn_stop_name"):
                        st.session_state["is_searching_name"] = False
                        if "stop_event_name" in st.session_state:
                            st.session_state["stop_event_name"].set()
                        st.session_state["upload_banner"] = "검색이 중단되었습니다."
                        st.rerun()
            else:
                with st.form(key="realtime_search_form", clear_on_submit=False):
                    col_in, col_btn = st.columns([3, 1], vertical_alignment="bottom")
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
                        st.session_state["active_search_name"] = name_clean
                        # 무조건 실시간 온라인 검색을 디폴트로 실행
                        st.session_state["is_searching_name"] = True
                        st.rerun()
                    else:
                        st.warning("검색할 기자 이름을 입력해주세요.")

        if is_searching_name:
            stop_event = threading.Event()
            st.session_state["stop_event_name"] = stop_event
            target_name = st.session_state.get("active_search_name", "")
            with st.spinner("실시간 검색 중..."):
                nid = get_secret("naver_client_id", "")
                nsec = get_secret("naver_client_secret", "")
                res_box = {}

                def search_worker():
                    try:
                        res_box["res"] = OnlineNewsTracker.track_reporter_online(
                            target_name, client_id=nid, client_secret=nsec, stop_event=stop_event
                        )
                    except Exception as e:
                        res_box["error"] = str(e)

                t = threading.Thread(target=search_worker)
                t.start()

                while t.is_alive():
                    time.sleep(0.1)

                st.session_state["is_searching_name"] = False
                if not stop_event.is_set():
                    res = res_box.get("res", {})
                    st.session_state["upload_banner"] = res.get("message", "검색이 완료되었습니다.")
                    st.session_state["active_search_name"] = target_name
                    st.rerun()

    # 2. 매체 및 부서별 검색 (검색 버튼 포함, 50% 길이 축소)
    else:
        st.markdown("##### 매체 및 부서별 검색")
        col_form_half, col_empty_half = st.columns([1, 1])
        is_searching_media = st.session_state.get("is_searching_media", False)

        with col_form_half:
            if is_searching_media:
                fcol1, fcol2, fcol3, fcol_btn, fcol_stop = st.columns([2.5, 2.5, 2.5, 1.1, 1.1], vertical_alignment="bottom")
                with fcol1:
                    st.text_input("매체명", value=st.session_state.get("filter_media_val", ""), key="f_media_dis", disabled=True)
                with fcol2:
                    st.text_input("부서명", value=st.session_state.get("filter_dept_val", ""), key="f_dept_dis", disabled=True)
                with fcol3:
                    st.text_input("직급명", value=st.session_state.get("filter_rank_val", ""), key="f_rank_dis", disabled=True)
                with fcol_btn:
                    st.button("검색", type="primary", disabled=True, use_container_width=True, key="btn_media_dis")
                with fcol_stop:
                    if st.button("검색 중단", type="secondary", use_container_width=True, key="btn_stop_media"):
                        st.session_state["is_searching_media"] = False
                        if "stop_event_media" in st.session_state:
                            st.session_state["stop_event_media"].set()
                        st.session_state["upload_banner"] = "검색이 중단되었습니다."
                        st.rerun()
            else:
                with st.form(key="media_dept_search_form", clear_on_submit=False):
                    fcol1, fcol2, fcol3, fcol_btn = st.columns([3, 3, 3, 1.2], vertical_alignment="bottom")
                    with fcol1:
                        media_input = st.text_input("매체명", value=st.session_state.get("filter_media_val", ""), key="f_media")
                    with fcol2:
                        dept_input = st.text_input("부서명", value=st.session_state.get("filter_dept_val", ""), key="f_dept")
                    with fcol3:
                        rank_input = st.text_input("직급명", value=st.session_state.get("filter_rank_val", ""), key="f_rank")
                    with fcol_btn:
                        filter_submitted = st.form_submit_button("검색", type="primary", use_container_width=True)

                if filter_submitted:
                    m_clean = media_input.strip()
                    d_clean = dept_input.strip()
                    r_clean = rank_input.strip()
                    st.session_state["filter_media_val"] = m_clean
                    st.session_state["filter_dept_val"] = d_clean
                    st.session_state["filter_rank_val"] = r_clean

                    if m_clean or d_clean or r_clean:
                        # 무조건 실시간 온라인 검색을 디폴트로 실행
                        st.session_state["is_searching_media"] = True
                        st.rerun()

        if is_searching_media:
            stop_event_med = threading.Event()
            st.session_state["stop_event_media"] = stop_event_med
            m_val = st.session_state.get("filter_media_val", "")
            d_val = st.session_state.get("filter_dept_val", "")
            r_val = st.session_state.get("filter_rank_val", "")
            with st.spinner("실시간 검색 중..."):
                nid = get_secret("naver_client_id", "")
                nsec = get_secret("naver_client_secret", "")
                res_box_med = {}

                def media_worker():
                    try:
                        res_box_med["res"] = OnlineNewsTracker.track_media_dept_online(
                            media=m_val, dept=d_val, rank=r_val, client_id=nid, client_secret=nsec, stop_event=stop_event_med
                        )
                    except Exception as e:
                        res_box_med["error"] = str(e)

                tm = threading.Thread(target=media_worker)
                tm.start()

                while tm.is_alive():
                    time.sleep(0.1)

                st.session_state["is_searching_media"] = False
                if not stop_event_med.is_set():
                    res = res_box_med.get("res", {})
                    st.session_state["upload_banner"] = res.get("message", "검색이 완료되었습니다.")
                    st.rerun()

    selected_reporter_id = None

    if search_mode == "이름 검색":
        active_name = st.session_state.get("active_search_name", "")
        filtered_reporters = all_reporters
        if active_name:
            matched_records = [
                r for r in db.search_records(name=active_name)
                if is_media_organization(r.get("media", "")) and is_personnel_article_title(r.get("article_title", ""))
            ]
        else:
            matched_records = []
    else:
        active_name = ""
        m_val = st.session_state.get("filter_media_val", "")
        d_val = st.session_state.get("filter_dept_val", "")
        r_val = st.session_state.get("filter_rank_val", "")
        if m_val or d_val or r_val:
            filtered_reporters = [
                r for r in db.search_reporters(media=m_val, dept=d_val, rank=r_val)
                if not is_media_organization(r["name"])
            ]
            matched_records = [
                r for r in db.search_records(media=m_val, dept=d_val, rank=r_val)
                if is_media_organization(r.get("media", "")) and is_personnel_article_title(r.get("article_title", ""))
            ]
        else:
            filtered_reporters = all_reporters
            matched_records = []

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
                st.caption(f"선택된 언론인: **{matched_reporters[0]['display_label']}**")
        else:
            st.info(f"온라인 인사 기사에서 '{active_name}' 기자의 발령 기록을 찾지 못했습니다.")
    elif filtered_reporters:
        if len(filtered_reporters) > 1:
            options_dict = {r["display_label"]: r["reporter_id"] for r in filtered_reporters}
            selected_label = st.selectbox(
                f"인물 선택 : 검색된 언론인 {len(filtered_reporters)}명",
                options=list(options_dict.keys())
            )
            selected_reporter_id = options_dict[selected_label]
        else:
            selected_reporter_id = filtered_reporters[0]["reporter_id"]
            st.caption(f"선택된 언론인: **{filtered_reporters[0]['display_label']}**")
    elif search_mode == "매체 및 부서별 검색" and (st.session_state.get("filter_media_val") or st.session_state.get("filter_dept_val")):
        st.info("조건에 부합하는 온라인 인사 발령 기록을 찾지 못했습니다.")

    # 인사 소식 (발령 명단) 테이블 표시 - 이름 검색 또는 매체/부서별 검색 결과 공통
    if matched_records:
        st.markdown("##### 인사 소식")
        df_display = pd.DataFrame([{
            "언론사": r.get("media", ""),
            "발령구분": r.get("action_type", "발령"),
            "부서": r.get("dept", ""),
            "직급": r.get("rank_title", ""),
            "성명": r.get("name", ""),
            "발령일자": r.get("pub_date", ""),
            "기사원문(링크주소)": r.get("article_url", "")
        } for r in matched_records[:50]])
        st.dataframe(
            df_display,
            column_config={
                "기사원문(링크주소)": st.column_config.LinkColumn(
                    "기사원문(링크주소)",
                    help="클릭 시 포털 기사 원문 페이지로 이동합니다.",
                    validate=r"^https?://.*",
                    max_chars=60,
                    display_text="기사 원문 열기"
                )
            },
            use_container_width=True, 
            hide_index=True
        )

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

            # 타임라인 카드 렌더링 (1. 최근순 정렬, 2. 날짜는 박스 위에 표기)
            timeline_desc = sorted(
                timeline,
                key=lambda x: str(x.get("pub_date", "1970-01-01"))[:10],
                reverse=True
            )
            timeline_html = '<div class="timeline-container">'
            for item in timeline_desc:
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
                    <div class="timeline-date-above">{item['pub_date']}</div>
                    <div class="timeline-card">
                        <div class="timeline-header">
                            <span class="timeline-title">{item['media']} · {item['dept']} {item['rank_title']}</span>
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
# TAB 2: 인사 모니터링
# =========================================================================
with tab_monitoring:
    st.markdown("### 인사 모니터링")
    st.caption("기사 제목에 [인사] 또는 <인사>가 포함된 언론사 공식 인사 기사를 실시간 모니터링하여 언론사별, 발령 구분별로 일목요연하게 정리한 인사 소식 브리핑입니다.")

    # 상단 컨트롤 바 (일자 선택, 검색창, 실시간 수집 버튼)
    m_col_date, m_col_search, m_col_btn = st.columns([1.2, 1.8, 1.2], vertical_alignment="bottom")
    with m_col_date:
        today_date = datetime.date.today()
        selected_date = st.date_input("모니터링 일자", value=today_date, max_value=today_date)
    with m_col_search:
        filter_kw = st.text_input("인사 소식 내 검색", placeholder="언론사명, 기자명, 부서명 검색", label_visibility="visible")
    with m_col_btn:
        if st.button("당일 인사 실시간 수집 및 새로고침", use_container_width=True, type="primary"):
            with st.spinner("온라인 포털에서 당일 및 최신 [인사]/<인사> 기사 전문을 실시간 수집 및 분석 중..."):
                nid = get_secret("naver_client_id", "")
                nsec = get_secret("naver_client_secret", "")
                res = OnlineNewsTracker.fetch_daily_personnel_news(
                    target_date=str(selected_date),
                    max_articles=25,
                    client_id=nid,
                    client_secret=nsec
                )
                st.session_state["upload_banner"] = res.get("message", "공식 [인사]/<인사> 기사 실시간 수집 및 정리가 완료되었습니다.")
                st.rerun()

    # 데이터 로드: 선택된 일자 우선 조회, 없으면 최근 전체 조회
    date_str = str(selected_date)
    summary_data = db.get_daily_personnel_summary(date_str)
    is_showing_fallback = False

    if summary_data["total_records"] == 0:
        # 당일 데이터가 아직 수집되지 않은 경우 최근 발령 데이터로 대체 표시
        summary_data = db.get_daily_personnel_summary("")
        is_showing_fallback = True

    # [인사] 또는 <인사> 기사 중 언론사 인사소식만 엄격 필터링
    all_raw_recs = [
        r for r in summary_data["raw_records"] 
        if is_personnel_article_title(r.get("article_title", "")) and is_media_organization(r.get("media", ""))
    ]
    media_news_list = []
    for mn in summary_data["media_news"]:
        if not is_media_organization(mn.get("media", "")):
            continue
        valid_recs = [
            r for r in mn.get("records", []) 
            if is_personnel_article_title(r.get("article_title", "")) and is_media_organization(r.get("media", ""))
        ]
        if valid_recs:
            by_act = {}
            for r in valid_recs:
                act = r.get("action_type", "발령")
                if act not in by_act:
                    by_act[act] = []
                by_act[act].append(r)
            mn_copy = dict(mn)
            mn_copy["records"] = valid_recs
            mn_copy["by_action"] = by_act
            media_news_list.append(mn_copy)

    # 키워드 필터링 적용 (언론사명, 기자명, 부서명)
    if filter_kw.strip():
        kw = filter_kw.strip().lower()
        filtered_media_news = []
        filtered_recs = []
        for mn in media_news_list:
            matched_records = [
                r for r in mn["records"]
                if kw in r.get("media", "").lower() or kw in r.get("name", "").lower() or kw in r.get("dept", "").lower() or kw in r.get("rank_title", "").lower()
            ]
            if matched_records or kw in mn.get("media", "").lower():
                recs_to_use = matched_records if matched_records else mn["records"]
                by_act = {}
                for r in recs_to_use:
                    act = r.get("action_type", "발령")
                    if act not in by_act:
                        by_act[act] = []
                    by_act[act].append(r)
                filtered_media_news.append({
                    "media": mn["media"],
                    "records": recs_to_use,
                    "by_action": by_act,
                    "latest_date": mn.get("latest_date", ""),
                    "article_title": mn.get("article_title", ""),
                    "article_url": mn.get("article_url", "")
                })
                filtered_recs.extend(recs_to_use)
        media_news_list = filtered_media_news
        all_raw_recs = filtered_recs

    # 상단 4대 핵심 모니터링 KPI 카드
    total_recs_count = len(all_raw_recs)
    total_media_count = len(media_news_list)
    promotions_count = sum(1 for r in all_raw_recs if "승진" in r.get("action_type", ""))
    transfers_count = sum(1 for r in all_raw_recs if any(k in r.get("action_type", "") for k in ["전보", "보임", "보직"]))
    
    # 최근 감지된 이직 건수
    job_changes = db.get_job_changes(limit=10)
    job_changes_count = len(job_changes)

    date_display_text = date_str if not is_showing_fallback else f"최근 수집"

    mcol1, mcol2, mcol3, mcol4 = st.columns(4)
    with mcol1:
        render_html(f"""
        <div class="metric-card">
            <div class="metric-label">모니터링 기준 일자</div>
            <div class="metric-value" style="font-size:1.4rem;">{date_display_text}</div>
            <div class="metric-sub">공식 인사 기사 모니터링</div>
        </div>
        """)
    with mcol2:
        render_html(f"""
        <div class="metric-card">
            <div class="metric-label">발령 언론사 수</div>
            <div class="metric-value">{total_media_count}개사</div>
            <div class="metric-sub">주요 신문·방송·통신</div>
        </div>
        """)
    with mcol3:
        render_html(f"""
        <div class="metric-card">
            <div class="metric-label">총 발령 인원</div>
            <div class="metric-value">{total_recs_count}명</div>
            <div class="metric-sub">승진 {promotions_count}건 · 전보/보임 {transfers_count}건</div>
        </div>
        """)
    with mcol4:
        render_html(f"""
        <div class="metric-card">
            <div class="metric-label">매체 간 이직 탐지</div>
            <div class="metric-value">{job_changes_count}건</div>
            <div class="metric-sub">최근 매체 이동 감지</div>
        </div>
        """)

    st.markdown("<br>", unsafe_allow_html=True)

    if is_showing_fallback and not filter_kw.strip():
        st.info(f"{date_str} 일자의 공식 [인사]/<인사> 기사가 아직 수집되지 않아 가장 최근 수집된 인사 소식을 표시하고 있습니다. 실시간 기사를 확인하시려면 상단 '당일 인사 실시간 수집 및 새로고침' 버튼을 눌러주세요.")

    if not media_news_list:
        st.warning("표시할 [인사]/<인사> 공식 인사 소식이 없습니다. 상단 '당일 인사 실시간 수집 및 새로고침' 버튼을 눌러 온라인 기사에서 발령 명단을 수집하세요.")
    else:
        st.markdown("#### 언론사별 인사 소식 정리")

        for mn in media_news_list:
            m_name = mn["media"]
            m_recs = mn["records"]
            m_by_act = mn.get("by_action", {})
            m_date = mn.get("latest_date", "")
            art_title = mn.get("article_title", "")
            art_url = mn.get("article_url", "")

            # 기사 원문 링크
            art_link_html = ""
            if art_url:
                art_link_html = f'<div style="margin-top:6px;"><a href="{art_url}" target="_blank" style="font-size:0.85rem; font-weight:600; color:#2563EB; text-decoration:underline;">기사 원문: {art_title or (m_name + " 인사")}</a></div>'
            elif art_title:
                art_link_html = f'<div style="margin-top:6px; font-size:0.85rem; color:#475569;">기사 제목: <b>{art_title}</b></div>'

            # 발령 구분별 요약 렌더링
            actions_html = '<div style="margin-top:12px; display:flex; flex-direction:column; gap:8px;">'
            for act_name, act_list in m_by_act.items():
                if "승진" in act_name:
                    badge_style = "background:#DCFCE7; color:#15803D; border:1px solid #BBF7D0;"
                elif "전보" in act_name or "보직" in act_name or "보임" in act_name:
                    badge_style = "background:#DBEAFE; color:#1D4ED8; border:1px solid #BFDBFE;"
                else:
                    badge_style = "background:#F1F5F9; color:#334155; border:1px solid #E2E8F0;"

                people_text_list = []
                for r in act_list:
                    d = r.get("dept", "").strip()
                    rt = r.get("rank_title", "").strip()
                    n = r.get("name", "").strip()
                    dept_str = f"{d} " if d and d != "본사" else ""
                    people_text_list.append(f"<b>{n}</b> ({dept_str}{rt})")

                actions_html += f"""
                <div style="font-size:0.92rem; line-height:1.6; background:#F8FAFC; padding:8px 12px; border-radius:6px; border:1px solid #F1F5F9;">
                    <span style="display:inline-block; padding:2px 8px; border-radius:4px; font-size:0.8rem; font-weight:700; margin-right:8px; {badge_style}">{act_name}</span>
                    <span style="color:#1E293B;">{", ".join(people_text_list)}</span>
                </div>
                """
            actions_html += '</div>'

            render_html(f"""
            <div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:10px; padding:18px 20px; margin-bottom:14px; box-shadow:0 1px 3px rgba(0,0,0,0.03);">
                <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
                    <div>
                        <span style="font-size:1.2rem; font-weight:800; color:#0F172A;">{m_name}</span>
                        <span style="margin-left:8px; font-size:0.82rem; font-weight:700; background:#E2E8F0; color:#334155; padding:3px 8px; border-radius:12px;">발령 {len(m_recs)}명</span>
                    </div>
                    <div style="font-size:0.85rem; color:#64748B; font-weight:600;">
                        발령 일자: {m_date}
                    </div>
                </div>
                {art_link_html}
                {actions_html}
            </div>
            """)

        # 전체 명단 표 & 다운로드
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 인사 소식")
        
        table_rows = []
        for r in all_raw_recs:
            table_rows.append({
                "언론사": r.get("media", ""),
                "발령구분": r.get("action_type", "발령"),
                "부서": r.get("dept", ""),
                "직급": r.get("rank_title", ""),
                "성명": r.get("name", ""),
                "발령일자": r.get("pub_date", ""),
                "기사원문(링크주소)": r.get("article_url", "")
            })
        
        if table_rows:
            df_table = pd.DataFrame(table_rows)
            st.dataframe(
                df_table,
                column_config={
                    "기사원문(링크주소)": st.column_config.LinkColumn(
                        "기사원문(링크주소)",
                        help="클릭 시 포털 기사 원문 페이지로 이동합니다.",
                        validate=r"^https?://.*",
                        max_chars=60,
                        display_text="기사 원문 열기"
                    )
                },
                use_container_width=True, 
                hide_index=True
            )

            # 엑셀 다운로드
            dl_buf = io.BytesIO()
            with pd.ExcelWriter(dl_buf, engine="openpyxl") as writer:
                df_table.to_excel(writer, sheet_name="인사소식", index=False)
            
            st.download_button(
                "인사 소식 엑셀 다운로드",
                data=dl_buf.getvalue(),
                file_name=f"{date_str}_언론사인사소식.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="btn_dl_daily"
            )

        # 이직 모니터링 특이사항
        if job_changes:
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("#### 최근 매체 간 이직 모니터링")
            st.caption("인사 리스트를 기반으로 자동 감지된 매체 이동 내역입니다.")

            for jc in job_changes[:5]:
                render_html(f"""
                <div class="jobchange-card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                        <div style="font-size: 1.1rem; font-weight: 800; color: #0F172A;">
                            <span style="color: #475569;">이직</span> {jc['name']} 기자
                        </div>
                        <div style="font-size: 0.82rem; color: #64748B; font-weight: 600;">
                            발령일: {jc['to_date']} : 이전 발령 후 {jc['days_gap']}일 경과
                        </div>
                    </div>
                    <div style="font-size: 1rem; font-weight: 700; color: #1E293B; margin: 6px 0;">
                        <span style="color: #64748B;">{jc['from_media']} {jc['from_dept']}</span> {jc['from_rank']}
                        &nbsp;➔&nbsp;
                        <span style="color: #0F172A;">{jc['to_media']} {jc['to_dept']}</span> {jc['to_rank']}
                    </div>
                    <div style="font-size: 0.82rem; color: #475569; display: flex; gap: 8px; align-items: center; margin-top: 6px;">
                        <span class="badge badge-normal">신뢰도: {jc['confidence_score']}점</span>
                        <span>탐지 근거: <b>{jc['detected_reason']}</b></span>
                    </div>
                </div>
                """)

