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
    NaverNewsEmailFinder,
    load_and_process_bigkinds_file,
    normalize_media_name,
    DOMAIN_TAXONOMY,
    RANK_HIERARCHY
)


# ==========================================
# 1. 페이지 설정 및 모던 카드 스타일 CSS
# ==========================================

st.set_page_config(
    page_title="언론사 인사 트래커 (Media Personnel Tracker)",
    page_icon="📰",
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
        border: 1px solid #D61F69;
    }
    .badge-normal {
        background-color: #F1F5F9;
        color: #475569;
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
</style>
""", unsafe_allow_html=True)


# ==========================================
# 2. 세션 기반 개인 전용 보안 인증
# ==========================================

def get_secret(key: str, default: str = "") -> str:
    """Streamlit Secrets 또는 환경 변수에서 값을 안전하게 조회합니다."""
    try:
        return st.secrets.get(key, default)
    except Exception:
        return os.environ.get(key, default)


# ==========================================
# 3. 데이터베이스 및 서비스 초기화
# ==========================================

DB_FILE = "media_tracker.db"
db = MediaDatabase(DB_FILE)

# 초기 실행 시 DB가 비어있고 샘플 데이터가 존재하면 자동 1회 로드
stats = db.get_stats()
if stats["reporters"] == 0:
    sample_file = "sample_data/bigkinds_personnel_sample.xlsx"
    if os.path.exists(sample_file):
        load_and_process_bigkinds_file(sample_file)
        stats = db.get_stats()


# ==========================================
# 4. 사이드바 및 네비게이션
# ==========================================

with st.sidebar:
    st.markdown("""
    <div style="padding: 10px 0 16px 0;">
        <span style="font-size: 1.5rem; font-weight: 800; color: #1E293B;">📰 인사 트래커</span>
        <div style="font-size: 0.8rem; color: #64748B; margin-top: 2px;">Media Personnel Radar</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    
    # DB 현황 요약 미니 위젯
    st.markdown("**📊 데이터베이스 현황**")
    st.write(f"• 등록 언론인: **{stats['reporters']:,}명**")
    st.write(f"• 인사 발령 기록: **{stats['records']:,}건**")
    st.write(f"• 탐지된 이직: **{stats['changes']:,}건**")
    st.write(f"• 수집 언론사: **{stats['media_count']:,}개사**")


# ==========================================
# 5. 메인 대시보드 탭 레이아웃
# ==========================================

st.markdown("""
<div style="margin-bottom: 20px;">
    <h2 style="font-weight: 800; color: #0F172A; margin-bottom: 4px;">언론사 인사 트래커 (Media Personnel Tracker)</h2>
    <div style="font-size: 0.95rem; color: #64748B;">
        빅카인즈 대량 발령 데이터와 바이라인 이메일을 결합한 동명이인 분리 및 이직 탐지 대시보드
    </div>
</div>
""", unsafe_allow_html=True)

tab_search, tab_jobchanges, tab_analytics, tab_data = st.tabs([
    "🔍 통합 스마트 검색",
    "🔄 최근 이직 모니터링",
    "📊 이동 통계 및 네트워크",
    "⚙️ 데이터 관리 및 수집"
])


# =========================================================================
# TAB 1: 🔍 통합 스마트 검색
# =========================================================================
with tab_search:
    st.markdown("### 🔍 언론인 검색 및 경력 타임라인")

    search_mode = st.radio(
        "검색 모드 선택",
        ["이름으로 검색 (동명이인 구분)", "매체 + 부서 / 직급 필터 검색"],
        horizontal=True
    )

    all_reporters = db.get_all_reporters()

    selected_reporter_id = None

    if search_mode == "이름으로 검색 (동명이인 구분)":
        col_s1, col_s2 = st.columns([1, 2])
        with col_s1:
            name_query = st.text_input("기자 이름 입력", placeholder="예: 김희준, 이영희, 박철수").strip()

        if name_query:
            matched_reporters = [r for r in all_reporters if name_query in r["name"]]
        else:
            matched_reporters = all_reporters

        if not matched_reporters:
            st.warning(f"'{name_query}' 검색 결과가 없습니다.")
        else:
            # 동명이인이 존재할 경우 상세 표시명(출신 언론사 및 이메일) 드롭다운 제공
            options_dict = {r["display_label"]: r["reporter_id"] for r in matched_reporters}
            with col_s2:
                selected_label = st.selectbox(
                    f"인물 선택 (검색된 기자 {len(matched_reporters)}명)",
                    options=list(options_dict.keys()),
                    help="동명이인이 있는 경우 출신 매체, 현재 부서, 이메일 정보로 명확히 구분됩니다."
                )
                selected_reporter_id = options_dict[selected_label]

    else:
        # 매체 + 부서 / 직급 필터 검색 모드
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            media_input = st.text_input("매체명 (예: 뉴스1, 조선일보, 한국경제)")
        with fcol2:
            dept_input = st.text_input("부서명 (예: 건설부동산부, 사회부, 정치부)")
        with fcol3:
            rank_input = st.text_input("직급명 (예: 부장, 차장, 부국장)")

        filtered = db.search_reporters(media=media_input, dept=dept_input, rank=rank_input)
        if not filtered:
            st.info("조건에 부합하는 언론인 기록이 없습니다.")
        else:
            opt_dict = {f"{r['display_label']} [최종확인: {r['last_seen']}]": r["reporter_id"] for r in filtered}
            sel_l = st.selectbox(f"검색 결과 ({len(filtered)}명)", list(opt_dict.keys()))
            selected_reporter_id = opt_dict[sel_l]

    # 선택된 기자의 상세 프로필 및 타임라인 렌더링
    if selected_reporter_id:
        reporter_obj = next((r for r in all_reporters if r["reporter_id"] == selected_reporter_id), None)
        timeline = db.get_reporter_timeline(selected_reporter_id)

        if reporter_obj and timeline:
            # 프로필 카드
            total_records = len(timeline)
            job_change_count = sum(1 for t in timeline if "이직" in t.get("action_type", ""))
            current_media = reporter_obj["current_media"]
            current_dept = reporter_obj["current_dept"]
            current_rank = reporter_obj["current_rank"]
            email = reporter_obj.get("email", "")
            first_date = reporter_obj["first_seen"]
            last_date = reporter_obj["last_seen"]

            st.markdown(f"""
            <div class="profile-card">
                <div class="profile-name">
                    <span>{reporter_obj['name']}</span>
                    <span class="badge badge-transfer">{current_media}</span>
                    <span class="badge badge-normal">{current_dept}</span>
                    <span class="badge badge-promotion">{current_rank}</span>
                    {'<span class="badge badge-jobchange">이직 이력 있음</span>' if job_change_count > 0 else ''}
                </div>
                <div class="profile-meta">
                    • <b>고유 식별자(ID):</b> <code>{reporter_obj['reporter_id']}</code><br>
                    • <b>바이라인 이메일:</b> {f'<a href="mailto:{email}">{email}</a>' if email else '<span style="color:#94A3B8;">미등록 (네이버 API 동기화 가능)</span>'}<br>
                    • <b>활동 추적 기간:</b> {first_date} ~ {last_date} (총 {total_records}건 발령 / 매체 간 이직 {job_change_count}회)
                </div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("#### ⏳ 커리어 이동 타임라인 (Chronological Timeline)")

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
                        <div class="timeline-body">
                            <b>발령 원문:</b> <code>{item.get('raw_text', '')}</code>
                        </div>
                    </div>
                </div>
                """
            timeline_html += '</div>'
            st.markdown(timeline_html, unsafe_allow_html=True)


# =========================================================================
# TAB 2: 🔄 최근 이직 모니터링
# =========================================================================
with tab_jobchanges:
    st.markdown("### 🔄 최근 매체 간 이직 모니터링 (Job Change Radar)")
    st.markdown("정규표현식 파싱 및 알고리즘 휴리스틱(직급 연속성, 도메인 친화도, 이메일 일치)을 통해 탐지된 언론인 매체 이동 내역입니다.")

    changes = db.get_job_changes(limit=200)

    if not changes:
        st.info("탐지된 매체 간 이직 데이터가 없습니다. 상단 '데이터 관리' 탭에서 데이터를 로드하세요.")
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
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">총 이직 탐지 건수</div>
                <div class="metric-value">{total_jc}건</div>
                <div class="metric-sub">매체 간 이동 감지</div>
            </div>
            """, unsafe_allow_html=True)
        with mcol2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">최다 유입 언론사</div>
                <div class="metric-value">{top_in}</div>
                <div class="metric-sub">외부 기자 적극 영입</div>
            </div>
            """, unsafe_allow_html=True)
        with mcol3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">최다 유출 언론사</div>
                <div class="metric-value">{top_out}</div>
                <div class="metric-sub">타사 이직 발생 매체</div>
            </div>
            """, unsafe_allow_html=True)
        with mcol4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">평균 알고리즘 신뢰도</div>
                <div class="metric-value">{avg_score}점</div>
                <div class="metric-sub">연속성·도메인 일치도</div>
            </div>
            """, unsafe_allow_html=True)

        # 필터링 컨트롤
        st.markdown("##### 🔍 이직 내역 필터")
        fcol1, fcol2, fcol3 = st.columns(3)
        with fcol1:
            media_filter = st.selectbox("언론사 필터 (유입/유출)", ["전체"] + sorted(list(set(df_changes["from_media"].tolist() + df_changes["to_media"].tolist()))))
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
            st.markdown(f"""
            <div class="jobchange-card">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                    <div style="font-size: 1.15rem; font-weight: 800; color: #0F172A;">
                        <span style="color: #7C3AED;">[이직]</span> {row['name']} 기자
                    </div>
                    <div style="font-size: 0.85rem; color: #64748B; font-weight: 600;">
                        발령일: {row['to_date']} (이전 발령 후 {row['days_gap']}일 경과)
                    </div>
                </div>
                <div style="font-size: 1.05rem; font-weight: 700; color: #1E293B; margin: 8px 0;">
                    <span style="color: #DC2626;">{row['from_media']} {row['from_dept']}</span> ({row['from_rank']})
                    &nbsp;➔&nbsp;
                    <span style="color: #2563EB;">{row['to_media']} {row['to_dept']}</span> ({row['to_rank']})
                </div>
                <div style="font-size: 0.85rem; color: #475569; display: flex; gap: 8px; align-items: center; margin-top: 6px;">
                    <span class="badge badge-jobchange">신뢰도: {row['confidence_score']}점</span>
                    <span>탐지 근거: <b>{row['detected_reason']}</b></span>
                </div>
            </div>
            """, unsafe_allow_html=True)


# =========================================================================
# TAB 3: 📊 이동 통계 및 네트워크
# =========================================================================
with tab_analytics:
    st.markdown("### 📊 언론인 이동 통계 및 출입처 분석")

    with db.get_connection() as conn:
        df_all_rec = pd.read_sql_query("SELECT * FROM personnel_records", conn)

    if df_all_rec.empty:
        st.info("통계 분석을 위한 데이터가 부족합니다.")
    else:
        acol1, acol2 = st.columns(2)
        with acol1:
            st.markdown("##### 🏢 주요 언론사별 인사 발령 건수")
            media_counts = df_all_rec["media"].value_counts().reset_index()
            media_counts.columns = ["언론사", "발령 건수"]
            st.dataframe(media_counts, use_container_width=True, hide_index=True)

        with acol2:
            st.markdown("##### 📁 출입처 도메인별 인사 분포")
            domain_counts = df_all_rec["domain"].value_counts().reset_index()
            domain_counts.columns = ["도메인", "발령 건수"]
            st.dataframe(domain_counts, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("##### 📈 직급별 발령 분포")
        rank_counts = df_all_rec["rank_title"].value_counts().head(10).reset_index()
        rank_counts.columns = ["직급", "건수"]
        st.bar_chart(data=rank_counts.set_index("직급"), use_container_width=True)


# =========================================================================
# TAB 4: ⚙️ 데이터 관리 및 수집
# =========================================================================
with tab_data:
    st.markdown("### ⚙️ 데이터 파이프라인 관리 및 수집")

    dcol1, dcol2 = st.columns(2)

    with dcol1:
        st.markdown("#### 📂 빅카인즈 파일 업로드")
        st.write("빅카인즈에서 다운로드한 엑셀(`.xlsx`) 또는 CSV(`.csv`) 파일을 업로드하세요.")
        uploaded_file = st.file_uploader(
            "빅카인즈 파일 선택",
            type=["xlsx", "xls", "csv"],
            help="일자, 언론사, 제목, 본문 컬럼이 포함되어 있어야 합니다."
        )

        if uploaded_file is not None:
            if st.button("🚀 업로드 파일 처리 및 DB 반영", use_container_width=True):
                with st.spinner("빅카인즈 기사 본문 정규식 고속 파싱 및 동명이인 클러스터링 중..."):
                    try:
                        res = load_and_process_bigkinds_file(uploaded_file)
                        st.success(
                            f"데이터 반영 완료! (추출 레코드: {res['total_records']}건, "
                            f"고유 언론인: {res['unique_reporters']}명, "
                            f"탐지된 이직: {res['job_changes_detected']}건)"
                        )
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"파일 처리 중 오류가 발생했습니다: {e}")

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 🧪 기본 샘플 데이터 로드")
        st.write("테스트용 빅카인즈 표준 샘플 데이터(뉴스1 김희준 이직 및 중앙일보 동명이인 시나리오 포함)를 즉시 로드합니다.")
        if st.button("📥 샘플 데이터 즉시 로드/초기화", use_container_width=True):
            sample_path = "sample_data/bigkinds_personnel_sample.xlsx"
            if os.path.exists(sample_path):
                with st.spinner("샘플 데이터 파싱 및 파이프라인 실행 중..."):
                    res = load_and_process_bigkinds_file(sample_path)
                    st.success(f"샘플 데이터 로드 완료! (추출 레코드 {res['total_records']}건, 이직 {res['job_changes_detected']}건)")
                    time.sleep(1)
                    st.rerun()
            else:
                st.error("샘플 데이터 파일을 찾을 수 없습니다.")

    with dcol2:
        st.markdown("#### 🌐 네이버 뉴스 검색 API 연동 (바이라인 이메일 동기화)")
        st.write("기자의 이름과 언론사명을 검색해 본문 바이라인에서 이메일을 추출하고 고유 식별자를 보완합니다.")

        naver_id = st.text_input("Naver Client ID", value=get_secret("naver_client_id", ""), type="password")
        naver_secret = st.text_input("Naver Client Secret", value=get_secret("naver_client_secret", ""), type="password")

        if st.button("🔄 네이버 API 바이라인 이메일 동기화 실행", use_container_width=True):
            if not naver_id or not naver_secret:
                st.warning("네이버 개발자 센터에서 발급받은 Client ID와 Client Secret을 입력해주세요.")
            else:
                with st.spinner("네이버 뉴스 검색 API를 통해 기자별 이메일 바이라인 탐색 중..."):
                    finder = NaverNewsEmailFinder(client_id=naver_id, client_secret=naver_secret)
                    reporters = db.get_all_reporters()
                    updated_cnt = 0
                    for r in reporters[:30]:  # API 호출 제한 고려 30명 우선 진행
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
                    st.success(f"이메일 동기화 완료! 총 {updated_cnt}명의 바이라인 이메일을 보완 등록했습니다.")
                    time.sleep(1)
                    st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 💾 데이터 내보내기")
        with db.get_connection() as conn:
            export_df = pd.read_sql_query("SELECT * FROM reporters", conn)
            csv_bytes = export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button(
                "📥 등록 기자 명단 CSV 다운로드",
                data=csv_bytes,
                file_name="media_reporters_master.csv",
                mime="text/csv",
                use_container_width=True
            )
