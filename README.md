# 📰 언론사 인사 트래커 (Media Personnel Tracker)

한국언론진흥재단 **빅카인즈(BIG Kinds)**의 대량 인사 발령 기사 텍스트와 **네이버 뉴스 바이라인 이메일** 식별 방식을 결합하여, 언론인의 이동(승진, 보직 이동, 매체 간 이직)을 추적하고 검색할 수 있는 풀스택 Streamlit 웹 애플리케이션입니다.

---

## 🌟 핵심 기능 및 알고리즘

### 1. 빅카인즈 대량 발령 기사 Regex 고속 파서
- 한국 언론사 특유의 발령 기호(`◆`, `◇`, `▲`, `△`, `■`, `◎`, `·` 등)와 섹션 헤더(`<승진>`, `<전보>`, `[국장급]`)를 분석.
- `[부서명] [직책/직급] [이름]` 패턴 및 쉼표 나열형, 복합 언론사 통합 기사를 정밀하게 고속 추출.

### 2. 동명이인 완전 분리를 위한 고유 식별자(Unique ID) 체계
- **1순위 (이메일 바이라인 매칭):**
  - 기사 본문 바이라인 또는 네이버 뉴스 검색 API에서 추출된 이메일 접두사(`{name}_{email_prefix}`)를 고유 키로 활용.
- **2순위 (알고리즘 휴리스틱 스코어링):**
  - **직급 연속성 검증:** 1~10단계 직급 서열 체계 기반, 단기간 급격한 강등(오탐) 또는 비정상 도약 분리.
  - **출입처/분야 연관성:** 8대 도메인(부동산/건설, 경제/산업/금융, 정치/외교, 사회/법조, IT/과학, 문화/스포츠 등) 친화도 매트릭스 적용.
  - **타임라인 중복 방지:** 30일 이내 서로 다른 언론사 동시 발령 기록은 물리적 불가능으로 판단하여 100% 강제 분리.
- **실제 검증 사례:**
  - `김희준 (한국경제 부동산부 (뉴스1 출신) / abc@news1.kr)` ➔ 뉴스1 건설부동산부 차장(2022) ➔ 부장(2023) ➔ 한국경제 부동산부 부장(2024 이직)으로 하나의 타임라인 연결.
  - `김희준 (중앙일보 사회부 / xyz@joongang.co.kr)` ➔ 중앙일보 사회부 기자(2022) ➔ 차장대우(2024)로 완전 별개 인물로 분리.

### 3. 매체 간 이직(Job Change) 자동 탐지 엔진
- $M_1$ 매체 최종 직급과 $M_2$ 매체 신규 직급 간 수평 이동($\pm 1$ 직급) 또는 정상 승진($+2$), 도메인 유사도 충족 시 '이직' 이벤트 생성 및 경로 매핑.

### 4. 세련된 모던 카드 UI & 단독 사용 보안
- [dahi-screener](https://dahi-screener.streamlit.app/) 스타일의 반응형 와이드 카드 인터페이스 (`Pretendard` 타이포그래피, 섀도우, 배지).
- `.streamlit/secrets.toml`의 비밀번호를 활용한 세션 기반 인증 (`st.session_state.authenticated`)으로 PC 및 모바일 단독 접속 보호.

---

## 📁 프로젝트 디렉토리 구조

```plaintext
media_personnel_tracker/
├── .streamlit/
│   └── secrets.toml          # 단독 사용 비밀번호 및 네이버 API 키 설정
├── data/
│   ├── bigkinds_personnel_sample.xlsx  # 빅카인즈 표준 엑셀 파일 (자동 생성)
│   └── bigkinds_personnel_sample.csv   # 빅카인즈 표준 CSV 파일 (자동 생성)
├── sample_data/
│   ├── generate_sample_data.py         # 동명이인 & 이직 시나리오 샘플 생성기
│   ├── bigkinds_personnel_sample.xlsx
│   └── bigkinds_personnel_sample.csv
├── app.py                    # Streamlit 프론트엔드 (인증, 검색, 타임라인, 이직 레이더)
├── data_pipeline.py          # 정규식 파서, 도메인 분류, 동명이인 클러스터링, 이직 탐지
├── test_pipeline.py          # 파이프라인 검증용 스크립트
├── requirements.txt          # 파이썬 의존성 패키지 목록
└── README.md                 # 프로젝트 가이드
```

---

## 🚀 빠른 시작 (Local Quickstart)

### 1. 패키지 설치
```bash
pip install -r requirements.txt
```

### 2. 샘플 데이터 생성 (선택 사항)
```bash
python sample_data/generate_sample_data.py
```

### 3. 애플리케이션 실행
```bash
streamlit run app.py
```
- 브라우저가 열리면 비밀번호 `admin1234`를 입력하여 로그인합니다. (비밀번호는 `.streamlit/secrets.toml`에서 변경 가능)

---

## ☁️ Streamlit Community Cloud 배포 가이드

1. **GitHub 저장소 푸시:**
   - 본 프로젝트 폴더를 본인의 GitHub 저장소에 푸시합니다.
   - 단, `.streamlit/secrets.toml` 파일은 보안상 `.gitignore`에 추가하는 것을 권장합니다.

2. **Streamlit Community Cloud 접속 및 배포:**
   - [share.streamlit.io](https://share.streamlit.io/)에 접속하여 GitHub 계정으로 로그인합니다.
   - **New app** 버튼을 클릭하고 해당 저장소와 브랜치, `app.py`를 지정합니다.

3. **Secrets 환경 설정 (중요):**
   - 배포 설정 화면의 **Advanced settings...** ➔ **Secrets** 탭에 아래 내용을 입력합니다.
   ```toml
   admin_password = "본인만의_비밀번호_입력"
   
   # 네이버 뉴스 검색 API (선택 사항 - 바이라인 이메일 자동 동기화용)
   naver_client_id = "발급받은_CLIENT_ID"
   naver_client_secret = "발급받은_CLIENT_SECRET"
   ```

4. **Deploy 버튼 클릭:**
   - 1~2분 내로 모바일과 PC에서 언제든지 접속할 수 있는 단독 전용 URL이 생성됩니다.

---

## 🔑 네이버 뉴스 검색 API 발급 안내 (선택 사항)

1. [네이버 개발자 센터](https://developers.naver.com/) 접속 및 로그인
2. **Application ➔ 애플리케이션 등록** 메뉴 이동
3. 애플리케이션 이름 입력 후 사용 API에서 **'검색'** 선택
4. 발급된 **Client ID**와 **Client Secret**을 `.streamlit/secrets.toml` 또는 웹 대시보드의 **'데이터 관리'** 탭에 입력하면, 이메일이 없는 기자들의 바이라인 이메일을 원클릭으로 탐색하여 동기화할 수 있습니다.
