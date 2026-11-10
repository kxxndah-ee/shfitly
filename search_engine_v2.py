import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

DB_NAME = "media_personnel.db"

class MediaPersonnelEngine:
    def __init__(self, db_path: str = DB_NAME):
        self.db_path = db_path
        self._ensure_view_exists()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_view_exists(self):
        """
        [핵심 격리 장치]
        1. 출처 매체: '연합뉴스', '뉴스1'만 허용
        2. 기사 제목: '[인사]' 또는 '<인사>' 포함 기사만 허용
        3. 필터링: 순수 언론사 인사(is_media_company = 1)만 허용
        """
        conn = self._get_connection()
        cur = conn.cursor()

        # 기본 테이블들이 없으면 뷰 생성이 실패하므로 DDL 사전 보장
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS person_profiles (
            person_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            current_company TEXT,
            current_department TEXT,
            current_position TEXT,
            last_event_date DATE,
            needs_review BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS turnover_histories (
            turnover_id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            from_company TEXT NOT NULL,
            to_company TEXT NOT NULL,
            from_dept TEXT,
            to_dept TEXT,
            gap_days INTEGER NOT NULL,
            turnover_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_media_only_events AS
        SELECT 
            c.event_id,
            c.article_id,
            c.person_name,
            c.company_name,
            c.department_raw,
            c.dept_category,
            c.position_raw,
            c.rank_score,
            c.action_type,
            c.event_date,
            r.media_source,
            r.title,
            r.publish_date
        FROM career_events c
        JOIN raw_articles r ON c.article_id = r.article_id
        WHERE r.media_source IN ('연합뉴스', '뉴스1')
          AND (r.title LIKE '%[인사]%' OR r.title LIKE '%<인사>%')
          AND r.is_media_company = 1;
        """)
        conn.commit()
        conn.close()

    def _get_40_years_cutoff(self) -> str:
        """현재 시점(2026년 기준)으로부터 40년 전 일자 반환 (1986-XX-XX)"""
        cutoff = datetime.now() - timedelta(days=40 * 365.25)
        return cutoff.strftime("%Y-%m-%d")

    # -------------------------------------------------------------
    # 1. 매체·부서(직급 포함) 검색
    # -------------------------------------------------------------
    def search_by_org_and_dept(
        self,
        company: str,
        department: Optional[str] = None,
        position: Optional[str] = None,
        years_back: int = 40
    ) -> List[Dict[str, Any]]:
        """
        연합뉴스/뉴스1 [인사] 기사 중 특정 언론사 + 부서(+직급)에 발령받은 기자 명단 조회
        """
        conn = self._get_connection()
        cur = conn.cursor()

        cutoff_date = (datetime.now() - timedelta(days=years_back * 365.25)).strftime("%Y-%m-%d")

        query = """
        SELECT 
            person_name,
            company_name,
            department_raw,
            position_raw,
            action_type,
            event_date,
            media_source,
            title
        FROM v_media_only_events
        WHERE company_name LIKE ?
          AND event_date >= ?
        """
        params = [f"%{company.strip()}%", cutoff_date]

        if department and department.strip():
            query += " AND department_raw LIKE ?"
            params.append(f"%{department.strip()}%")

        if position and position.strip():
            query += " AND position_raw LIKE ?"
            params.append(f"%{position.strip()}%")

        query += " ORDER BY event_date DESC, person_name ASC"

        cur.execute(query, params)
        rows = cur.fetchall()

        results = []
        for r in rows:
            results.append({
                "name": r["person_name"],
                "company": r["company_name"],
                "department": r["department_raw"] or "부서 미표기",
                "position": r["position_raw"] or "직급 미표기",
                "action": r["action_type"] or "발령",
                "event_date": r["event_date"],
                "source": f"[{r['media_source']}] {r['title']}"
            })

        conn.close()
        return results

    # -------------------------------------------------------------
    # 2. 매체·이름 검색
    # -------------------------------------------------------------
    def search_by_media_and_name(
        self,
        company: str,
        name: str,
        years_back: int = 40
    ) -> Dict[str, Any]:
        """
        연합뉴스/뉴스1 [인사] 기사 중 특정 언론사에서 해당 인물이 거친 40년간의 인사 기록 및 타사 이직 연결
        """
        conn = self._get_connection()
        cur = conn.cursor()

        cutoff_date = (datetime.now() - timedelta(days=years_back * 365.25)).strftime("%Y-%m-%d")
        company_clean = company.strip()
        name_clean = name.strip()

        # 1) 해당 언론사 내 발령 시계열 조회
        query_events = """
        SELECT 
            person_name,
            company_name,
            department_raw,
            position_raw,
            action_type,
            event_date,
            media_source,
            title
        FROM v_media_only_events
        WHERE company_name LIKE ?
          AND person_name = ?
          AND event_date >= ?
        ORDER BY event_date ASC
        """
        cur.execute(query_events, (f"%{company_clean}%", name_clean, cutoff_date))
        rows = cur.fetchall()

        if not rows:
            conn.close()
            return {
                "found": False,
                "company": company_clean,
                "name": name_clean,
                "message": f"최근 {years_back}년간 연합뉴스·뉴스1 [인사] 기사 중 [{company_clean}] 소속 '{name_clean}' 기자의 인사 기록이 없습니다."
            }

        # 2) 타 언론사 이직 내역 연계 (연합/뉴스1 출처 기반 판정 데이터만)
        cur.execute("""
        SELECT 
            t.from_company,
            t.to_company,
            t.from_dept,
            t.to_dept,
            t.gap_days,
            t.turnover_date
        FROM turnover_histories t
        JOIN person_profiles p ON t.person_id = p.person_id
        WHERE p.name = ?
          AND (t.from_company LIKE ? OR t.to_company LIKE ?)
          AND t.turnover_date >= ?
        ORDER BY t.turnover_date ASC
        """, (name_clean, f"%{company_clean}%", f"%{company_clean}%", cutoff_date))
        turnovers = [dict(t) for t in cur.fetchall()]

        timeline = []
        for r in rows:
            timeline.append({
                "date": r["event_date"],
                "department": r["department_raw"] or "-",
                "position": r["position_raw"] or "-",
                "action": r["action_type"] or "발령",
                "source": f"[{r['media_source']}] {r['title']}"
            })

        conn.close()
        return {
            "found": True,
            "name": name_clean,
            "company": company_clean,
            "total_records": len(timeline),
            "timeline": timeline,
            "turnover_history": turnovers
        }
