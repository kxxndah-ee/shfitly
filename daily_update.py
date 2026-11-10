"""
daily_update.py
===============
언론사 인사 발령 데이터 자동 증분 수집(Incremental Update) 스케줄러 진입점.
- 수집 소스: 중앙일보 피플 인사탭 (https://www.joongang.co.kr/people/personnel)
- 1페이지부터 최신순으로 탐색하다가 이미 DB에 저장된 기사를 만나면 즉시 멈추는 증분 수집 방식
- 신규 기사 본문 추출, 순수 언론사 인사 발령 정밀 필터링 및 클러스터링 DB 자동 갱신
- Windows 작업 스케줄러(Task Scheduler) 및 Linux/macOS cron을 통한 1일 1회 정기 자동 실행 지원
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime
from typing import Optional

# UTF-8 출력 보장
if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from data_pipeline import OnlineNewsTracker, MediaDatabase, PersonnelTrackerEngine


def setup_logger(log_file: str = "daily_update.log") -> logging.Logger:
    """콘솔 및 파일 출력을 동시 지원하는 로거를 설정합니다."""
    logger = logging.getLogger("DailyPersonnelUpdate")
    logger.setLevel(logging.INFO)

    # 기존 핸들러 중복 방지
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러 (UTF-8 인코딩)
    try:
        file_handler = logging.FileHandler(log_file, encoding="utf-8", mode="a")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"[경고] 로그 파일 생성 실패 ({e}), 콘솔 출력만 유지합니다.")

    return logger


def print_scheduler_guide(python_exec: str, script_path: str, log_path: str):
    """Windows 작업 스케줄러 및 cron 등록 안내를 출력합니다."""
    print("\n" + "=" * 70)
    print("1일 1회 자동 실행(스케줄러) 설정 가이드")
    print("=" * 70)

    print("\n1. Windows 작업 스케줄러 (Task Scheduler) 등록 방법")
    print("   아래 명령어를 PowerShell 또는 명령 프롬프트(관리자 권한)에서 실행하세요.")
    print("   (매일 오전 06:00에 자동 실행되도록 등록하는 예시입니다)")
    print("-" * 70)
    schtasks_cmd = (
        f'schtasks /create /tn "MediaPersonnelDailyUpdate" '
        f'/tr "\\"{python_exec}\\" \\"{script_path}\\"" '
        f'/sc daily /st 06:00 /f'
    )
    print(schtasks_cmd)
    print("-" * 70)
    print("   * 등록된 작업 확인: schtasks /query /tn \"MediaPersonnelDailyUpdate\"")
    print("   * 작업 삭제: schtasks /delete /tn \"MediaPersonnelDailyUpdate\" /f")

    print("\n2. Linux / macOS crontab 등록 방법")
    print("   터미널에서 'crontab -e' 명령을 실행한 후 아래 한 줄을 추가하세요.")
    print("-" * 70)
    cron_line = f"0 6 * * * \"{python_exec}\" \"{script_path}\" >> \"{log_path}\" 2>&1"
    print(cron_line)
    print("-" * 70)
    print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="언론사 인사 발령 데이터 자동 증분 수집(Daily Update) 스케줄러"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="중앙일보 인사 섹션 최대 탐색 페이지 수 (기본값: 10)"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="media_tracker.db",
        help="메인 SQLite 데이터베이스 파일 경로 (기본값: media_tracker.db)"
    )
    parser.add_argument(
        "--secondary-db",
        type=str,
        default="media_personnel.db",
        help="보조(검색엔진용) SQLite 데이터베이스 파일 경로 (기본값: media_personnel.db)"
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="daily_update.log",
        help="실행 로그 저장 파일 경로 (기본값: daily_update.log)"
    )
    parser.add_argument(
        "--no-stop",
        action="store_true",
        help="기존 기사를 만나도 멈추지 않고 지정된 최대 페이지까지 전수 탐색"
    )
    parser.add_argument(
        "--include-wires",
        action="store_true",
        help="중앙일보 외에 통신사(연합뉴스·뉴스1) 당일 인사 기사 보조 수집 함께 실행"
    )
    parser.add_argument(
        "--guide",
        action="store_true",
        help="Windows 작업 스케줄러 및 cron 등록 가이드를 출력하고 종료합니다."
    )

    args = parser.parse_args()

    # 현재 작업 디렉터리 기준 절대 경로 산출
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_abs = os.path.join(base_dir, args.db_path) if not os.path.isabs(args.db_path) else args.db_path
    sec_db_abs = os.path.join(base_dir, args.secondary_db) if not os.path.isabs(args.secondary_db) else args.secondary_db
    log_abs = os.path.join(base_dir, args.log_file) if not os.path.isabs(args.log_file) else args.log_file
    script_abs = os.path.abspath(__file__)
    python_exec = sys.executable

    if args.guide:
        print_scheduler_guide(python_exec, script_abs, log_abs)
        return 0

    logger = setup_logger(log_abs)
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("[언론사 인사 발령 일일 증분 수집 시작]")
    logger.info(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"파이썬 실행 경로: {python_exec}")
    logger.info(f"메인 DB 경로: {db_abs}")
    logger.info(f"최대 탐색 페이지: {args.max_pages}페이지")
    logger.info(f"증분 수집 조기 종료 모드: {'비활성화 (--no-stop)' if args.no_stop else '활성화 (기존 기사 감지 시 즉시 정지)'}")
    logger.info("=" * 60)

    try:
        # 1. 중앙일보 피플 공식 인사탭 증분 수집 실행
        summary = OnlineNewsTracker.crawl_joongang_personnel_incremental(
            max_pages=args.max_pages,
            db_path=db_abs,
            secondary_db_path=sec_db_abs,
            stop_on_existing=(not args.no_stop),
            logger=logger
        )

        # 2. 통신사 보조 수집 (옵션)
        wire_summary = None
        if args.include_wires:
            logger.info("\n[통신사 보조 수집] 연합뉴스 및 뉴스1 당일 인사 소식 수집 실행 중...")
            try:
                wire_summary = OnlineNewsTracker.fetch_daily_personnel_news()
                logger.info(f"통신사 기사 수집 결과: {wire_summary.get('message', '완료')}")
            except Exception as we:
                logger.warning(f"통신사 보조 수집 중 경고 발생: {we}")

        elapsed = time.time() - start_time

        logger.info("\n" + "=" * 60)
        logger.info("[일일 증분 수집 결과 요약]")
        logger.info(f"탐색 완료 페이지 수: {summary.get('pages_checked', 0)}페이지")
        logger.info(f"확인한 총 기사 수: {summary.get('articles_checked', 0)}건")
        logger.info(f"새로 수집된 신규 기사: {summary.get('new_articles_count', 0)}건")
        logger.info(f"새로 등록된 언론인 발령: {summary.get('new_records_count', 0)}건")
        
        if summary.get("stopped_on_existing_article"):
            logger.info("증분 수집 조기 종료: 기존 기사 발견으로 이전 기사 탐색을 안전하게 생략했습니다.")
            logger.info(f"종료 기준 기사 URL: {summary.get('stopped_article_url')}")
        else:
            logger.info("증분 수집 종료: 지정된 최대 탐색 페이지까지 전수 검토 완료.")

        logger.info(f"총 소요 시간: {elapsed:.2f}초")
        logger.info("=" * 60 + "\n")

        return 0

    except Exception as e:
        logger.error(f"[오류 발생] 일일 업데이트 중 예외가 발생했습니다: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
