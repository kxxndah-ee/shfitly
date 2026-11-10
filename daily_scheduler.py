"""
daily_scheduler.py
==================
daily_sync.py의 진입점 래퍼(Wrapper).
스케줄러 환경 및 명령줄에서 daily_scheduler.py 호출 시 동일하게 파이프라인을 구동합니다.
"""

import sys
from daily_sync import main

if __name__ == "__main__":
    sys.exit(main())
