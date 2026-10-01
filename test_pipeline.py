"""
test_pipeline.py
================
data_pipeline.py 의 동작 검증 스크립트.
"""

import io
import sys
import json
from data_pipeline import load_and_process_bigkinds_file, MediaDatabase

# 콘솔 출력 utf-8 강제
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

if __name__ == "__main__":
    result = load_and_process_bigkinds_file("sample_data/bigkinds_personnel_sample.xlsx")
    print("Process result:", json.dumps(result, ensure_ascii=False, indent=2))

    db = MediaDatabase()
    stats = db.get_stats()
    print("DB Stats:", stats)

    print("\n--- All Reporters ---")
    reporters = db.get_all_reporters()
    for r in reporters:
        print(f"[{r['reporter_id']}] {r['display_label']} (seen: {r['first_seen']} ~ {r['last_seen']})")

    print("\n--- Detected Job Changes ---")
    changes = db.get_job_changes()
    for c in changes:
        print(f"[{c['name']}] {c['from_media']} {c['from_dept']}({c['from_rank']}) -> {c['to_media']} {c['to_dept']}({c['to_rank']}) | 사유: {c['detected_reason']} | 점수: {c['confidence_score']}")

    # 김희준 기자 타임라인 검증
    print("\n--- Timeline for Reporters named '김희준' ---")
    h_reporters = [r for r in reporters if r["name"] == "김희준"]
    for hr in h_reporters:
        print(f"\nTimeline for {hr['display_label']} ({hr['reporter_id']}):")
        tl = db.get_reporter_timeline(hr["reporter_id"])
        for item in tl:
            print(f"  {item['pub_date']} | {item['media']} {item['dept']} | {item['rank_title']} | {item['action_type']}")
