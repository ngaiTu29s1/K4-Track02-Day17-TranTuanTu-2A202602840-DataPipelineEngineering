# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Trần Tuấn Tú / 2A202602840  
**Repo:** https://github.com/ngaiTu29s1/K4-Track02-Day17-TranTuanTu-2A202602840-DataPipelineEngineering  
**Commit bài nộp:** ba1177e  
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity AI (Gemini 3.8 Flash) hỗ trợ phân tích triệu chứng lỗi từ verify log, đề xuất giải pháp LSN guard cho MERGE, giải thích cơ chế Debezium delete envelope và hoàn thiện cấu trúc báo cáo.  
**Nguồn tham khảo khác (nếu có):** Slide bài giảng K4 Track 02 Ngày 17, tài liệu Debezium CDC Postgres connector, tài liệu dbt microbatch model.  

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `make verify` báo `silver_tickets has exactly one row per ticket_id (24 rows for 12 tickets)`, T-91 có 3 dòng trạng thái thay vì `high / closed / bug`. | `make verify` báo `gold_feature_daily` lệch checksum recompute (`c50b8851affe != 8630e04a61d1`); u05 ngày 08-12 nhận `(2, 0)` thay vì `(5, 1)`; check lateness fail (`0 < 3`). | `make verify` báo T-97 không là tombstone (`is_deleted=False`, còn PII); T-97 còn trong snapshot `v2026-08-16` (1 row) và trong `gold_doc_chunks` RAG index (2 chunks). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` (`pipeline/silver.py`) dùng `INSERT INTO` thuần tuý, không có khoá định danh và thiếu LSN guard, dẫn đến duplicate hàng và batch cũ ghi đè hỏng batch mới. | `pipeline/config.py` đặt cứng `LOOKBACK_DAYS = 0`, giả định ngây thơ dữ liệu đến ngay trong ngày; khi u05 mất mạng gửi trễ 3 ngày (08-15), pipeline không tính lại partition quá khứ. | `ticket_changes_sql` (`pipeline/staging.py`) chỉ đọc `after->>'ticket_id'`. Khi `op = 'd'`, `after = null` nên `ticket_id` thành NULL và bị lọc bỏ bởi `WHERE ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: Đổi sang `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn >= t._lsn THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...`. | `pipeline/config.py`: Đổi `LOOKBACK_DAYS = 3` (tương ứng với $\lceil P99 \rceil = 3$ ngày đo đạc thực tế từ Bronze qua lệnh `make lateness`). | `pipeline/staging.py`: Trích xuất `coalesce(after.ticket_id, before.ticket_id, key.ticket_id) AS ticket_id`. T-97 thành tombstone (`is_deleted = true`, PII = NULL), tự động bị loại khỏi Gold. |
| **Khái niệm trên slide** | *Silver — Có khoá* (1 hàng = 1 thực thể), *4 cách viết idempotent* (MERGE theo khoá và chặn bằng LSN). | *Data về muộn (Late-arriving data)*, *Event time vs Ingestion time*, *Lookback window = ceil(P99)*, *Overwrite-partition*. | *CDC log-based* (Phong bì Debezium: before/after/op, tombstone), *Xoá phải lan (Delete propagation / GDPR)*. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể có vòng đời thay đổi liên tục theo LSN cần MERGE theo khoá định danh, còn `gold_feature_daily` là bảng tổng hợp số liệu theo ngày sự kiện nên dùng overwrite-partition vừa đơn giản, chạy lại idempotent vừa dễ dàng recompute khi có dữ liệu đến muộn trong cửa sổ lookback.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ tombstone kèm LSN lớn nhất giúp ngăn chặn việc "hồi sinh" (resurrect) bản ghi cũ khi replay/backfill log từ Kafka, đồng thời lưu dấu vết kiểm toán (audit trail) rằng thực thể đã bị xoá mà vẫn xoá sạch dữ liệu định danh cá nhân (PII = NULL).
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Mô hình học máy cần tính tái lập (reproducibility) tuyệt đối để debug và benchmark; việc tái dựng snapshot theo thời điểm (point-in-time) từ Bronze đảm bảo không xảy ra rò rỉ dữ liệu tương lai (data leakage) và dữ liệu huấn luyện của quá khứ không bị thay đổi ngầm.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu của bài toán nằm trong phạm vi vừa và nhỏ (vài chục MB tới vài GB) mà máy đơn có thể xử lý trọn vẹn trong RAM; DuckDB chạy in-process với zero-overhead, không tốn tài nguyên quản lý cụm, không phát sinh chi phí hạ tầng hay network overhead như Spark.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?**  
   Áp dụng kỹ thuật **Crypto-shredding (Mã hoá có khoá huỷ)** kết hợp **Versioned Retraction**:
   - Khi lưu trữ văn bản người dùng trong snapshot, mã hoá trường văn bản bằng khoá mã hoá riêng của từng khách hàng (`user_encryption_key`). Khi khách hàng kích hoạt "Quyền được xoá dữ liệu" (GDPR Article 17), hệ thống chỉ cần huỷ khoá mã hoá của người đó trong Key Management Service (KMS). Toàn bộ văn bản của T-97 trong các snapshot lịch sử lập tức trở thành dữ liệu rác không thể giải mã, bảo toàn tính bất biến cấu trúc snapshot mà vẫn tuân thủ triệt để GDPR.
   - Nếu mô hình đã huấn luyện trên văn bản cũ, đánh dấu snapshot cũ là `deprecated`, tạo snapshot mới loại bỏ T-97 và kích hoạt pipeline retrain mô hình sạch (Machine Unlearning).

2. **Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?**  
   - **Chốt PII:** Xây dựng chốt chặn lai (Hybrid Guardrail): Regex cho các mẫu có cấu trúc định sẵn (email, phone, CCCD 12 số, số thẻ ngân hàng) kết hợp với mô hình **Named Entity Recognition (NER)** tiếng Việt (như PhoBERT-NER hoặc Microsoft Presidio) để nhận diện thực thể tên riêng người (PER), địa chỉ (LOC).
   - **Tầng áp dụng:** 
     - *Bronze ➔ Silver:* Regex chạy tốc độ cao ngay khi ingest để loại bỏ các PII chuẩn.
     - *Silver ➔ Gold / RAG / Training Set:* Chạy mô hình NER sâu trên các trường văn bản tự do (`subject`, `body`, transcripts) trước khi chunk hoặc đưa vào tập train. Các dòng có xác suất PII cao được đưa vào `quarantine_pii` để kiểm duyệt thủ công.
   - **Đo lường:** Dựng tập kiểm thử vàng (Golden Evaluation Set) gồm các cuộc hội thoại được chuyên gia gán nhãn PII thủ công; đo lường thường xuyên bằng chỉ số **Recall** (ưu tiên tối đa Recall > 99.5% để không bỏ sót PII) và **Precision** (để tránh che nhầm từ ngữ thông thường).

## 5. Output (dán nguyên văn)

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 3.28s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
cd dbt_project && DBT_PROFILES_DIR=. /home/tu/VinLab/K4-Track02-Day17-TranTuanTu-2A202602840-DataPipelineEngineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test

Concurrency: 1 threads (target='dev')

1 of 19 START sql view model main.stg_events ................................... [RUN]
1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.11s]
2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.04s]
3 of 19 START sql incremental model main.silver_events ......................... [RUN]
3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.16s]
4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.21s]
8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.19s]
5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.05s]
6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
7 of 19 START test unique_silver_events_event_id ............................... [RUN]
7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.04s]
9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.09s]
Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.04s]
Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.04s]
16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.39s]
17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.04s]
18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.03s]
19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]

Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.80 seconds (1.80s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bằng chứng Bonus

- **Bonus B1 (LLM Step with Hash Cache + Validation):**
```text
$ make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

- **Bonus B2 (Real-world Architecture Design):**
  - Tài liệu chi tiết: [`bonus/DESIGN.md`](../bonus/DESIGN.md) (1100+ từ, giải quyết bài toán Omnichannel AI Customer Support Pipeline tại thị trường Việt Nam theo Nghị định 13/2023/NĐ-CP, 5 quyết định kiến trúc then chốt kèm đánh đổi, 1 phương án bị loại bỏ và sơ đồ hệ thống).
