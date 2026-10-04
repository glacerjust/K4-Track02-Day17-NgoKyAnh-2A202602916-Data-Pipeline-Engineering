# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Ngô Kỳ Anh / 2A202602916
**Repo:** https://github.com/glacerjust/K4-Track02-Day17-NgoKyAnh-2A202602916-Data-Pipeline-Engineering
**Commit bài nộp:** b382f7df653e8f1da6d482c5cbbce0548841a593
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity IDE (Gemini 3.8 Flash) hỗ trợ phân tích nguyên nhân gốc của 3 lỗi dựa trên hợp đồng dữ liệu, hướng dẫn cú pháp SQL MERGE / LSN guard, cấu hình LOOKBACK_DAYS, cài đặt cache LLM và đối soát checklist nộp bài.
**Nguồn tham khảo khác (nếu có):** Slide bài giảng K4 Track 02 Day 17 (Data Pipeline Engineering).

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `scripts.verify` báo `silver_tickets has exactly one row per ticket_id` (24 rows for 12 tickets); test `test_silver_tickets_one_row_per_ticket` FAIL; rerun làm nhân đôi/ghi đè sai dòng. | `scripts.verify` báo `LOOKBACK_DAYS covers measured P99 lateness (LOOKBACK_DAYS=0 < 3)` và `gold_feature_daily` lệch full recompute; event ngày 08-12 của `u05` bị mất trên 08-12. | `scripts.verify` báo `deleted ticket T-97 is a tombstone` (got []), `latest training snapshot excludes the deleted ticket T-97` (1 row), `deletes propagate to the RAG index: 2 chunks`. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` trong `pipeline/silver.py` dùng `INSERT INTO` thẳng vào bảng thay vì MERGE theo khoá, không kiểm tra LSN nên batch cũ ghi đè trạng thái mới hơn. | `LOOKBACK_DAYS = 0` trong `pipeline/config.py` giả định dữ liệu đến tức thời trong ngày; mỗi daily run chỉ tính ngày hiện tại nên bỏ sót sự kiện đến trễ (late-arriving data). | Trong `pipeline/staging.py` (`ticket_changes_sql`), `ticket_id` chỉ lấy từ `after`. Với Debezium delete (`op = 'd'`), `after = null` khiến `ticket_id` bị NULL và rớt điều kiện lọc. |
| **Cách sửa** (file, vài dòng) | Sửa [`pipeline/silver.py`](../pipeline/silver.py): dùng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | Chạy đo P99 lateness từ Bronze qua `event_lateness_sql()` ra 3 ngày; sửa [`pipeline/config.py`](../pipeline/config.py): cập nhật `LOOKBACK_DAYS = 3`. | Sửa [`pipeline/staging.py`](../pipeline/staging.py): dùng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id', j->'key'->>'ticket_id') AS ticket_id`. |
| **Khái niệm trên slide** | Silver — có khoá; MERGE trên khoá thực thể; LSN guard (Log Sequence Number) bảo vệ idempotent chống out-of-order execution. | Xử lý dữ liệu đến muộn (Late data) theo event time; Lookback window = $\lceil\text{P99}\rceil$ đo từ Bronze ("Measure, don't guess"). | Đọc CDC log-based Debezium (phong bì before/after); Tombstone trong Silver (xoá PII, giữ khoá); "Xoá phải lan" (Propagate deletes) xuống Gold. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể có vòng đời thay đổi liên tục theo dòng sự kiện CDC cần cập nhật theo khoá và bảo vệ bằng LSN, trong khi `gold_feature_daily` là bảng tổng hợp aggregate theo phân vùng ngày event_time nên overwrite-partition giúp tính toán lại cửa sổ lookback đơn giản, triệt để và hoàn toàn idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: Tombstone giữ lại ID và cờ `is_deleted = true` cùng LSN để làm bằng chứng kiểm toán (audit trail), ngăn chặn các thông điệp đến muộn vô tình chèn lại bản ghi đã bị xoá, đồng thời xoá sạch dữ liệu PII để tuân thủ quyền riêng tư (GDPR).
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính tái lập (reproducibility) tuyệt đối cho các mô hình ML đã train trong quá khứ để debug/audit, tránh rò rỉ dữ liệu tương lai (data leakage) vào dữ liệu huấn luyện lịch sử.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu ở quy mô hàng nghìn/triệu dòng chạy local in-process trên DuckDB cho tốc độ vượt trội (zero-overhead, không tốn tài nguyên JVM hay phân tán mạng như Spark), đồng thời dbt cung cấp chuẩn quản lý metadata, lineage, contract test và microbatch chuyên nghiệp.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?**  
   Trong thực tế sản xuất và tuân thủ pháp lý (GDPR/CCPA "Right to be Forgotten"), quyền riêng tư của cá nhân luôn có mức ưu tiên cao hơn tính toàn vẹn kỹ thuật. Hướng xử lý:
   - *Ở tầng lưu trữ dữ liệu (Data Storage):* Áp dụng kỹ thuật *Cryptographic Erasure* (mỗi người dùng/ticket được mã hoá văn bản bằng một khoá riêng, khi có yêu cầu xoá chỉ cần huỷ khoá giải mã là toàn bộ dữ liệu lịch sử trong snapshot cũ trở thành chuỗi byte ngẫu nhiên vô nghĩa mà không cần sửa cấu trúc file) hoặc thực hiện *Targeted Snapshot Scrubbing* (ghi đè có kiểm soát trường PII/text thành `NULL`/`<DELETED>` trong các snapshot cũ, tăng minor version snapshot kèm biên bản audit log tuân thủ pháp luật).
   - *Ở tầng mô hình (Model Retraining/Unlearning):* Ghi nhận danh sách checkpoint mô hình đã huấn luyện trên snapshot có chứa T-97. Nếu dữ liệu có tính chất nhạy cảm cao, kích hoạt pipeline Machine Unlearning hoặc tái huấn luyện (retrain) mô hình trên snapshot đã làm sạch.

2. **Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?**  
   - *Vị trí chốt chặn:* Đặt ngay tại ranh giới chuyển tiếp từ Bronze sang Silver (trong bước Data Ingestion Quality Gate / Staging transform), ngăn tuyệt đối PII dạng định danh cá nhân lọt vào Silver và Gold.
   - *Công cụ / Kỹ thuật:* Sử dụng mô hình nhận diện thực thể tên riêng (Named Entity Recognition - NER) tiếng Việt (như PhoBERT-NER hoặc spaCy) để bắt các thực thể `PER` (Person), kết hợp với bảng tra cứu định danh khách hàng (Customer Identity Lookup Table ánh xạ `user_id` $\rightarrow$ họ tên đã đăng ký để thay thế triệt để thành `<NAME>`).
   - *Cách đo lường:*
     - *Đo lường ngoại tuyến (Offline Evaluation):* Xây dựng tập test đánh giá gán nhãn thủ công (Golden Test Set) với độ đa dạng cao về tên tiếng Việt, đo lường độ chính xác (Precision), độ bao phủ (Recall) và $F_1$-score; ưu tiên tối đa Recall ($\ge 99.5\%$) để giảm thiểu False Negative (bỏ sót PII).
     - *Giám sát trực tuyến (Online Quality Check):* Thiết lập automated test định kỳ quét ngẫu nhiên các cột text trên Silver/Gold bằng mô hình LLM Auditor / Shannon entropy test; nếu tỷ lệ nghi ngờ vượt quá ngưỡng 0% thì kích hoạt cảnh báo và tự động cô lập bản ghi vào bảng quarantine.

## 5. Output (dán nguyên văn)

### `$ make verify`
*(Tương đương PowerShell: `.\.venv\Scripts\python.exe -m scripts.verify`)*
```text
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
```

### `$ make test`
*(Tương đương PowerShell: `.\.venv\Scripts\python.exe -m pytest`)*
```text
============================= test session starts =============================
platform win32 -- Python 3.12.7, pytest-8.4.2, pluggy-1.6.0
rootdir: C:\Users\Admin\Desktop\K4-Track02-Day17-NgoKyAnh-2A202602916-Data-Pipeline-Engineering
configfile: pytest.ini
collected 34 items

tests\test_contracts.py ............                                     [35%]
tests\test_extensions.py .........                                       [61%]
tests\test_rerun.py .                                                    [64%]
tests\test_units.py ............                                         [100%]

============================= 34 passed in 3.23s ==============================
```

### `$ make rerun3`
*(Tương đương PowerShell: `.\.venv\Scripts\python.exe -m scripts.rerun_check`)*
```text
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

### `$ make lateness`
*(Tương đương PowerShell: `.\.venv\Scripts\python.exe main.py --lateness`)*
```text
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

### `$ make dbt`
*(Tương đương PowerShell: `.\.venv\Scripts\python.exe main.py --land-only; $env:DO_NOT_TRACK = '1'; Push-Location dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17; Pop-Location`)*
```text
05:40:11  Running with dbt=1.12.5
05:40:12  Registered adapter: duckdb=1.11.0
05:40:12  Unable to do partial parsing because saved manifest not found. Starting full parse.
05:40:16  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:40:16  
05:40:16  Concurrency: 1 threads (target='dev')
05:40:16  
05:40:21  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:40:21  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.17s]
05:40:21  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:40:21  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
05:40:21  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:40:21  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.12s]
05:40:21  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:40:22  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.16s]
05:40:22  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:40:22  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.12s]
05:40:22  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:40:22  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
05:40:22  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:40:22  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
05:40:22  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:40:22  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
05:40:22  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:40:22  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
05:40:22  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:40:22  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
05:40:22  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:40:22  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
05:40:22  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:40:22  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
05:40:22  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:40:22  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
05:40:22  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:40:22  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
05:40:22  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:40:22  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.03s]
05:40:22  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:40:22  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:22  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.06s]
05:40:22  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:22  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:22  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:22  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:22  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:40:22  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
05:40:22  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.29s]
05:40:22  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:40:22  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
05:40:22  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:40:22  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
05:40:22  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:40:22  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
05:40:22  
05:40:22  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 6.23 seconds (6.23s).
05:40:22  
05:40:22  Completed successfully
05:40:22  
05:40:22  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

### `$ make parity`
*(Tương đương PowerShell: `.\.venv\Scripts\python.exe -m scripts.parity`)*
```text
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bonus B1 Output: `$ python -m scripts.bonus_llm`
```text
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
