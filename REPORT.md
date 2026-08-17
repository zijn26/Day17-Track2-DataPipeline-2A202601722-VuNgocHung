# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Vũ Ngọc Hùng  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 23.3s
  run 2/3 … 23.9s
  run 3/3 … 23.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     43d3a08aa2  43d3a08aa2  43d3a08aa2   ✓
  gold_feature_daily    f36bfb1049  f36bfb1049  f36bfb1049   ✓
  gold_doc_chunks       92d8e50131  92d8e50131  92d8e50131   ✓
  quarantine_tickets    4054a856bf  4054a856bf  4054a856bf   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ catchup=False / max_active_runs=1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt + 5 điểm thưởng Bài A (105 / 100 điểm)**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` bị trùng lặp dòng, số hàng vọt từ 12,480 lên 38,750 sau 3 lần chạy lại pipeline (thừa 26,270 hàng). |
| **Nguyên nhân** | Model `gold_training_set` khai báo `materialized = 'incremental'` nhưng không khai báo `unique_key`. dbt mặc định dùng câu lệnh `INSERT INTO` (append); khi chạy lại cùng một partition ngày cũ, các bản ghi bị ghi nối tiếp vào đuôi bảng thay vì ghi đè. |
| **Cách khắc phục** | 1. Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()` của file `dbt/models/gold/gold_training_set.sql`.<br>2. Cấu hình `catchup = False` và `max_active_runs = 1` trong file `dags/ai_training_pipeline.py`. |
| **Bằng chứng** | trước: 38,750 hàng · sau: 12,480 hàng · checksum 3 lượt: `43d3a08aa2` (tuyệt đối ổn định) |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu 455 hàng ở các ngày quá khứ (chỉ đạt 8,645 / 9,100 hàng). |
| **P99 độ trễ đo được** | **2.73 ngày** (Max độ trễ: **2.95 ngày**) |
| **Lookback đã chọn** | **3 ngày** (`interval 3 day`) — vì độ trễ P99 và Max của dữ liệu sự kiện rơi vào khoảng 2.73 - 2.95 ngày, việc lùi 3 ngày đảm bảo bắt trọn vẹn 100% dữ liệu về muộn (late-arriving data). |
| **Nguyên nhân** | Mệnh đề lọc cũ `where event_date > (select max(event_date) from {{ this }})` chỉ lấy các event có ngày lớn hơn ngày lớn nhất trong bảng đích. Khi dữ liệu sự kiện bị trễ 1–3 ngày mới tới warehouse, chúng bị điều kiện này bỏ sót hoàn toàn. |
| **Cách khắc phục** | 1. Đổi mệnh đề lọc thành `where event_date >= (select max(event_date) - interval 3 day from {{ this }})` trong `dbt/models/gold/gold_feature_daily.sql`.<br>2. Bổ sung `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` vào `config()`. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (checksum 3 lượt: `f36bfb1049`) |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 đại diện cho 99% phân bố dữ liệu thực tế, giúp loại bỏ các giá trị ngoại lệ (outliers) rải rác quá xa. Chi phí khi chọn `max` quá lớn hoặc lùi vô thời hạn là công quét (`rows scanned`) và chi phí tính toán lại (`compute cost`) ở MỌI lượt chạy sau này sẽ tăng vọt. Chọn P99 (~3 ngày) là sự cân bằng tối ưu giữa tính chính xác dữ liệu (99–100%) và tài nguyên tính toán.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Bảng `silver_tickets` bị 6,606 hàng sai định dạng priority (`priority ∉ 1..4`), bảng `quarantine_tickets` có 0 / 312 hàng rác. |
| **Nguyên nhân** | Kể từ ngày 2026-08-10, team backend đổi cách ghi dữ liệu CDC từ số (`1..4`) sang nhãn chuỗi (`urgent`, `high`, `medium`, `low`), đồng thời nguồn CDC còn bị lẫn các giá trị hỏng thật (`P1`, `unknown`, `0`, `5`, `-1`, `''`, `NULL`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. Nhóm 1 (`'1'`, `'2'`, `'3'`, `'4'`): Giữ nguyên, ép kiểu integer.<br>2. Nhóm 2 (`'urgent'`, `'high'`, `'medium'`, `'low'`): Map tương ứng sang 1, 2, 3, 4.<br>3. Nhóm 3 (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL`): Trả về `NULL` và đẩy sang `quarantine_tickets`. |
| **Cách khắc phục** | 1. Sửa macro `dbt/macros/normalize_priority.sql` dùng khối `CASE WHEN` phân loại 3 nhóm.<br>2. Sửa `dbt/models/silver/silver_tickets.sql`: Lọc loại bỏ bản ghi `NULL` TRƯỚC, sau đó mới xếp hạng `ROW_NUMBER()`.<br>3. Sửa `dbt/models/silver/quarantine_tickets.sql`: Đổi điều kiện `where {{ normalize_priority('priority_raw') }} is null`.<br>4. Sửa `dbt/models/silver/schema.yml`: Đổi `enforced: true` và mở comment test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (checksum: `4054a856bf`) · `dbt test` 11/11 pass (sạch 100%) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> Nên làm sạch và chặn bản ghi lỗi ở tầng Silver. Tầng Bronze phải đóng vai trò là "Nhật ký bất biến" (Immutable Log) lưu trữ chính xác dữ liệu gốc từ nguồn để phục vụ audit/điều tra sự cố. Không nên để pipeline dừng lại vì vài trăm bản ghi hỏng không được phép chặn 130.000 event và 31.200 tài liệu hợp lệ khác đến tay người dùng; các bản ghi hỏng được đẩy vào `quarantine_tickets` để đội vận hành xử lý sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Bài A — Query dashboard chậm *(+5 điểm)* |
| **Nguyên nhân** | Dataset gốc `data/gold_events/` gồm 5.000 file Parquet rất nhỏ (small-file problem), không được phân vùng (partition) và không sắp xếp. Ngoài ra, điều kiện lọc cũ `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột trong hàm làm vô hiệu hóa khả năng dùng Hive Partitioning và min/max stats pruning. Engine buộc phải mở và quét qua tất cả 5.000 file (5.000.000 rows scanned) tốn 38 giây. |
| **Cách khắc phục** | 1. Viết `tools/compact.py` dùng `COPY TO` gom 5.000 file nhỏ thành 14 file lớn theo `PARTITION_BY (event_date)`, sắp xếp `ORDER BY customer_name, event_time` và `row_group_size 1000`.<br>2. Sửa `queries/dashboard.sql`: Trỏ vào `data/gold_events_v2/**/*.parquet`, bật `hive_partitioning=true` và đổi lọc thành `event_date = '2026-08-09'`. |
| **Bằng chứng** | `rows scanned`: 5,000,000 → 9,324 (giảm **536.3×**) · `files`: 5,000 → 14 · `result hash`: `4379e4c5d9f3` (giữ nguyên 100%) · Thời gian chạy: 38s → 29ms |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra cấu hình `incremental` model (đã có `unique_key` và `incremental_strategy = 'merge'` chưa) để tránh lặp dữ liệu khi re-run. |
| 2 | Kiểm tra phân bố độ trễ dữ liệu đầu vào (P99 latency) và độ rộng của cửa sổ nhìn lùi (lookback window) để không bị mất dữ liệu về muộn. |
| 3 | Kiểm tra Data Contracts, Data Quality Tests và cơ chế Quarantine cách ly dữ liệu hỏng để bảo vệ kho dữ liệu chính. |
