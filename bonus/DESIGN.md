# K4-Track02-Day17 — Bonus B2: Thiết kế Kiến trúc Pipeline Dữ liệu Thực tế

## 1. Bài toán & Ràng buộc Thực tế (Context & Constraints)

### Bài toán
Thiết kế hệ thống **Data Pipeline & Feature Store phục vụ Nền tảng AI Hỗ trợ Khách hàng Đa kênh (Omnichannel AI Customer Support)** cho một doanh nghiệp bán lẻ & thương mại điện tử tại Việt Nam. Hệ thống phục vụ 3 mục tiêu cốt lõi:
1. **Agent Hỗ trợ Real-time (RAG & Routing Agent):** Tự động phân loại intent, định tuyến ticket cho đúng nhân viên và gợi ý câu trả lời từ kho tri thức chính sách/sản phẩm.
2. **Offline ML Classifier & Scoring:** Dự báo khách hàng rời bỏ (Churn prediction) và chấm điểm mức độ hài lòng (CSAT scoring).
3. **Data Flywheel:** Thu thập log tương tác thực tế từ tổng đài viên và khách hàng để tạo tập dữ liệu DPO (Direct Preference Optimization) và fine-tune LLM định kỳ.

### Ràng buộc thực tế
- **Nguồn dữ liệu đa dạng & phân tán:**
  - Webhook tin nhắn từ **Zalo Official Account (OA)** và **Facebook Fanpage** (tốc độ cao, nhiều teencode, lỗi mạng cục bộ).
  - CDC (Debezium qua Kafka) từ **PostgreSQL Core Ticket System**.
  - File ghi âm và transcripts sinh ra từ tổng đài **VoIP (FreeSWITCH/Asterisk)** được đẩy theo lô mỗi 15 phút lên MinIO/S3.
- **Bảo vệ dữ liệu cá nhân theo Nghị định 13/2023/NĐ-CP (Việt Nam):**
  - Khách hàng thường gửi số CCCD (12 số), số tài khoản ngân hàng, địa chỉ giao hàng và số điện thoại di động trong chat. Phải đảm bảo PII bị che (masked) triệt để trước khi đi vào kho vector RAG hoặc tập train của LLM.
- **Ràng buộc ngân sách & hạ tầng:** Doanh nghiệp vừa và nhỏ (SMB), hạ tầng chạy on-premise hoặc hybrid cloud tiết kiệm, không chấp nhận chi phí đắt đỏ của các SaaS vector DB hay cụm streaming khổng lồ (như Databricks/Pinecone).

---

## 2. Các Quyết định Kiến trúc Then chốt (Key Architectural Decisions & Trade-offs)

### Quyết định 1: Lựa chọn Mô hình Xử lý — Microbatching (5 phút) thay vì Full Streaming (Kappa Architecture)
- **Phương án lựa chọn:** Kiến trúc Microbatching định kỳ 5 phút kết hợp Parquet Lakehouse và DuckDB/dbt.
- **Đánh đổi (Trade-off):**
  - *Full Streaming (Flink + Kafka + Pinot):* Độ trễ chỉ tính bằng mili-giây nhưng chi phí hạ tầng máy chủ lớn, yêu cầu đội ngũ vận hành 24/7 chuyên sâu, và đặc biệt là cực kỳ khó khăn khi cần **Backfill / Replay** lại lịch sử 6 tháng khi logic prompt hoặc thuật toán trích xuất entity thay đổi.
  - *Microbatching (5 phút):* Độ trễ 5 phút là hoàn toàn chấp nhận được với bài toán CSKH (thời gian phản hồi ticket SLA tiêu chuẩn là 15-30 phút). Điểm mạnh vượt trội là khả năng **tái lập kết quả (reproducibility)**, chạy lại dữ liệu idempotent, kiểm tra checksum đơn giản và chi phí phần cứng rẻ hơn gấp 5 lần.
- **Lý do chọn:** Với bài toán CSKH, tính nhất quán (consistency) và khả năng backfill an toàn quan trọng hơn độ trễ sub-second.

### Quyết định 2: Chiến lược Quản lý Lateness & Train/Serve Parity — Lookback Window = ceil(P99)
- **Phương án lựa chọn:** Áp dụng mô hình Overwrite-partition với cửa sổ trễ Lookback Window được đo lường tự động từ Bronze log.
- **Đánh đổi:**
  - Nếu chỉ tính theo *Ingestion Time* (thời điểm tin nhắn tới server): Dữ liệu phân tích báo cáo sẽ bị méo mó nghiêm trọng khi có sự cố nghẽn mạng 4G/WiFi của khách hàng (như khách trên tàu/máy bay, gửi tin lúc 10h tối nhưng 8h sáng hôm sau tin nhắn mới đồng bộ tới Zalo webhook).
  - Bằng cách đo đạc P99 lateness thực tế từ Bronze ($t_{ingested} - t_{event}$), hệ thống đặt Lookback Window = 3 ngày. Mỗi lần batch chạy, nó recompute lại các partition trong khoảng `[day - 3, day]`.
- **Lý do chọn:** Đảm bảo triệt để **Train/Serve Parity**; mô hình khi huấn luyện và khi chạy thực tế đều nhìn thấy cùng một phân phối đặc trưng theo đúng *Event Time*.

### Quyết định 3: Xử lý CDC Deletes và Quyền được lãng quên (GDPR / Nghị định 13) — Soft Tombstone ở Silver + Lan truyền xoá xuống Gold
- **Phương án lựa chọn:** Khi nhận Debezium CDC delete (`op = 'd'`), Silver không xóa hẳn dòng (hard delete) mà cập nhật thành **Tombstone** (`is_deleted = true`, gán toàn bộ thông tin cá nhân `user_id, subject, body = NULL`, giữ lại `ticket_id` và `_lsn`). Sau đó, tầng Gold loại bỏ hoàn toàn các ticket này khỏi RAG vector index và các snapshot huấn luyện mới.
- **Đánh đổi:**
  - *Hard Delete:* Nếu xóa đứt dòng trong Silver, khi replay lại các log Kafka cũ, hệ thống sẽ không biết dòng đó đã từng bị xóa và sẽ "hồi sinh" (resurrect) dữ liệu bị xoá nếu gặp một message cũ đến muộn.
  - *Soft Tombstone với LSN Guard:* Giữ lại LSN xóa cao nhất giúp chặn đứng việc hồi sinh dữ liệu khi backfill, đồng thời xóa sạch văn bản PII đáp ứng chuẩn bảo mật.
- **Lý do chọn:** Vừa đảm bảo tính toàn vẹn khi replay dữ liệu, vừa tuân thủ quyền yêu cầu xóa dữ liệu của người dùng.

### Quyết định 4: Chốt chặn PII Đa tầng cho Tiếng Việt — Regex + Hybrid Named Entity Recognition (NER)
- **Phương án lựa chọn:**
  - *Tầng Bronze ➔ Silver:* Áp dụng Regex tốc độ cao để bóc tách và che lập tức các định dạng chuẩn (Email, SĐT Việt Nam +84/09x/08x/07x/03x/05x, số CCCD 12 số, số thẻ ATM).
  - *Tầng Silver ➔ Gold & LLM Flywheel:* Bổ sung mô hình PhoBERT-NER / Presidio Tiếng Việt nhận diện thực thể tên riêng người Việt (Họ + Tên lót + Tên), địa chỉ cụ thể.
  - Dữ liệu nghi vấn rò rỉ PII sẽ không bị drop âm thầm mà được đưa vào bảng `quarantine_pii_review` để nhân viên phụ trách dữ liệu kiểm tra.
- **Đánh đổi:** Chấp nhận tốn thêm ~15ms cho mỗi ticket tại tầng Gold để đảm bảo không một mảnh dữ liệu nhạy cảm nào lọt vào prompt hay vector DB của đối tác thứ ba (OpenAI/Anthropic).

### Quyết định 5: LLM Transformation Caching & Quarantine — Deterministic Content-Hash
- **Phương án lựa chọn:** Mọi bước gọi LLM (tóm tắt cuộc gọi, trích xuất nguyên nhân khiếu nại, gắn tag sản phẩm) bắt buộc phải tuân thủ 4 nguyên tắc:
  1. Cache key = `SHA256(Input_Text + Model_Name + Prompt_Version)`.
  2. Bắt buộc Structured Output (JSON Schema).
  3. Validate schema trước khi vào Gold; câu trả lời lệch schema đẩy vào `llm_quarantine` và trigger fallback model.
  4. Ước tính chi phí token trước mỗi lần chạy batch lớn.
- **Đánh đổi:** Không cho phép gọi LLM trực tiếp trong các câu query ad-hoc; toàn bộ bước enrich phải qua pipeline trung tâm để kiểm soát ngân sách chi phí token và đảm bảo chạy lại 10 lần thì 0 lần phát sinh thêm tiền gọi API nếu dữ liệu không đổi.

---

## 3. Phương án bị loại bỏ (Rejected Alternative)

### Phương án bị loại: "Kiến trúc Lambda với Apache Spark + Pinecone Cloud Vector Database"
- **Mô tả:** Sử dụng Spark Streaming kết hợp Delta Lake và đồng bộ trực tiếp vector lên Pinecone qua managed connector.
- **Lý do kiên quyết loại bỏ:**
  1. **Chi phí vượt trần (Cost Prohibitive):** Pinecone tính phí duy trì pod/index hàng tháng rất đắt đỏ đối với các doanh nghiệp Việt Nam có quy mô ~50k cuộc gọi/ngày nhưng chỉ truy vấn RAG theo giờ hành chính. Cụm Spark yêu cầu tối thiểu 3-5 nodes RAM lớn chỉ để duy trì trạng thái streaming.
  2. **Vấn đề bảo mật dữ liệu xuyên biên giới (Data Sovereignty):** Pinecone không có máy chủ đặt tại Việt Nam, tiềm ẩn rủi ro pháp lý theo Nghị định 13/2023/NĐ-CP khi truyền dữ liệu hội thoại của công dân Việt Nam ra nước ngoài.
  3. **Giải pháp thay thế ưu việt hơn:** Sử dụng DuckDB + dbt cho tính toán xử lý dữ liệu và **Qdrant / pgvector (PostgreSQL)** tự host trên máy chủ on-premise tại Việt Nam. Vừa kiểm soát 100% dữ liệu, vừa tiết kiệm hơn 80% chi phí vận hành hàng tháng.

---

## 4. Sơ đồ Kiến trúc Hệ thống (System Architecture Sketch)

```text
 [ Zalo OA / FB ]     [ VoIP Call Center ]     [ PostgreSQL Core ]
        │                      │                        │
  Webhooks (JSON)       Audio Transcripts          Debezium CDC
        │                      │                        │
        ▼                      ▼                        ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │                     KAFKA EVENT LOG / MINIO                      │
 └──────────────────────────────────────────────────────────────────┘
                                │
                                ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │                       BRONZE LAKE (Parquet)                      │
 │   - Immutable daily dumps, Raw truth (keeps tombstones & dups)   │
 └──────────────────────────────────────────────────────────────────┘
                                │
                      [ Pydantic Quality Gate ]
                      [ Fast Regex PII Masking ]
                                │
                                ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │                       SILVER WAREHOUSE                           │
 │   - silver_tickets (Keyed MERGE on ticket_id, LSN Guard)         │
 │   - silver_events  (Deduped by event_id, Late event time)        │
 │   - silver_transcripts (Merged by exported_at)                   │
 │   - quarantine_events & quarantine_pii                           │
 └──────────────────────────────────────────────────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            │                   │                   │
   [ Lookback Window ]    [ PhoBERT NER ]     [ LLM Transform ]
    (ceil(P99) = 3d)      (Deep PII Mask)     (Hash Cache & Quarantine)
            │                   │                   │
            ▼                   ▼                   ▼
 ┌───────────────────┐ ┌───────────────────┐ ┌─────────────────────┐
 │gold_feature_daily │ │  gold_doc_chunks  │ │  gold_training_set  │
 │  (Routing Agent / │ │(RAG Index Qdrant, │ │(Immutable Snapshots,│
 │  Churn Prediction)│ │  Embed Cache v1)  │ │ Point-in-time state)│
 └───────────────────┘ └───────────────────┘ └─────────────────────┘
                                                      │
                                                      ▼
                                            ┌─────────────────────┐
                                            │    DATA FLYWHEEL    │
                                            │ DPO / SFT LLM Tuning│
                                            └─────────────────────┘
```

---

## 5. Kết luận & Tác động Nghiệp vụ
Thiết kế trên giải quyết trọn vẹn sự cân bằng giữa:
- **Tính chính xác kỹ thuật:** Đảm bảo tính idempotent, train/serve parity, không rò rỉ dữ liệu tương lai.
- **Tuân thủ pháp luật Việt Nam:** Bảo vệ dữ liệu cá nhân theo Nghị định 13/2023/NĐ-CP qua chốt chặn PII đa tầng và cơ chế soft tombstone.
- **Hiệu quả kinh tế:** Tối ưu hóa chi phí vận hành bằng kiến trúc Microbatch trên DuckDB/dbt và tự host vector engine, tiết kiệm hàng nghìn USD chi phí cloud mỗi tháng.
