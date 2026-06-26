# Design Spec: Gold Data Pipeline Master Prompt
Date: 2026-06-26
Topic: Gold Data Pipeline Prompt Optimization

## Objective
This document archives the optimized Master Prompt designed to prompt an AI agent to architect a professional-grade gold data pipeline (XAU/USD) with a Python & PostgreSQL stack.

## The Master Prompt

```markdown
# Role (Vai trò)
Bạn là một Senior Financial Data Engineer kiêm Data Scientist hàng đầu, có chuyên môn sâu trong việc xây dựng hệ thống dữ liệu (Data Pipelines) và mô hình hóa dữ liệu tài chính (đặc biệt là cặp tỷ giá XAU/USD - Vàng/Đô la Mỹ).

# Context (Ngữ cảnh)
Tôi đang thực hiện một dự án cá nhân để học hỏi toàn diện về quy trình kỹ thuật dữ liệu (Data Engineering) và khoa học dữ liệu (Data Science). Dự án tập trung vào việc thu thập, xử lý và trực quan hóa dữ liệu vàng (XAU/USD) kết hợp với các chỉ số kinh tế vĩ mô và tin tức tài chính để chuẩn bị dữ liệu cho các mô hình AI/ML dự báo xu hướng giá vàng.
Yêu cầu của tôi là xây dựng một hệ thống bài bản, chuẩn hóa theo cách các doanh nghiệp tài chính/công nghệ lớn đang vận hành, đồng thời phải trực quan và dễ hiểu để phục vụ mục đích học tập.

# Tech Stack (Bộ công nghệ sử dụng)
- Ngôn ngữ: Python
- Thu thập dữ liệu: BeautifulSoup, Scrapy, hoặc các thư viện gọi API (như `yfinance`, `requests` kết hợp FRED API cho chỉ số vĩ mô).
- Tiền xử lý & Phân tích: Pandas, NumPy.
- Lưu trữ: PostgreSQL (sử dụng thư viện `psycopg2` hoặc `SQLAlchemy` trong Python để kết nối).
- Trực quan hóa: Matplotlib, Seaborn hoặc Plotly.

# Tasks (Nhiệm vụ chi tiết)
Hãy thiết kế cho tôi một kiến trúc Pipeline hoàn chỉnh gồm 4 giai đoạn cốt lõi sau:

## Giai đoạn 1: Data Ingestion (Thu thập dữ liệu)
- Đề xuất phương pháp cào dữ liệu tin tức tài chính (ví dụ từ Reuters, Bloomberg hoặc ForexFactory) và cách gọi API để lấy dữ liệu giá vàng lịch sử (Yahoo Finance, Alpha Vantage) & chỉ số vĩ mô (DXY, lợi suất trái phiếu chính phủ Mỹ US10Y, chỉ số lạm phát CPI từ FRED).
- Cách xử lý giới hạn lượt gọi (Rate Limiting) và cơ chế tự động thử lại (Retry Mechanism) khi gặp lỗi kết nối.

## Giai đoạn 2: Data Preprocessing & Storage (Tiền xử lý & Lưu trữ)
- Giải quyết bài toán lệch tần suất (Data Alignment): Cách khớp dữ liệu vĩ mô (công bố theo tháng/quý) và tin tức (không định kỳ) với dữ liệu giá vàng (theo ngày/giờ).
- Xử lý các giá trị thiếu (Missing values), dữ liệu ngoại lai (Outliers) và các ngày nghỉ giao dịch (Cuối tuần, Lễ Tết).
- Thiết kế Schema cơ sở dữ liệu quan hệ tối ưu trên PostgreSQL để lưu trữ và truy vấn nhanh dữ liệu chuỗi thời gian (Time-series data).

## Giai đoạn 3: Feature Engineering (Trích xuất đặc trưng)
- Trích xuất các đặc trưng kỹ thuật (Technical Indicators): RSI, MACD, Bollinger Bands, Moving Averages (EMA/SMA).
- Trích xuất đặc trưng vĩ mô & Lagged Features (các giá trị trễ của giá để mô hình học chuỗi thời gian).
- Hướng dẫn xử lý dữ liệu văn bản tin tức thành điểm số cảm xúc (Sentiment Scores) đơn giản.

## Giai đoạn 4: Model-Ready Data Packaging (Đóng gói dữ liệu)
- Cách thực hiện Time-Series Train-Test Split chuẩn để tránh rò rỉ dữ liệu (Data Leakage / Look-ahead Bias).
- Chuẩn hóa dữ liệu (Scaling/Normalization) phù hợp cho các mô hình khác nhau (LSTM, XGBoost, Random Forest).
- Thiết kế đầu ra của pipeline dưới dạng một Dataset generator sẵn sàng đưa vào các mô hình học máy.

# Output Format (Định dạng đầu ra mong muốn)
1. **Sơ đồ kiến trúc (System Architecture)** bằng Mermaid.js mô tả luồng dữ liệu từ nguồn đến khi đóng gói.
2. **Thiết kế chi tiết từng giai đoạn** kèm theo giải thích lý do lựa chọn giải pháp đó (Best Practices).
3. **Mã nguồn Python mẫu (Skeletal Code / Boilerplate)** cho từng giai đoạn, sử dụng các thư viện chuẩn đã nêu ở phần Tech Stack, đảm bảo sạch sẽ và có chú thích rõ ràng.
4. **Quy trình kiểm thử chất lượng dữ liệu (Data Quality Checks)** đơn giản để đảm bảo không có lỗi logic trước khi đưa vào mô hình.

# Constraints (Ràng buộc)
- Không bịa đặt thông tin, không sử dụng các thư viện hoặc API đã lỗi thời hoặc không tồn tại.
- Các giải pháp đưa ra phải nhấn mạnh vào việc tránh "rò rỉ dữ liệu" (Data Leakage) - lỗi phổ biến nhất trong dự báo tài chính chuỗi thời gian.
- Tài liệu hóa chi tiết, dễ hiểu cho người mới bắt đầu học AI.
```
