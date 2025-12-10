```text
██████╗ ███████╗███╗   ██╗ ██████╗██╗  ██╗███╗   ███╗ █████╗ ██████╗ ██╗  ██╗
██╔══██╗██╔════╝████╗  ██║██╔════╝██║  ██║████╗ ████║██╔══██╗██╔══██╗██║ ██╔╝
██████╔╝█████╗  ██╔██╗ ██║██║     ███████║██╔████╔██║███████║██████╔╝█████╔╝
██╔══██╗██╔════╝██║╚██╗██║██║     ██╔══██║██║╚██╔╝██║██╔══██║██╔══██╗██╔═██╗
██████╔╝███████╗██║ ╚████║╚██████╗██║  ██║██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██╗
╚═════╝ ╚══════╝╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝
=============================================================================
      🚀 PERFORMANCE TESTING & DATABASE UTILITY TOOLKIT GUIDE 🚀
=============================================================================
```

# 📘 Hướng Dẫn Sử Dụng Bộ Benchmark

Chào mừng bạn đến với bộ công cụ kiểm thử hiệu năng dành cho Case Study so sánh **MySQL vs PostgreSQL vs MongoDB**. Tài liệu này sẽ hướng dẫn bạn cách sử dụng các script để đo lường, phân tích và thao tác dữ liệu.

---

## ⚙️ 1. Cấu Hình Môi Trường (.env)

Trước khi chạy bất kỳ lệnh nào, hãy đảm bảo bạn đã trỏ `DB_CONNECTION` vào đúng database muốn thao tác trong file `.env`.

```ini
# --- CHỌN MÔI TRƯỜNG TEST ---

# 1. Để test MongoDB:
DB_CONNECTION=mongodb
DATABASE_URL=mongodb://localhost:27017/cnnt_casestudy

# 2. Để test MySQL:
# DB_CONNECTION=mysql
# DATABASE_URL=mysql://root:root@localhost:3306/cnnt_casestudy

# 3. Để test PostgreSQL:
# DB_CONNECTION=postgresql
# DATABASE_URL=postgresql://postgres:password@localhost:5432/cnnt_casestudy
```

---

## 🛠️ 2. Công Cụ Tiện Ích (Utility Tools)

Nhóm công cụ này giúp bạn thao tác nhanh với database từ dòng lệnh (CLI) mà không cần mở GUI.

| Lệnh NPM                    | Tên Script               | Chức năng & Cách dùng                                                                             |
| :-------------------------- | :----------------------- | :------------------------------------------------------------------------------------------------ |
| **`npm run tool:sql`**      | `exec-raw-sql-cli.js`    | **Chạy SQL Query (MySQL/Postgres)**<br>👉 `npm run tool:sql "SELECT * FROM users LIMIT 5"`        |
| **`npm run tool:mongo`**    | `helper-mongo-native.js` | **Chạy MongoDB Aggregation**<br>👉 `npm run tool:mongo orders '[{"$count": "total"}]'`            |
| **`npm run seed:50k`**      | `seed-50k-users.js`      | **Bơm 50.000 User giả**<br>Dùng để tạo tải lớn trước khi benchmark.<br>👉 `npm run seed:50k`      |
| **`npm run fix-sequences`** | `fix-sequences.js`       | **Sửa lỗi ID**<br>Đồng bộ lại Auto Increment ID (SQL) sau khi seed.<br>👉 `npm run fix-sequences` |

---

## 📊 3. Bộ Kiểm Thử Hiệu Năng (Benchmarks)

Nhóm script này dùng để đo đạc và so sánh tốc độ xử lý.

### 🏎️ A. So Sánh Tổng Thể (Cross-DB)

- **Lệnh:** `npm run bench:cross-db`
- **File:** `tests/benchmark-cross-db-performance.js`
- **Mục đích:** Chạy một loạt các bài test tiêu chuẩn trên cả 3 DB để xem ai vô địch.
- **Các bài test:**
  1. `Count (100k)`: Đếm số lượng bản ghi lớn.
  2. `Sum ($)`: Tính tổng tiền (Aggregation).
  3. `Search (JSON)`: Tìm kiếm text trong JSON.
  4. `Join`: Kết nối bảng Users và Orders.

### 📦 B. JSON vs Relational (Architecture)

- **Lệnh:** `npm run bench:json-rel`
- **File:** `tests/benchmark-json-vs-relational.js`
- **Mục đích:** Trả lời câu hỏi _"Nên lưu Items dạng JSON hay tách bảng con?"_
- **Chỉ số:** So sánh tốc độ Read, Write và dung lượng ổ cứng (Storage Size).

### ⚡ C. Native vs Adapter (Optimization)

- **Lệnh:** `npm run demo:native`
- **File:** `tests/demo-native-speed-gain.js`
- **Mục đích:** Chứng minh sự chênh lệch hiệu năng giữa việc dùng thư viện (Adapter/ORM) so với truy vấn thuần (Native Query).

### 🔬 D. Phân Tích Sâu (Deep Dive)

- **Lệnh:** `npm run analyze:adapter`
- **File:** `tests/analysis-adapter-vs-native.js`
- **Mục đích:** Phân tích các rủi ro kỹ thuật như _Race Condition_, _Memory Leak_ khi xử lý dữ liệu lớn bằng logic ứng dụng thay vì Database Engine.

---

## 📝 4. Kịch Bản Test Mẫu (Workflow)

Để thực hiện bài nghiên cứu hoàn chỉnh, bạn có thể đi theo lộ trình sau:

### 🔹 Bước 1: Chuẩn bị dữ liệu

```bash
# Bơm dữ liệu lớn vào MongoDB
# (Sửa .env -> DB_CONNECTION=mongodb)
npm run seed:50k

# Bơm dữ liệu lớn vào MySQL
# (Sửa .env -> DB_CONNECTION=mysql)
npm run seed:50k
```

### 🔹 Bước 2: Chạy Benchmark so sánh

```bash
# Chạy bảng so sánh tổng quát
npm run bench:cross-db
```

_Ghi lại kết quả bảng console hiện ra._

### 🔹 Bước 3: Kiểm chứng Query cụ thể

Nếu thấy MySQL chậm ở phần tìm kiếm, hãy thử chạy query tay để debug:

```bash
npm run tool:sql "SELECT COUNT(*) FROM orders WHERE JSON_SEARCH(items, 'one', '%Phở%') IS NOT NULL"
```

---

> **Lưu ý:** Kết quả benchmark phụ thuộc vào cấu hình phần cứng (CPU/RAM/Disk) của máy bạn. Hãy tắt các ứng dụng nặng khác khi chạy để có kết quả chính xác nhất.
