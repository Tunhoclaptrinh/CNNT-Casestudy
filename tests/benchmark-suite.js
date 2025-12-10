require('dotenv').config();
const colors = require('colors');
const { MongoClient } = require('mongodb');

// Import tất cả Adapter
const mysqlAdapter = require('../utils/MySQLAdapter');
const pgAdapter = require('../utils/PostgreSQLAdapter');
const mongoAdapter = require('../utils/MongoAdapter');

// Cấu hình kết nối Mongo Native (Dùng chung)
const MONGO_URL = process.env.MONGO_URI || process.env.DATABASE_URL; // <--- Cập nhật dòng này

// ============================================================
// HÀM CHẠY QUERY NATIVE (ĐA NĂNG)
// ============================================================
async function runNative(dbType, adapter, action) {
  const start = Date.now();
  try {
    if (dbType === 'mongodb') {
      const client = new MongoClient(MONGO_URL);
      await client.connect();
      const db = client.db();
      const result = await action.mongo(db);
      await client.close();
      return { time: Date.now() - start, result };
    }
    else if (dbType === 'mysql') {
      // MySQL trả về [rows, fields]
      const [rows] = await adapter.pool.query(action.sql_mysql);
      return { time: Date.now() - start, result: rows };
    }
    else if (dbType === 'postgresql') {
      // PG trả về { rows, ... }
      const { rows } = await adapter.pool.query(action.sql_pg);
      return { time: Date.now() - start, result: rows };
    }
  } catch (e) {
    return { time: -1, error: e.message };
  }
}

// ============================================================
// KHUNG TEST CHUNG
// ============================================================
async function runTestForDB(name, adapter, suffix) {
  console.log(`\n⏳ Đang chạy benchmark cho: ${name.toUpperCase()}...`.cyan);

  // Khởi tạo kết nối nếu cần
  if (adapter.initConnection) await adapter.initConnection();

  // Chờ 1 chút cho kết nối ấm máy
  await new Promise(r => setTimeout(r, 500));

  const stats = {
    Database: name,
    'Count (100k)': '...',
    'Sum ($)': '...',
    'Search (JSON)': '...',
    'Join (50 rows)': '...'
  };

  // --- TEST 1: COUNT ---
  // SQL: SELECT COUNT(*) ...
  // Mongo: estimatedDocumentCount()
  const res1 = await runNative(name, adapter, {
    mongo: (db) => db.collection('users').estimatedDocumentCount(),
    sql_mysql: 'SELECT COUNT(*) as c FROM users',
    sql_pg: 'SELECT COUNT(*) as c FROM users'
  });
  stats['Count (100k)'] = res1.time === -1 ? 'ERROR' : `${res1.time}ms`;

  // --- TEST 2: SUM (AGGREGATION) ---
  // SQL: SELECT SUM(total) ...
  // Mongo: aggregate $group
  const res2 = await runNative(name, adapter, {
    mongo: async (db) => {
      const r = await db.collection('orders').aggregate([{ $group: { _id: null, t: { $sum: "$total" } } }]).toArray();
      return r[0]?.t || 0;
    },
    sql_mysql: 'SELECT SUM(total) as t FROM orders',
    sql_pg: 'SELECT SUM(total) as t FROM orders'
  });
  stats['Sum ($)'] = res2.time === -1 ? 'ERROR' : `${res2.time}ms`;

  // --- TEST 3: DEEP SEARCH (JSON) ---
  // Tìm đơn hàng có món "Phở"
  const keyword = "Phở";
  const res3 = await runNative(name, adapter, {
    mongo: (db) => db.collection('orders').find({ "items.name": { $regex: keyword, $options: 'i' } }).toArray(),
    sql_mysql: `SELECT COUNT(*) FROM orders WHERE JSON_SEARCH(items, 'one', '%${keyword}%') IS NOT NULL`,
    sql_pg: `SELECT COUNT(*) FROM orders WHERE items::text ILIKE '%${keyword}%'`
  });
  stats['Search (JSON)'] = res3.time === -1 ? 'ERROR' : `${res3.time}ms`;

  // --- TEST 4: JOIN ---
  // Lấy 50 order kèm user
  const res4 = await runNative(name, adapter, {
    mongo: (db) => db.collection('orders').aggregate([
      { $limit: 50 },
      { $lookup: { from: 'users', localField: 'userId', foreignField: '_id', as: 'user' } }
    ]).toArray(),
    sql_mysql: 'SELECT o.id, u.name FROM orders o LEFT JOIN users u ON o.user_id = u.id LIMIT 50',
    sql_pg: 'SELECT o.id, u.name FROM orders o LEFT JOIN users u ON o.user_id = u.id LIMIT 50'
  });
  stats['Join (50 rows)'] = res4.time === -1 ? 'ERROR' : `${res4.time}ms`;

  // Đóng kết nối
  try {
    // Chỉ gọi hàm close() của Adapter nếu có, nó sẽ tự xử lý việc đóng pool
    if (adapter.close) {
      await adapter.close();
    }
    // Fallback: Nếu không có hàm close thì mới tự đóng pool (dành cho PG/MySQL nếu thiếu hàm close)
    else if (adapter.pool && typeof adapter.pool.end === 'function') {
      await adapter.pool.end();
    }
  } catch (e) {
    // Bỏ qua lỗi đóng kết nối để không làm hỏng bảng kết quả
  }

  return stats;
}

// ============================================================
// MAIN RUNNER
// ============================================================
(async () => {
  console.log(`\n🚀 BẮT ĐẦU BENCHMARK TOÀN DIỆN (NATIVE QUERY) 🚀`.bold.green);
  console.log(`==================================================`);

  const results = [];

  // Chạy lần lượt (Sequential) để không tranh chấp băng thông
  try {
    // 1. MySQL
    // Lưu ý: Cần đảm bảo .env đang có cấu hình kết nối đúng cho từng cái. 
    // Trong thực tế, bạn có thể cần set lại process.env.DATABASE_URL động nếu 3 DB dùng 3 URL khác nhau.
    // Ở đây giả định Adapter đã hardcode hoặc tự load config riêng.
    results.push(await runTestForDB('mysql', mysqlAdapter));

    // 2. PostgreSQL
    results.push(await runTestForDB('postgresql', pgAdapter));

    // 3. MongoDB
    results.push(await runTestForDB('mongodb', mongoAdapter));

  } catch (e) {
    console.error(e);
  }

  console.log(`\n📊 BẢNG TỔNG SẮP HIỆU NĂNG (Thấp hơn là tốt hơn)`.bold.yellow);
  console.table(results);
  console.log(`\n✅ Hoàn tất!`.green);
  process.exit(0);
})();