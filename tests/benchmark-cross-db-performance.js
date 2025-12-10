require('dotenv').config();
const colors = require('colors');
const { MongoClient } = require('mongodb');

// Import Adapters
const mysqlAdapter = require('../utils/MySQLAdapter');
const pgAdapter = require('../utils/PostgreSQLAdapter');
const mongoAdapter = require('../utils/MongoAdapter');

const MONGO_URL = process.env.MONGO_URI || process.env.DATABASE_URL;

// ============================================================
// HÀM CHẠY QUERY NATIVE (ĐÃ TỐI ƯU KẾT NỐI)
// ============================================================
async function runNative(dbType, context, action) {
  const start = Date.now();
  try {
    if (dbType === 'mongodb') {
      // Sử dụng db instance đã kết nối sẵn
      const result = await action.mongo(context.mongoDbInstance);
      return { time: Date.now() - start, result };
    }
    else if (dbType === 'mysql') {
      const [rows] = await context.mysqlPool.query(action.sql_mysql);
      return { time: Date.now() - start, result: rows };
    }
    else if (dbType === 'postgresql') {
      const { rows } = await context.pgPool.query(action.sql_pg);
      return { time: Date.now() - start, result: rows };
    }
  } catch (e) {
    return { time: -1, error: e.message };
  }
}

// ============================================================
// KHUNG TEST CHO TỪNG DB
// ============================================================
async function runTestForDB(name, adapter) {
  console.log(`\n⏳ Đang chuẩn bị môi trường cho: ${name.toUpperCase()}...`.cyan);

  const context = {};

  // 1. KHỞI TẠO KẾT NỐI (KHÔNG TÍNH VÀO THỜI GIAN BENCHMARK)
  try {
    if (name === 'mongodb') {
      const client = new MongoClient(MONGO_URL);
      await client.connect();
      context.mongoClient = client;
      context.mongoDbInstance = client.db();
    } else if (name === 'mysql') {
      if (!adapter.pool) await adapter.initConnection();
      context.mysqlPool = adapter.pool;
    } else if (name === 'postgresql') {
      if (!adapter.pool) await adapter.initConnection();
      context.pgPool = adapter.pool;
    }
  } catch (e) {
    console.log(`❌ Không thể kết nối ${name}: ${e.message}`.red);
    return { Database: name, Note: 'Connection Failed' };
  }

  // Warmup (Chạy nháp 1 lần để máy nóng máy)
  await new Promise(r => setTimeout(r, 200));

  const stats = {
    Database: name,
    'Count (100k)': '...',
    'Sum ($)': '...',
    'Search (JSON)': '...',
    'Join (50 rows)': '...'
  };

  console.log(`   🚀 Đang chạy tests...`.green);

  // --- TEST 1: COUNT ---
  const res1 = await runNative(name, context, {
    mongo: (db) => db.collection('users').estimatedDocumentCount(),
    sql_mysql: 'SELECT COUNT(*) as c FROM users',
    sql_pg: 'SELECT COUNT(*) as c FROM users'
  });
  stats['Count (100k)'] = res1.time === -1 ? 'ERROR' : `${res1.time}ms`;

  // --- TEST 2: SUM ---
  const res2 = await runNative(name, context, {
    mongo: async (db) => {
      const r = await db.collection('orders').aggregate([{ $group: { _id: null, t: { $sum: "$total" } } }]).toArray();
      return r[0]?.t || 0;
    },
    sql_mysql: 'SELECT SUM(total) as t FROM orders',
    sql_pg: 'SELECT SUM(total) as t FROM orders'
  });
  stats['Sum ($)'] = res2.time === -1 ? 'ERROR' : `${res2.time}ms`;

  // --- TEST 3: SEARCH JSON ---
  const keyword = "Phở";
  const res3 = await runNative(name, context, {
    mongo: (db) => db.collection('orders').find({ "items.name": { $regex: keyword, $options: 'i' } }).toArray(),
    sql_mysql: `SELECT COUNT(*) FROM orders WHERE JSON_SEARCH(items, 'one', '%${keyword}%') IS NOT NULL`,
    sql_pg: `SELECT COUNT(*) FROM orders WHERE items::text ILIKE '%${keyword}%'`
  });
  stats['Search (JSON)'] = res3.time === -1 ? 'ERROR' : `${res3.time}ms`;

  // --- TEST 4: JOIN ---
  const res4 = await runNative(name, context, {
    mongo: (db) => db.collection('orders').aggregate([
      { $limit: 50 },
      { $lookup: { from: 'users', localField: 'userId', foreignField: '_id', as: 'user' } }
    ]).toArray(),
    sql_mysql: 'SELECT o.id, u.name FROM orders o LEFT JOIN users u ON o.user_id = u.id LIMIT 50',
    sql_pg: 'SELECT o.id, u.name FROM orders o LEFT JOIN users u ON o.user_id = u.id LIMIT 50'
  });
  stats['Join (50 rows)'] = res4.time === -1 ? 'ERROR' : `${res4.time}ms`;

  // CLEANUP
  if (name === 'mongodb') await context.mongoClient.close();
  // SQL adapters giữ pool để tái sử dụng hoặc đóng ở cuối cùng script

  return stats;
}

// ============================================================
// MAIN RUNNER
// ============================================================
(async () => {
  console.log(`\n🚀 BẮT ĐẦU BENCHMARK TOÀN DIỆN (NATIVE QUERY) 🚀`.bold.green);
  console.log(`========================================================`);

  const results = [];

  // Chạy tuần tự
  try {
    // MySQL
    // Lưu ý: Đảm bảo .env của bạn đang trỏ đúng hoặc Adapter tự xử lý config
    results.push(await runTestForDB('mysql', mysqlAdapter));

    // Postgres
    results.push(await runTestForDB('postgresql', pgAdapter));

    // Mongo
    results.push(await runTestForDB('mongodb', mongoAdapter));

  } catch (e) {
    console.error(e);
  }

  console.log(`\n📊 BẢNG TỔNG SẮP HIỆU NĂNG THỰC TẾ (Thấp hơn là tốt hơn)`.bold.yellow);
  console.table(results);

  console.log(`\n✅ Hoàn tất!`.green);
  process.exit(0);
})();