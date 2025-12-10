require('dotenv').config();
const colors = require('colors');
const { MongoClient } = require('mongodb');

// 1. Cấu hình môi trường
const dbType = process.env.DB_CONNECTION || 'mongodb';
const adapterPath = `../utils/${dbType === 'postgresql' ? 'PostgreSQL' : dbType === 'mysql' ? 'MySQL' : 'Mongo'}Adapter`;
const adapter = require(adapterPath);

console.log(`\n🔥 BẮT ĐẦU CUỘC ĐUA: ${dbType.toUpperCase()} 🔥`.bold.cyan);
console.log(`==================================================`);

// ============================================================
// HÀM HỖ TRỢ: CHẠY NATIVE QUERY (Bỏ qua logic Adapter)
// ============================================================

// A. Chạy SQL Thuần (Cho MySQL / PostgreSQL)
async function runNativeSQL(sql) {
  const start = Date.now();
  try {
    if (dbType === 'mysql') {
      const [rows] = await adapter.pool.query(sql);
      return { time: Date.now() - start, result: rows, count: Array.isArray(rows) ? rows.length : 0 };
    } else if (dbType === 'postgresql') {
      const { rows } = await adapter.pool.query(sql);
      return { time: Date.now() - start, result: rows, count: rows.length };
    }
  } catch (e) {
    console.error("❌ SQL Error:".red, e.message);
    return { time: 0, result: [], count: 0 };
  }
}

// B. Chạy Mongo Driver Thuần (Bỏ qua Mongoose)
async function runNativeMongo(callback) {
  const client = new MongoClient(process.env.DATABASE_URL);
  try {
    await client.connect();
    const db = client.db();
    const start = Date.now();

    // Thực thi logic native được truyền vào
    const result = await callback(db);

    const time = Date.now() - start;
    return { time, result };
  } catch (e) {
    console.error("❌ Mongo Error:".red, e.message);
    return { time: 0, result: null };
  } finally {
    await client.close();
  }
}

// ============================================================
// KỊCH BẢN TEST
// ============================================================
async function runBenchmark() {
  // Khởi động kết nối Adapter
  if (adapter.initConnection) await adapter.initConnection();

  // Đợi 1 chút để kết nối ổn định
  await new Promise(r => setTimeout(r, 1000));

  // ---------------------------------------------------------
  // TEST 1: COUNT (ĐẾM SỐ LƯỢNG LỚN)
  // Kịch bản: Đếm tổng số user trong hệ thống
  // ---------------------------------------------------------
  console.log('\n🏁 TEST 1: COUNT PERFORMANCE (100k records)'.yellow.bold);

  // --- CÁCH 1: ADAPTER (Tải về RAM đếm) ---
  const t1 = Date.now();
  const allUsers = await adapter.findAll('users');
  const countAdapter = allUsers.length;
  const timeAdapter1 = Date.now() - t1;
  console.log(`   🔸 Adapter (Node.js):  ${timeAdapter1}ms | Count: ${countAdapter} (Tốn RAM)`);

  // --- CÁCH 2: NATIVE (Database đếm) ---
  let timeNative1;
  if (dbType === 'mongodb') {
    const res = await runNativeMongo(async (db) => {
      return await db.collection('users').estimatedDocumentCount();
    });
    timeNative1 = res.time;
  } else {
    const res = await runNativeSQL('SELECT COUNT(*) as c FROM users');
    timeNative1 = res.time;
  }
  console.log(`   🔹 Native (Database):  ${timeNative1}ms    | Optimized 🚀`);
  console.log(`   => Native nhanh gấp ${(timeAdapter1 / (timeNative1 || 1)).toFixed(1)} lần`.green);


  // ---------------------------------------------------------
  // TEST 2: AGGREGATION (TÍNH TỔNG TIỀN)
  // Kịch bản: Tính tổng doanh thu từ bảng Orders
  // ---------------------------------------------------------
  console.log('\n🏁 TEST 2: AGGREGATION (SUM TOTAL)'.yellow.bold);

  // --- CÁCH 1: ADAPTER (Dùng Javascript Reduce) ---
  const t2 = Date.now();
  const allOrders = await adapter.findAll('orders');
  const sumAdapter = allOrders.reduce((sum, o) => sum + (Number(o.total) || 0), 0);
  const timeAdapter2 = Date.now() - t2;
  console.log(`   🔸 Adapter (JS Reduce): ${timeAdapter2}ms | Sum: ${sumAdapter}`);

  // --- CÁCH 2: NATIVE (Dùng SQL SUM / Mongo $group) ---
  let timeNative2, sumNative;
  if (dbType === 'mongodb') {
    const res = await runNativeMongo(async (db) => {
      const r = await db.collection('orders').aggregate([
        { $group: { _id: null, total: { $sum: "$total" } } }
      ]).toArray();
      return r[0]?.total || 0;
    });
    timeNative2 = res.time;
    sumNative = res.result;
  } else {
    const res = await runNativeSQL('SELECT SUM(total) as t FROM orders');
    timeNative2 = res.time;
    // Postgres trả về string cho SUM lớn, MySQL trả về number/string tùy driver
    sumNative = Number(res.result[0].t || res.result[0].sum || 0);
  }
  console.log(`   🔹 Native (DB Engine):  ${timeNative2}ms    | Sum: ${sumNative}`);
  console.log(`   => Native nhanh gấp ${(timeAdapter2 / (timeNative2 || 1)).toFixed(1)} lần`.green);


  // ---------------------------------------------------------
  // TEST 3: DEEP JSON SEARCH (TÌM TRONG JSON)
  // Kịch bản: Tìm đơn hàng có chứa món "Phở" trong mảng items
  // ---------------------------------------------------------
  console.log('\n🏁 TEST 3: DEEP JSON SEARCH'.yellow.bold);
  const keyword = "Phở";

  // --- CÁCH 1: ADAPTER (Tải hết về rồi Filter bằng JS) ---
  const t3 = Date.now();
  // Tái sử dụng allOrders từ Test 2 để công bằng (coi như đã fetch xong)
  const foundAdapter = allOrders.filter(o => {
    let items = o.items;
    // MySQL Adapter trả về string JSON, cần parse
    if (typeof items === 'string') {
      try { items = JSON.parse(items); } catch (e) { items = []; }
    }
    return Array.isArray(items) && items.some(i => i.name && i.name.includes(keyword));
  });
  const timeAdapter3 = Date.now() - t3;
  console.log(`   🔸 Adapter (JS Filter): ${timeAdapter3}ms | Found: ${foundAdapter.length}`);

  // --- CÁCH 2: NATIVE (Query JSON trực tiếp) ---
  let timeNative3, countNative3;
  if (dbType === 'mongodb') {
    const res = await runNativeMongo(async (db) => {
      // Mongo tìm trong mảng cực dễ với dot notation
      return await db.collection('orders').find({
        "items.name": { $regex: keyword, $options: 'i' }
      }).toArray();
    });
    timeNative3 = res.time;
    countNative3 = res.result.length;
    console.log(`   🔹 Native (Mongo Find): ${timeNative3}ms    | Found: ${countNative3}`);

  } else if (dbType === 'postgresql') {
    // Postgres dùng toán tử JSONB text search
    const sql = `SELECT COUNT(*) as c FROM orders WHERE items::text ILIKE '%${keyword}%'`;
    const res = await runNativeSQL(sql);
    timeNative3 = res.time;
    countNative3 = res.result[0].c;
    console.log(`   🔹 Native (PG ILIKE):   ${timeNative3}ms    | Found: ${countNative3}`);

  } else { // MySQL
    // MySQL dùng JSON_SEARCH
    const sql = `SELECT COUNT(*) as c FROM orders WHERE JSON_SEARCH(items, 'one', '%${keyword}%') IS NOT NULL`;
    const res = await runNativeSQL(sql);
    timeNative3 = res.time;
    countNative3 = res.result[0].c;
    console.log(`   🔹 Native (JSON_SEARCH):${timeNative3}ms    | Found: ${countNative3}`);
  }

  if (timeNative3) {
    console.log(`   => Native nhanh gấp ${(timeAdapter3 / (timeNative3 || 1)).toFixed(1)} lần`.green);
  }


  // ---------------------------------------------------------
  // TEST 4: JOIN (QUAN HỆ DỮ LIỆU)
  // Kịch bản: Lấy 50 orders kèm thông tin User
  // ---------------------------------------------------------
  console.log('\n🏁 TEST 4: JOIN PERFORMANCE'.yellow.bold);

  // --- CÁCH 1: ADAPTER (Application-Level Join) ---
  const t4 = Date.now();
  // Giả sử hàm findAllAdvanced của bạn có logic populate/expand
  await adapter.findAllAdvanced('orders', { limit: 50, expand: 'user' });
  const timeAdapter4 = Date.now() - t4;
  console.log(`   🔸 Adapter (App Join):  ${timeAdapter4}ms | (N+1 Query simulation)`);

  // --- CÁCH 2: NATIVE (SQL JOIN / Mongo Lookup) ---
  let timeNative4;
  if (dbType === 'mongodb') {
    const res = await runNativeMongo(async (db) => {
      return await db.collection('orders').aggregate([
        { $limit: 50 },
        {
          $lookup: {
            from: 'users',
            localField: 'userId', // Lưu ý: Field name trong DB Mongo
            foreignField: '_id', // Field name trong DB Mongo
            as: 'user'
          }
        }
      ]).toArray();
    });
    timeNative4 = res.time;
    console.log(`   🔹 Native ($lookup):    ${timeNative4}ms`);

  } else { // SQL
    const sql = `SELECT o.*, u.name as user_name FROM orders o LEFT JOIN users u ON o.user_id = u.id LIMIT 50`;
    const res = await runNativeSQL(sql);
    timeNative4 = res.time;
    console.log(`   🔹 Native (SQL JOIN):   ${timeNative4}ms`);
  }
  console.log(`   => Native nhanh gấp ${(timeAdapter4 / (timeNative4 || 1)).toFixed(1)} lần`.green);

  console.log(`\n✅ TEST HOÀN TẤT CHO ${dbType.toUpperCase()}`.bold.cyan);
  process.exit(0);
}

runBenchmark();