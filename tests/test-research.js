// tests/test-research.js
require('dotenv').config();
const colors = require('colors');
const dbType = process.env.DB_CONNECTION;
const adapter = require(`../utils/${dbType === 'postgresql' ? 'PostgreSQL' : dbType === 'mysql' ? 'MySQL' : 'Mongo'}Adapter`);
const { MongoClient } = require('mongodb');

// Hàm chạy SQL Native (Dùng lại logic pool của Adapter)
async function runNativeSQL(sql) {
  const start = Date.now();
  if (dbType === 'mysql') {
    const [rows] = await adapter.pool.query(sql);
    return { time: Date.now() - start, result: rows[0] };
  } else if (dbType === 'postgresql') {
    const { rows } = await adapter.pool.query(sql);
    return { time: Date.now() - start, result: rows[0] };
  }
}

async function run() {
  if (adapter.initConnection) await adapter.initConnection();
  console.log(`\n🔬 NGHIÊN CỨU SÂU: ${dbType.toUpperCase()} (Trên 50.000 Users)`.bold.cyan);

  // --- TEST 1: ĐẾM SỐ LƯỢNG (COUNT) ---
  // So sánh: Tải hết về Node.js (Adapter) vs Lệnh COUNT (Native)
  console.log('\n--- 1. COUNT PERFORMANCE ---'.yellow);

  // Cách 1: Adapter (Tải hết về RAM -> Đếm) -> Cực chậm với Big Data
  const t1 = Date.now();
  const allUsers = await adapter.findAll('users');
  const countJS = allUsers.length;
  const timeJS = Date.now() - t1;
  console.log(`Adapter (JS): ${timeJS}ms | Count: ${countJS}`);

  // Cách 2: Native DB
  let timeNative, countNative;
  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t2 = Date.now();
    countNative = await client.db().collection('users').countDocuments();
    timeNative = Date.now() - t2;
    await client.close();
  } else {
    const res = await runNativeSQL('SELECT COUNT(*) as c FROM users');
    timeNative = res.time;
    countNative = res.result.c || res.result.count;
  }
  console.log(`Native DB:    ${timeNative}ms    | Count: ${countNative}`);
  console.log(`=> Native nhanh gấp ${(timeJS / timeNative).toFixed(1)} lần`.green);

  // --- TEST 2: COMPLEX SEARCH (Tìm kiếm email chứa chuỗi) ---
  console.log('\n--- 2. SEARCH PERFORMANCE ---'.yellow);
  const search = "999"; // Tìm chuỗi '999' trong email

  // Cách 1: Adapter (Filter JS)
  const t3 = Date.now();
  const foundJS = allUsers.filter(u => u.email.includes(search));
  const timeSearchJS = Date.now() - t3;
  console.log(`Adapter (JS): ${timeSearchJS}ms | Found: ${foundJS.length}`);

  // Cách 2: Native
  let timeSearchNative;
  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t4 = Date.now();
    const res = await client.db().collection('users').find({ email: /999/ }).toArray();
    timeSearchNative = Date.now() - t4;
    await client.close();
  } else {
    const res = await runNativeSQL(`SELECT COUNT(*) as c FROM users WHERE email LIKE '%${search}%'`);
    timeSearchNative = res.time;
  }
  console.log(`Native DB:    ${timeSearchNative}ms    | Found: (checked)`);




  // =========================================================
  // TEST C: SCHEMA MIGRATION (THÊM CỘT MỚI CHO 50K USER)
  // Kịch bản: Thêm cột 'loyalty_points' default = 0
  // =========================================================
  console.log('\n--- TEST C: SCHEMA MIGRATION (ADD FIELD) ---'.yellow);

  let timeMigrate;

  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t5 = Date.now();
    // Mongo: Update tất cả document (Backfill data)
    await client.db().collection('users').updateMany({}, { $set: { loyalty_points: 0 } });
    timeMigrate = Date.now() - t5;
    await client.close();
    console.log(`MongoDB ($set all):     ${timeMigrate}ms`);
  } else {
    // SQL: ALTER TABLE
    // Reset cột nếu đã có để test chạy lại được
    try { await runNativeSQL('ALTER TABLE users DROP COLUMN loyalty_points'); } catch (e) { }

    const startSql = Date.now();
    await runNativeSQL('ALTER TABLE users ADD COLUMN loyalty_points INTEGER DEFAULT 0');
    timeMigrate = Date.now() - startSql;
    console.log(`Native SQL (ALTER):     ${timeMigrate}ms`);
  }

  // =========================================================
  // TEST D: PARTIAL JSON UPDATE (SỬA DỮ LIỆU LỒNG NHAU)
  // Kịch bản: Sửa 'quantity' của item đầu tiên trong JSON
  // =========================================================
  console.log('\n--- TEST D: PARTIAL JSON UPDATE ---'.yellow);

  // Lấy đại 1 order để test
  const oneOrder = (await adapter.findAll('orders'))[0];
  if (oneOrder) {
    const orderId = oneOrder.id;
    let timeUpdateJson;

    if (dbType === 'mongodb') {
      const client = new MongoClient(process.env.DATABASE_URL);
      await client.connect();
      const t6 = Date.now();
      // Mongo: Sửa trực tiếp field lồng nhau (Dot notation)
      await client.db().collection('orders').updateOne(
        { _id: orderId }, // Lưu ý: Cần chắc chắn orderId khớp kiểu dữ liệu (Int/ObjectId)
        { $set: { "items.0.quantity": 999 } }
      );
      timeUpdateJson = Date.now() - t6;
      await client.close();
      console.log(`MongoDB ($set nested):  ${timeUpdateJson}ms`);

    } else if (dbType === 'postgresql') {
      // Postgres: Dùng jsonb_set (Khá mạnh, nhưng cú pháp phức tạp)
      // Giả sử cột items là JSONB. Query: UPDATE orders SET items = jsonb_set(items, '{0, quantity}', '999') ...
      const startPg = Date.now();
      // Code native SQL cho Postgres update json path
      await runNativeSQL(`
                UPDATE orders 
                SET items = jsonb_set(items::jsonb, '{0, quantity}', '999') 
                WHERE id = ${orderId}
            `);
      timeUpdateJson = Date.now() - startPg;
      console.log(`Postgres (jsonb_set):   ${timeUpdateJson}ms`);

    } else {
      // MySQL: Dùng JSON_SET
      const startMy = Date.now();
      await runNativeSQL(`
                UPDATE orders 
                SET items = JSON_SET(items, '$[0].quantity', 999) 
                WHERE id = ${orderId}
            `);
      timeUpdateJson = Date.now() - startMy;
      console.log(`MySQL (JSON_SET):       ${timeUpdateJson}ms`);
    }
  } else {
    console.log("Skipping Test D (No orders found to update)".grey);
  }

  process.exit(0);

}
run();