require('dotenv').config();
const colors = require('colors');
const { MongoClient } = require('mongodb');

// Lấy Adapter và Loại DB từ biến môi trường
const dbType = process.env.DB_CONNECTION || 'mongodb';
const adapterPath = `../utils/${dbType === 'postgresql' ? 'PostgreSQL' : dbType === 'mysql' ? 'MySQL' : 'Mongo'}Adapter`;
const adapter = require(adapterPath);

// ============================================================
// HÀM HỖ TRỢ CHẠY SQL NATIVE (Bỏ qua logic Adapter)
// ============================================================
async function runNativeSQL(sql) {
  const start = Date.now();
  try {
    if (dbType === 'mysql') {
      const [rows] = await adapter.pool.query(sql);
      return { time: Date.now() - start, result: rows, count: rows.length };
    } else if (dbType === 'postgresql') {
      const { rows } = await adapter.pool.query(sql);
      return { time: Date.now() - start, result: rows, count: rows.length };
    }
  } catch (e) {
    console.error("SQL Error:".red, e.message);
    return { time: 0, result: [], count: 0 };
  }
}

// ============================================================
// MAIN TEST RUNNER
// ============================================================
async function runResearch() {
  if (adapter.initConnection) await adapter.initConnection();

  console.log(`\n█▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█`.cyan);
  console.log(`█  RESEARCH DEEP DIVE: ${dbType.toUpperCase().padEnd(35)} █`.cyan.bold);
  console.log(`█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄█\n`.cyan);

  // ---------------------------------------------------------
  // TEST A: COUNT PERFORMANCE (Đếm số lượng bản ghi lớn)
  // Giả lập: Trang Dashboard Admin cần hiển thị tổng user
  // ---------------------------------------------------------
  console.log('🔹 TEST A: COUNT ALL USERS (100k records)'.yellow.bold);

  // 1. Adapter (Tải hết về RAM -> Đếm)
  const t1 = Date.now();
  const allUsers = await adapter.findAll('users');
  const countJS = allUsers.length;
  const timeJS = Date.now() - t1;
  console.log(`   1. Adapter (JS .length):  ${timeJS}ms | RAM Usage: High ⚠️`);

  // 2. Native (DB Count)
  let timeNativeA, countNative;
  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t = Date.now();
    countNative = await client.db().collection('users').estimatedDocumentCount(); // Siêu nhanh
    timeNativeA = Date.now() - t;
    await client.close();
  } else {
    const res = await runNativeSQL('SELECT COUNT(*) as c FROM users');
    timeNativeA = res.time;
    countNative = res.result[0].c || res.result[0].count;
  }
  console.log(`   2. Native DB (Count):     ${timeNativeA}ms    | Optimized 🚀`);
  console.log(`   => Chênh lệch: ${(timeJS / (timeNativeA || 1)).toFixed(1)}x\n`.green);


  // ---------------------------------------------------------
  // TEST B: AGGREGATION (Tính tổng tiền)
  // Giả lập: Báo cáo doanh thu
  // ---------------------------------------------------------
  console.log('🔹 TEST B: REVENUE CALCULATION (SUM)'.yellow.bold);

  // 1. Adapter (Tải Orders -> JS Reduce)
  const t3 = Date.now();
  const orders = await adapter.findAll('orders');
  const sumJS = orders.reduce((sum, o) => sum + (Number(o.total) || 0), 0);
  const timeSumJS = Date.now() - t3;
  console.log(`   1. Adapter (JS Reduce):   ${timeSumJS}ms | Result: ${sumJS}`);

  // 2. Native (DB SUM)
  let timeNativeB, sumNative;
  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t = Date.now();
    const res = await client.db().collection('orders').aggregate([
      { $group: { _id: null, total: { $sum: "$total" } } }
    ]).toArray();
    timeNativeB = Date.now() - t;
    sumNative = res[0]?.total || 0;
    await client.close();
  } else {
    const res = await runNativeSQL('SELECT SUM(total) as total FROM orders');
    timeNativeB = res.time;
    sumNative = res.result[0].total || res.result[0].sum;
  }
  console.log(`   2. Native DB (SUM):       ${timeNativeB}ms    | Result: ${sumNative}`);
  console.log(`   => Chênh lệch: ${(timeSumJS / (timeNativeB || 1)).toFixed(1)}x\n`.green);


  // ---------------------------------------------------------
  // TEST C: DEEP JSON SEARCH (Tìm kiếm trong JSON Array)
  // Giả lập: Tìm tất cả đơn hàng có món "Phở"
  // ---------------------------------------------------------
  console.log('🔹 TEST C: DEEP JSON SEARCH (Search in Array)'.yellow.bold);
  const keyword = "Phở";

  // 1. Adapter (Filter JS)
  const t5 = Date.now();
  // (Dùng lại biến orders từ Test B để công bằng, coi như đã fetch)
  const foundJS = orders.filter(o => {
    let items = o.items;
    if (typeof items === 'string') {
      try { items = JSON.parse(items) } catch (e) { items = [] }
    }
    return Array.isArray(items) && items.some(i => i.name && i.name.includes(keyword));
  });
  const timeSearchJS = Date.now() - t5;
  console.log(`   1. Adapter (JS Filter):   ${timeSearchJS}ms | Found: ${foundJS.length}`);

  // 2. Native JSON Query
  let timeNativeC, countNativeC;
  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t = Date.now();
    const res = await client.db().collection('orders').find({
      "items.name": { $regex: keyword, $options: 'i' }
    }).toArray();
    timeNativeC = Date.now() - t;
    countNativeC = res.length;
    await client.close();
    console.log(`   2. Native (Dot Notation): ${timeNativeC}ms    | Found: ${countNativeC}`);
  } else if (dbType === 'postgresql') {
    // Postgres JSONB: items @> '[{"name": "..."}]' (Exact match) hoặc convert text tìm like
    const sql = `SELECT COUNT(*) as c FROM orders WHERE items::text ILIKE '%${keyword}%'`;
    const res = await runNativeSQL(sql);
    timeNativeC = res.time;
    countNativeC = res.result[0].c;
    console.log(`   2. Native (Text Search):  ${timeNativeC}ms    | Found: ${countNativeC}`);
  } else { // MySQL
    const sql = `SELECT COUNT(*) as c FROM orders WHERE JSON_SEARCH(items, 'one', '%${keyword}%') IS NOT NULL`;
    const res = await runNativeSQL(sql);
    timeNativeC = res.time;
    countNativeC = res.result[0].c;
    console.log(`   2. Native (JSON_SEARCH):  ${timeNativeC}ms    | Found: ${countNativeC}`);
  }


  // ---------------------------------------------------------
  // TEST D: PARTIAL UPDATE (Cập nhật 1 trường nhỏ)
  // Giả lập: Sửa số lượng của item đầu tiên trong đơn hàng
  // ---------------------------------------------------------
  console.log('\n🔹 TEST D: PARTIAL UPDATE (Atomic Operation)'.yellow.bold);

  if (orders.length > 0) {
    const targetId = orders[0].id;

    // 1. Adapter: Read -> Parse -> Modify -> Stringify -> Write
    const t7 = Date.now();
    const orderToUpdate = await adapter.findById('orders', targetId);
    let items = orderToUpdate.items;
    if (typeof items === 'string') items = JSON.parse(items);
    if (items.length > 0) items[0].quantity = 999; // Modify
    await adapter.update('orders', targetId, { items });
    const timeUpdateJS = Date.now() - t7;
    console.log(`   1. Adapter (Full Rewrite): ${timeUpdateJS}ms | Risk: Race Condition ⚠️`);

    // 2. Native: Set field directly
    let timeNativeD;
    if (dbType === 'mongodb') {
      const client = new MongoClient(process.env.DATABASE_URL);
      await client.connect();
      const t = Date.now();
      await client.db().collection('orders').updateOne(
        { _id: targetId }, // Lưu ý: ID có thể lệch kiểu (int vs objectId) tùy seed
        { $set: { "items.0.quantity": 888 } }
      );
      timeNativeD = Date.now() - t;
      await client.close();
      console.log(`   2. Native ($set):          ${timeNativeD}ms    | Safe & Fast ✅`);
    } else if (dbType === 'postgresql') {
      const sql = `UPDATE orders SET items = jsonb_set(items::jsonb, '{0,quantity}', '888') WHERE id = ${targetId}`;
      const res = await runNativeSQL(sql);
      timeNativeD = res.time;
      console.log(`   2. Native (jsonb_set):     ${timeNativeD}ms    | Safe & Fast ✅`);
    } else { // MySQL
      const sql = `UPDATE orders SET items = JSON_SET(items, '$[0].quantity', 888) WHERE id = ${targetId}`;
      const res = await runNativeSQL(sql);
      timeNativeD = res.time;
      console.log(`   2. Native (JSON_SET):      ${timeNativeD}ms    | Safe & Fast ✅`);
    }
  } else {
    console.log("   Skipping Test D (No orders found)");
  }

  // ---------------------------------------------------------
  // TEST E: NATIVE JOIN VS APPLICATION JOIN
  // Giả lập: Lấy danh sách 50 Order kèm thông tin User
  // ---------------------------------------------------------
  console.log('\n🔹 TEST E: JOIN PERFORMANCE'.yellow.bold);

  // 1. Adapter (App Join)
  const t9 = Date.now();
  // Giả sử hàm findAllAdvanced thực hiện join như code cũ
  const ordersWithUser = await adapter.findAllAdvanced('orders', { limit: 50, expand: 'user' });
  const timeJoinJS = Date.now() - t9;
  console.log(`   1. Adapter (App Logic):    ${timeJoinJS}ms`);

  // 2. Native Join
  let timeNativeE;
  if (dbType === 'mongodb') {
    const client = new MongoClient(process.env.DATABASE_URL);
    await client.connect();
    const t = Date.now();
    await client.db().collection('orders').aggregate([
      { $limit: 50 },
      {
        $lookup: {
          from: 'users',
          localField: 'userId', // Cần check kỹ field name trong DB thật
          foreignField: '_id',
          as: 'user'
        }
      }
    ]).toArray();
    timeNativeE = Date.now() - t;
    await client.close();
    console.log(`   2. Native ($lookup):       ${timeNativeE}ms`);
  } else {
    // SQL Join
    const sql = `SELECT o.*, u.name as user_name FROM orders o LEFT JOIN users u ON o.user_id = u.id LIMIT 50`;
    const res = await runNativeSQL(sql);
    timeNativeE = res.time;
    console.log(`   2. Native (LEFT JOIN):     ${timeNativeE}ms`);
  }

  console.log(`\n🏁 RESEARCH COMPLETE`.cyan.bold);
  process.exit(0);
}

runResearch();