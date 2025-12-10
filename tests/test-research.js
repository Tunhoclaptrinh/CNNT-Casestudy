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

  process.exit(0);
}
run();