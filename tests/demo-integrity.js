require('dotenv').config();
const mysqlAdapter = require('../utils/MySQLAdapter');
const mongoAdapter = require('../utils/MongoAdapter');

async function testIntegrity() {
  console.log("=== TEST 2: DATA INTEGRITY (RÀNG BUỘC KHÓA NGOẠI) ===");
  console.log("Kịch bản: Thêm một Order cho một User ID không tồn tại (ID = 999999)\n");

  const invalidOrder = {
    userId: 999999, // User này không có thật
    total: 50000,
    status: 'pending'
  };

  // --- TEST MONGODB ---
  try {
    console.log("1. [MongoDB] Thử insert order với userId rác...");
    await mongoAdapter.create('orders', invalidOrder);
    console.log("⚠️ [MongoDB] Thành công (Nhưng dữ liệu bị rác).");
    console.log("   -> Lý giải: MongoDB mặc định không kiểm tra quan hệ (trừ khi dùng Schema Validation phức tạp).");
  } catch (e) {
    console.log("❌ [MongoDB] Lỗi:", e.message);
  }

  // --- TEST MYSQL ---
  try {
    console.log("\n2. [MySQL] Thử insert order với user_id rác...");
    const sql = `INSERT INTO orders (user_id, total, status) VALUES (?, ?, ?)`;
    await mysqlAdapter.pool.query(sql, [invalidOrder.userId, invalidOrder.total, invalidOrder.status]);
    console.log("⚠️ [MySQL] Thành công.");
  } catch (e) {
    console.log("✅ [MySQL] Hệ thống từ chối: ", e.message);
    console.log("   -> Lý giải: MySQL Foreign Key Constraint ngăn chặn dữ liệu rác.");
  }

  process.exit(0);
}

testIntegrity();