require('dotenv').config();
const mysqlAdapter = require('../utils/MySQLAdapter');
const mongoAdapter = require('../utils/MongoAdapter');

async function testIntegrity() {
  if (mysqlAdapter.initConnection) await mysqlAdapter.initConnection();
  if (mongoAdapter.initConnection) await mongoAdapter.initConnection();

  console.log("=== TEST 2: DATA INTEGRITY (RÀNG BUỘC KHÓA NGOẠI) ===");
  console.log("Kịch bản: Thêm một Order cho một User ID không tồn tại (ID = 999999)\n");

  // 1. Cung cấp ĐẦY ĐỦ dữ liệu để vượt qua vòng validate cơ bản
  const invalidOrder = {
    userId: 999999, // <--- CỐ TÌNH SAI (User này không tồn tại)
    restaurantId: 1, // ID nhà hàng (giả định có tồn tại)
    total: 50000,
    subtotal: 35000,
    deliveryFee: 15000,
    status: 'pending',
    paymentMethod: 'cash',
    deliveryAddress: '123 Test Street',
    // Mongo cần mảng object, MySQL sẽ stringify sau
    items: [{ productId: 1, quantity: 1, price: 35000, name: "Test Product" }]
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

    // Cập nhật câu lệnh SQL để insert đủ các trường bắt buộc
    const sql = `
      INSERT INTO orders 
      (user_id, restaurant_id, total, subtotal, delivery_fee, status, payment_method, delivery_address, items) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    // Chuẩn bị params (MySQL lưu items dưới dạng JSON string)
    const params = [
      invalidOrder.userId,
      invalidOrder.restaurantId,
      invalidOrder.total,
      invalidOrder.subtotal,
      invalidOrder.deliveryFee,
      invalidOrder.status,
      invalidOrder.paymentMethod,
      invalidOrder.deliveryAddress,
      JSON.stringify(invalidOrder.items)
    ];

    await mysqlAdapter.pool.query(sql, params);
    console.log("⚠️ [MySQL] Thành công (Điều này là sai!).");
  } catch (e) {
    // Đây là kết quả mong đợi
    console.log("✅ [MySQL] Hệ thống từ chối: ", e.message);
    console.log("   -> Lý giải: MySQL Foreign Key Constraint ngăn chặn dữ liệu rác.");
  }

  process.exit(0);
}

testIntegrity();