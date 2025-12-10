require('dotenv').config();
const mysqlAdapter = require('../utils/MySQLAdapter');
const mongoAdapter = require('../utils/MongoAdapter'); // Giả sử bạn đã export MongoClient instance hoặc kết nối raw

async function testSchemaFlexibility() {
  console.log("=== TEST 1: SCHEMA FLEXIBILITY (THÊM TRƯỜNG MỚI 'isHotSale') ===");

  const newProductData = {
    name: "Bánh Tráng Trộn Special",
    price: 20000,
    description: "Ngon xoắn lưỡi",
    restaurantId: 1,
    categoryId: 1,
    // Đây là trường mới chưa từng có trong DB
    isHotSale: true,
    metaData: { origin: "Sai Gon", spicyLevel: 5 }
  };

  // --- TEST MONGODB ---
  try {
    console.log("\n1. [MongoDB] Đang thử insert document với trường mới...");
    // Gọi trực tiếp native driver hoặc qua adapter.create
    await mongoAdapter.create('products', newProductData);
    console.log("✅ [MongoDB] Thành công! Không cần định nghĩa trước Schema.");
  } catch (e) {
    console.log("❌ [MongoDB] Thất bại:", e.message);
  }

  // --- TEST MYSQL ---
  try {
    console.log("\n2. [MySQL] Đang thử insert row với cột mới 'isHotSale'...");
    // Giả sử bảng products chỉ có (id, name, price, description)
    const sql = `INSERT INTO products (name, price, description, isHotSale) VALUES (?, ?, ?, ?)`;
    await mysqlAdapter.pool.query(sql, [newProductData.name, newProductData.price, newProductData.description, true]);
    console.log("✅ [MySQL] Thành công!");
  } catch (e) {
    console.log("❌ [MySQL] Thất bại: ", e.message);
    console.log("   -> Lý giải: MySQL bắt buộc phải chạy 'ALTER TABLE ADD COLUMN' trước.");
  }

  process.exit(0);
}

testSchemaFlexibility();