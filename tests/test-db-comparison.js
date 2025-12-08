/**
 * 🧪 Database Adapter Comparison Test Suite (Fixed)
 * * Cách chạy:
 * 1. Đảm bảo file .env đã cấu hình đúng DB_CONNECTION (mysql, postgresql, mongodb)
 * 2. Chạy lệnh: node tests/test-db-comparison.js
 */

require('dotenv').config();
const colors = require('colors');

// Dữ liệu test mẫu
const TEST_DATA = {
  user: {
    name: 'DB Test User',
    email: `dbtest_${Date.now()}@test.com`,
    password: 'Password123',
    phone: '0987654321',
    role: 'customer',
    isActive: true
  },
  // Item order có cấu trúc phức tạp để test JSON
  orderItems: [
    { productId: 1, quantity: 2, note: "Không hành" },
    { productId: 5, quantity: 1, options: { size: "L", sugar: "50%" } }
  ]
};

// ==================== HELPERS ====================
function log(msg, type = 'info') {
  const map = { info: 'cyan', success: 'green', error: 'red', warn: 'yellow' };
  const symbol = { info: 'ℹ️', success: '✅', error: '❌', warn: '⚠️' };
  console.log(`${symbol[type]} ${msg}`[map[type]]);
}

async function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// ==================== TEST ENGINE ====================
class DatabaseTester {
  constructor(dbType) {
    this.dbType = dbType;
    this.db = null;
    this.userId = null;
  }

  async connect() {
    try {
      // Thiết lập môi trường và xóa cache để load lại adapter mới
      process.env.DB_CONNECTION = this.dbType;
      
      // Xóa cache của file database config và các adapter
      const modulesToClear = [
        '../config/database',
        '../utils/MySQLAdapter',
        '../utils/PostgreSQLAdapter',
        '../utils/MongoAdapter'
      ];
      
      modulesToClear.forEach(mod => {
        try {
          delete require.cache[require.resolve(mod)];
        } catch (e) {}
      });

      this.db = require('../config/database');
      
      // Đợi kết nối ổn định (quan trọng cho MySQL/PG pool)
      await delay(1000);
      return true;
    } catch (e) {
      log(`Không thể kết nối ${this.dbType}: ${e.message}`, 'error');
      return false;
    }
  }

  async run() {
    console.log(`\n============== TESTING: ${this.dbType.toUpperCase()} ==============`.bold.white);
    
    // --- TEST 1: CREATE & BASIC TYPE ---
    try {
      const start = Date.now();
      const user = await this.db.create('users', TEST_DATA.user);
      const time = Date.now() - start;
      
      if (!user || !user.id) throw new Error("Create failed: No ID returned");
      
      this.userId = user.id; // Lưu ID để dùng cho các test sau
      const idType = typeof user.id;
      log(`Create User: ${time}ms | ID Type: ${idType}`, 'success');
    } catch (e) {
      log(`Create Test Failed: ${e.message}`, 'error');
      return; // Dừng nếu không tạo được user
    }

    // --- TEST 2: CASE SENSITIVITY (Độ nhạy chữ hoa/thường) ---
    try {
      // Tìm email viết hoa toàn bộ: DBTEST_...@TEST.COM
      const upperEmail = TEST_DATA.user.email.toUpperCase();
      const found = await this.db.findOne('users', { email: upperEmail });
      
      if (found) {
        log(`Case Sensitivity: Case-Insensitive (Tìm thấy email viết hoa)`, 'warn');
      } else {
        log(`Case Sensitivity: Case-Sensitive (Không tìm thấy email viết hoa)`, 'success');
      }
    } catch (e) {
      log(`Case Test Failed: ${e.message}`, 'error');
    }

    // --- TEST 3: JSON HANDLING (Cấu trúc dữ liệu phức tạp) ---
    try {
      // Tạo một order chứa JSON array phức tạp
      const orderData = {
        userId: this.userId,
        restaurantId: 1,
        items: TEST_DATA.orderItems, // JSON Array
        subtotal: 100000,
        deliveryFee: 15000,
        total: 115000,
        status: 'pending',
        paymentMethod: 'cash',
        deliveryAddress: 'Test Address'
      };

      const order = await this.db.create('orders', orderData);
      
      // Đọc lại từ DB để kiểm tra
      const fetchedOrder = await this.db.findById('orders', order.id);
      
      // Kiểm tra xem items có còn là Array không hay bị biến thành String
      const isArray = Array.isArray(fetchedOrder.items);
      let isDeepEqual = false;
      
      if (isArray && fetchedOrder.items[1] && fetchedOrder.items[1].options) {
        isDeepEqual = fetchedOrder.items[1].options.sugar === "50%";
      }
      
      if (isArray && isDeepEqual) {
        log(`JSON Handling: Perfect (Object structure preserved)`, 'success');
      } else {
        log(`JSON Handling: Broken or Stringified (Got type: ${typeof fetchedOrder.items})`, 'warn');
      }
      
      // Dọn dẹp order
      if(order && order.id) await this.db.delete('orders', order.id);

    } catch (e) {
      log(`JSON Test Failed: ${e.message}`, 'error');
    }

    // --- TEST 4: FULL-TEXT SEARCH (FIXED) ---
    // Fix: Thay vì search bảng 'users' (không có cột description), ta tạo dummy category
    try {
      // 1. Tạo Category để test search (Bảng categories có cột description trong MySQLAdapter)
      const catName = `SearchTest_${Date.now()}`;
      const catData = { 
        name: catName,
        description: "This is a searchable description for testing" 
      };
      
      const cat = await this.db.create('categories', catData);
      
      // 2. Thực hiện search chữ "searchable"
      const start = Date.now();
      const results = await this.db.findAllAdvanced('categories', { q: 'searchable' });
      const time = Date.now() - start;
      
      // 3. Kiểm tra kết quả
      const found = results.data.find(c => c.id === cat.id);
      
      if (found) {
        log(`Search Feature: Working (Found record via description in ${time}ms)`, 'success');
      } else {
        log(`Search Feature: Failed (Record not found)`, 'warn');
      }

      // 4. Dọn dẹp
      if(cat && cat.id) await this.db.delete('categories', cat.id);

    } catch (e) {
      // MySQLAdapter mặc định search cả 'name' và 'description', 
      // nếu bảng không có cột description sẽ lỗi. Test này dùng bảng categories nên sẽ an toàn.
      log(`Search Test Failed: ${e.message}`, 'error');
    }

    // --- CLEANUP & CLOSE ---
    if (this.userId) {
      await this.db.delete('users', this.userId);
    }
    
    // Fix lỗi đóng kết nối: Chỉ gọi close 1 lần
    if (this.db && typeof this.db.close === 'function') {
      try {
        await this.db.close();
        // log('Connection closed', 'info');
      } catch (e) {
        // Bỏ qua lỗi khi đóng kết nối
      }
    }
  }
}

// ==================== MAIN EXECUTION ====================
async function main() {
  const dbType = process.env.DB_CONNECTION || 'json';
  
  console.log(`🚀 STARTING DB TEST FOR: ${dbType.toUpperCase()}`);

  // Nếu là mongodb nhưng chưa config URL thì báo lỗi
  if (dbType === 'mongodb' && !process.env.DATABASE_URL) {
    log(`Skipping MongoDB (Missing DATABASE_URL in .env)`, 'warn');
    process.exit(0);
  }

  const tester = new DatabaseTester(dbType);
  
  if (await tester.connect()) {
    await tester.run();
  }
  
  console.log('\n✨ Test Complete!');
  process.exit(0);
}

main();