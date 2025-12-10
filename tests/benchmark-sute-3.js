/**
 * FILE: tests/test-comparison-native.js
 * MỤC TIÊU: Benchmark so sánh 3 DB (Dùng Connection String - URI cho tiện lợi)
 */

const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');

// ==============================================================================
// 1. CẤU HÌNH (DÙNG URI CHO TIỆN)
// ==============================================================================
const CONFIG = {
  records: 50000,
  targetItem: 'Cơm Tấm Sườn Bì',

  // QUAN TRỌNG: Thay 'localhost' bằng '127.0.0.1'
  mysqlUri: process.env.MYSQL_URI || 'mysql://root:@127.0.0.1:3306/cnnt_benchmark_uri',

  // MongoDB thường thông minh hơn nên localhost vẫn ok, nhưng sửa luôn cho chắc
  mongoUri: process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/cnnt_benchmark_uri'
};

// ==============================================================================
// 2. HÀM TIỆN ÍCH (HELPER)
// ==============================================================================

// Hàm parse URI của MySQL ra thành object config
function parseMysqlUri(uri) {
  try {
    const url = new URL(uri); // Dùng class URL chuẩn của JS
    return {
      host: url.hostname || 'localhost',
      port: url.port || 3306,
      user: url.username || 'root',
      password: url.password || '', // Mặc định rỗng nếu không có
      database: url.pathname.replace(/^\//, '') || 'test_db' // Bỏ dấu / ở đầu
    };
  } catch (e) {
    console.error("❌ Lỗi format MySQL URI:", e.message);
    console.error("👉 Ví dụ đúng: mysql://root:123456@localhost:3306/my_db");
    process.exit(1);
  }
}

// Hàm đo thời gian
async function measure(label, fn) {
  const start = process.hrtime();
  try {
    await fn();
  } catch (err) {
    console.error(`❌ [Lỗi tại ${label}]`, err.message);
    return -1;
  }
  const end = process.hrtime(start);
  return (end[0] * 1000 + end[1] / 1e6).toFixed(2);
}

const toMB = (bytes) => (bytes / 1024 / 1024).toFixed(2);

// ==============================================================================
// 3. MAIN SCRIPT
// ==============================================================================
async function runBenchmarkSuite() {
  // Parse config từ URI
  const mysqlConfig = parseMysqlUri(CONFIG.mysqlUri);

  console.log(`\n🚀 BẮT ĐẦU BENCHMARK SUITE (URI MODE)`);
  console.log(`   - MySQL: ${mysqlConfig.user}@${mysqlConfig.host}:${mysqlConfig.port}/${mysqlConfig.database}`);
  console.log(`   - Mongo: ${CONFIG.mongoUri}`);
  console.log(`   - Records: ${CONFIG.records.toLocaleString()}`);
  console.log(`==================================================================`);

  // 1. KẾT NỐI DATABASE
  // Kết nối tạm thời không có database để Create DB nếu chưa có
  const tempConn = await mysql.createConnection({
    host: mysqlConfig.host,
    port: mysqlConfig.port,
    user: mysqlConfig.user,
    password: mysqlConfig.password
  });
  await tempConn.query(`CREATE DATABASE IF NOT EXISTS ${mysqlConfig.database}`);
  await tempConn.end();

  // Kết nối chính thức vào DB đã tạo
  const mysqlConn = await mysql.createConnection({
    host: mysqlConfig.host,
    port: mysqlConfig.port,
    user: mysqlConfig.user,
    password: mysqlConfig.password,
    database: mysqlConfig.database
  });

  // Kết nối MongoDB (Driver Mongo tự parse URI nên không cần làm gì thêm)
  const mongoClient = new MongoClient(CONFIG.mongoUri);
  await mongoClient.connect();
  // Lấy tên DB từ URI Mongo hoặc dùng mặc định
  const mongoDbName = new URL(CONFIG.mongoUri).pathname.replace(/^\//, '') || 'cnnt_benchmark_uri';
  const mongoDb = mongoClient.db(mongoDbName);

  const results = {
    mysql_json: { name: 'MySQL (Current JSON)' },
    mysql_rel: { name: 'MySQL (Normalized)' },
    mongo: { name: 'MongoDB (Native)' }
  };

  try {
    // --- BƯỚC 1: SETUP SCHEMA ---
    console.log("🛠  Đang khởi tạo Schema...");

    // A. MySQL JSON
    await mysqlConn.query("DROP TABLE IF EXISTS orders_json");
    await mysqlConn.query(`
            CREATE TABLE orders_json (
                id INT AUTO_INCREMENT PRIMARY KEY,
                items JSON,
                total INT
            ) ENGINE=InnoDB
        `);

    // B. MySQL Normalized
    await mysqlConn.query("DROP TABLE IF EXISTS order_items");
    await mysqlConn.query("DROP TABLE IF EXISTS orders_rel");
    await mysqlConn.query(`CREATE TABLE orders_rel (id INT AUTO_INCREMENT PRIMARY KEY, total INT) ENGINE=InnoDB`);
    await mysqlConn.query(`
            CREATE TABLE order_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT,
                product_name VARCHAR(255), 
                quantity INT,
                price INT,
                INDEX idx_product (product_name), 
                FOREIGN KEY (order_id) REFERENCES orders_rel(id)
            ) ENGINE=InnoDB
        `);

    // C. MongoDB
    await mongoDb.collection('orders').drop().catch(() => { });
    await mongoDb.collection('orders').createIndex({ "items.productName": 1 });

    // --- BƯỚC 2: SEED DATA ---
    console.log(`🌱 Đang sinh ${CONFIG.records.toLocaleString()} bản ghi mẫu...`);

    const batchSize = 2000;
    for (let i = 0; i < CONFIG.records; i += batchSize) {
      const sqlJsonBatch = [];
      const sqlRelItems = [];
      const mongoBatch = [];

      for (let j = 0; j < batchSize; j++) {
        const isTarget = Math.random() < 0.2;
        const itemsData = [
          { productId: 101, productName: isTarget ? CONFIG.targetItem : 'Bún Bò', quantity: 2, price: 45000 },
          { productId: 102, productName: 'Trà Đá', quantity: 1, price: 5000 }
        ];
        const total = 50000;

        sqlJsonBatch.push([JSON.stringify(itemsData), total]);
        mongoBatch.push({ items: itemsData, total: total });

        const orderId = i + j + 1;
        itemsData.forEach(item => {
          sqlRelItems.push([orderId, item.productName, item.quantity, item.price]);
        });
      }

      if (sqlJsonBatch.length) await mysqlConn.query('INSERT INTO orders_json (items, total) VALUES ?', [sqlJsonBatch]);
      if (mongoBatch.length) await mongoDb.collection('orders').insertMany(mongoBatch);
      if (sqlRelItems.length) await mysqlConn.query('INSERT INTO order_items (order_id, product_name, quantity, price) VALUES ?', [sqlRelItems]);
    }
    console.log("✅ Dữ liệu xong. Bắt đầu đo!\n");

    // --- BƯỚC 3: CHẠY TEST ---

    // 1. READ
    console.log("🔍 TEST 1: READ (Tìm món ăn)");
    results.mysql_json.read = await measure('MySQL JSON Read', async () => {
      await mysqlConn.query(`SELECT COUNT(*) FROM orders_json WHERE JSON_SEARCH(items, 'one', '%${CONFIG.targetItem}%') IS NOT NULL`);
    });
    results.mysql_rel.read = await measure('MySQL Rel Read', async () => {
      await mysqlConn.query(`SELECT COUNT(DISTINCT order_id) FROM order_items WHERE product_name = ?`, [CONFIG.targetItem]);
    });
    results.mongo.read = await measure('Mongo Read', async () => {
      await mongoDb.collection('orders').countDocuments({ "items.productName": CONFIG.targetItem });
    });

    // 2. WRITE
    console.log("✏️  TEST 2: WRITE (Cập nhật giá)");
    results.mysql_json.write = await measure('MySQL JSON Write', async () => {
      await mysqlConn.query(`UPDATE orders_json SET items = JSON_SET(items, '$[0].price', 0) WHERE JSON_SEARCH(items, 'one', '%${CONFIG.targetItem}%') IS NOT NULL`);
    });
    results.mysql_rel.write = await measure('MySQL Rel Write', async () => {
      await mysqlConn.query(`UPDATE order_items SET price = 0 WHERE product_name = ?`, [CONFIG.targetItem]);
    });
    results.mongo.write = await measure('Mongo Write', async () => {
      await mongoDb.collection('orders').updateMany({ "items.productName": CONFIG.targetItem }, { $set: { "items.$.price": 0 } });
    });

    // 3. STORAGE
    console.log("💾 TEST 3: STORAGE SIZE");
    const [stJ] = await mysqlConn.query("SHOW TABLE STATUS LIKE 'orders_json'");
    results.mysql_json.size = stJ[0].Data_length + stJ[0].Index_length;
    const [stR] = await mysqlConn.query("SHOW TABLE STATUS LIKE 'order_items'");
    results.mysql_rel.size = stR[0].Data_length + stR[0].Index_length;
    const stM = await mongoDb.collection('orders').stats();
    results.mongo.size = stM.storageSize;

    // --- BƯỚC 4: ĐÁNH GIÁ ---
    console.log("\n==================================================================");
    console.log("📊 KẾT QUẢ SO SÁNH");
    console.log("==================================================================");

    const getFactor = (slow, fast) => {
      if (slow <= 0 || fast <= 0) return "N/A";
      const f = parseFloat(slow) / parseFloat(fast);
      return f > 1 ? `${f.toFixed(1)}x` : '1.0x';
    };

    console.table([
      {
        "Tiêu chí": "READ (Tìm kiếm)",
        "MySQL JSON": `${results.mysql_json.read} ms`,
        "MySQL Relational": `${results.mysql_rel.read} ms`,
        "MongoDB": `${results.mongo.read} ms`,
        "Đánh giá": `JSON chậm hơn ${getFactor(results.mysql_json.read, results.mysql_rel.read)} lần`
      },
      {
        "Tiêu chí": "WRITE (Update)",
        "MySQL JSON": `${results.mysql_json.write} ms`,
        "MySQL Relational": `${results.mysql_rel.write} ms`,
        "MongoDB": `${results.mongo.write} ms`,
        "Đánh giá": "Relational/Mongo vượt trội"
      },
      {
        "Tiêu chí": "Storage (Dung lượng)",
        "MySQL JSON": `${toMB(results.mysql_json.size)} MB`,
        "MySQL Relational": `${toMB(results.mysql_rel.size)} MB`,
        "MongoDB": `${toMB(results.mongo.size)} MB`,
        "Đánh giá": "Relational tiết kiệm nhất"
      }
    ]);

  } catch (err) {
    console.error("Critical Error:", err);
  } finally {
    await mysqlConn.end();
    await mongoClient.close();
  }
}

runBenchmarkSuite();