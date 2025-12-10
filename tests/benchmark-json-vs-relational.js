/**
 * FILE: tests/benchmark-sute-3.js (FIXED COLLSTATS)
 * FIX: Thay mongoCol.stats() bằng mongoDb.command({ collStats: ... })
 */
require('dotenv').config();

const { MongoClient } = require('mongodb');
const mysqlAdapter = require('../utils/MySQLAdapter');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/cnnt_benchmark_uri';
const RECORDS = 50000;
const TARGET_ITEM = 'Cơm Tấm Sườn Bì';

async function runBenchmarkSuite() {
  console.log(`\n🚀 BẮT ĐẦU BENCHMARK (${RECORDS.toLocaleString()} records)`);
  console.log(`-------------------------------------------------------------`);

  // 1. KẾT NỐI
  await mysqlAdapter.initConnection();
  const mysqlPool = mysqlAdapter.pool;

  const mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  const mongoDb = mongoClient.db();

  const measure = async (label, fn) => {
    const start = process.hrtime();
    try {
      await fn();
    } catch (e) {
      console.error(`❌ Lỗi tại ${label}:`, e.message);
      return -1;
    }
    const end = process.hrtime(start);
    return (end[0] * 1000 + end[1] / 1e6).toFixed(2);
  };

  const toMB = (bytes) => (bytes / 1024 / 1024).toFixed(2);

  try {
    console.log("🛠  Đang khởi tạo Schema...");

    // A. MySQL
    await mysqlPool.query("DROP TABLE IF EXISTS bench_orders_json");
    await mysqlPool.query(`CREATE TABLE bench_orders_json (id INT AUTO_INCREMENT PRIMARY KEY, items JSON, total INT)`);

    await mysqlPool.query("DROP TABLE IF EXISTS bench_order_items");
    await mysqlPool.query("DROP TABLE IF EXISTS bench_orders_rel");
    await mysqlPool.query(`CREATE TABLE bench_orders_rel (id INT AUTO_INCREMENT PRIMARY KEY, total INT)`);
    await mysqlPool.query(`
            CREATE TABLE bench_order_items (
                id INT AUTO_INCREMENT PRIMARY KEY, 
                order_id INT, 
                product_name VARCHAR(255), 
                quantity INT, price INT, 
                INDEX idx_bench_product (product_name)
            )
        `);

    // B. Mongo
    const mongoCol = mongoDb.collection('bench_orders');
    await mongoCol.drop().catch(() => { });
    await mongoCol.createIndex({ "items.productName": 1 });

    // --- SEED DATA ---
    console.log(`🌱 Đang sinh ${RECORDS.toLocaleString()} bản ghi mẫu...`);
    const batchSize = 2000;

    for (let i = 0; i < RECORDS; i += batchSize) {
      const sqlJson = [];
      const sqlRelItems = [];
      const mongoDocs = [];

      for (let j = 0; j < batchSize; j++) {
        const isTarget = Math.random() < 0.2;
        const items = [
          { productName: isTarget ? TARGET_ITEM : 'Phở Bò', quantity: 2, price: 45000 },
          { productName: 'Trà Đá', quantity: 1, price: 5000 }
        ];

        sqlJson.push([JSON.stringify(items), 50000]);
        mongoDocs.push({ items, total: 50000 });

        const orderId = i + j + 1;
        items.forEach(item => {
          sqlRelItems.push([orderId, item.productName, item.quantity, item.price]);
        });
      }

      if (sqlJson.length) await mysqlPool.query('INSERT INTO bench_orders_json (items, total) VALUES ?', [sqlJson]);
      if (sqlRelItems.length) await mysqlPool.query('INSERT INTO bench_order_items (order_id, product_name, quantity, price) VALUES ?', [sqlRelItems]);
      if (mongoDocs.length) await mongoCol.insertMany(mongoDocs);
    }

    console.log("✅ Dữ liệu xong. Bắt đầu đo!\n");

    const results = { mysql_json: {}, mysql_rel: {}, mongo: {} };

    // 1. READ
    console.log("🔍 TEST 1: READ (Tìm kiếm)");
    results.mysql_json.read = await measure('MySQL JSON', async () => {
      await mysqlPool.query(`SELECT COUNT(*) FROM bench_orders_json WHERE JSON_SEARCH(items, 'one', '%${TARGET_ITEM}%') IS NOT NULL`);
    });
    results.mysql_rel.read = await measure('MySQL Relational', async () => {
      await mysqlPool.query(`SELECT COUNT(DISTINCT order_id) FROM bench_order_items WHERE product_name = ?`, [TARGET_ITEM]);
    });
    results.mongo.read = await measure('MongoDB', async () => {
      await mongoCol.countDocuments({ "items.productName": TARGET_ITEM });
    });

    // 2. WRITE
    console.log("✏️  TEST 2: WRITE (Update giá)");
    results.mysql_json.write = await measure('MySQL JSON Update', async () => {
      await mysqlPool.query(`UPDATE bench_orders_json SET items = JSON_SET(items, '$[0].price', 0) WHERE JSON_SEARCH(items, 'one', '%${TARGET_ITEM}%') IS NOT NULL`);
    });
    results.mysql_rel.write = await measure('MySQL Relational Update', async () => {
      await mysqlPool.query(`UPDATE bench_order_items SET price = 0 WHERE product_name = ?`, [TARGET_ITEM]);
    });
    results.mongo.write = await measure('MongoDB Update', async () => {
      await mongoCol.updateMany({ "items.productName": TARGET_ITEM }, { $set: { "items.$.price": 0 } });
    });

    // 3. STORAGE
    console.log("💾 TEST 3: STORAGE SIZE");
    const [stJ] = await mysqlPool.query("SHOW TABLE STATUS LIKE 'bench_orders_json'");
    results.mysql_json.size = stJ[0].Data_length + stJ[0].Index_length;

    const [stR] = await mysqlPool.query("SHOW TABLE STATUS LIKE 'bench_order_items'");
    results.mysql_rel.size = stR[0].Data_length + stR[0].Index_length;

    // --- FIX LỖI TẠI ĐÂY: Dùng mongoDb.command thay vì mongoCol.stats() ---
    const stM = await mongoDb.command({ collStats: 'bench_orders' });
    results.mongo.size = stM.storageSize;

    // --- KẾT QUẢ ---
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
        "Đánh giá": "Relational tiết kiệm nhất (SQL)"
      }
    ]);



  } catch (err) {
    console.error("Critical Error:", err);
  } finally {
    await mysqlAdapter.close();
    await mongoClient.close();
  }
}

runBenchmarkSuite();