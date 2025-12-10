require('dotenv').config();
const { MongoClient } = require('mongodb');
const colors = require('colors');

/**
 * HÀM CORE: Chạy Raw Aggregation Pipeline
 * Dùng để export cho các module khác hoặc chạy nội bộ
 */
async function runMongoAggregate(collectionName, pipeline) {
  let client;
  try {
    client = new MongoClient(process.env.DATABASE_URL || process.env.MONGO_URI);
    await client.connect();
    const db = client.db();

    const start = Date.now();

    // Kiểm tra nếu pipeline là array (Aggregate) hay object (Find)
    let result;
    if (Array.isArray(pipeline)) {
      result = await db.collection(collectionName).aggregate(pipeline).toArray();
    } else {
      // Fallback: Nếu truyền vào object query thường (Find)
      result = await db.collection(collectionName).find(pipeline).limit(20).toArray();
    }

    const time = Date.now() - start;

    return { time, result, count: result.length, client }; // Trả về client để close sau nếu cần
  } catch (e) {
    if (client) await client.close();
    throw e;
  }
}

// ============================================================
// CLI RUNNER (Chạy khi gọi trực tiếp: node helper-mongo-native.js ...)
// ============================================================
if (require.main === module) {
  (async () => {
    // 1. Lấy tham số dòng lệnh
    // Usage: node helper-mongo-native.js [collection] [pipeline_json]
    const args = process.argv.slice(2);

    // Mặc định: Lấy 5 users nếu không truyền tham số
    const collectionName = args[0] || 'users';
    const pipelineStr = args[1] || '[{ "$limit": 5 }]';

    console.log('\n╔════════════════════════════════════════════════════════╗');
    console.log('║           🍃 MONGO NATIVE RUNNER                       ║');
    console.log('╚════════════════════════════════════════════════════════╝');
    console.log(`\n📦 Collection: ${collectionName.green.bold}`);
    console.log(`🔍 Pipeline:   ${pipelineStr.cyan}`);

    let pipeline;
    try {
      // Xử lý JSON lỏng lẻo (cho phép quên dấu quote ở key nếu đơn giản - tuỳ chọn, ở đây dùng JSON.parse chuẩn)
      pipeline = JSON.parse(pipelineStr);
    } catch (e) {
      console.error('\n❌ Lỗi Parse JSON Pipeline:'.red, e.message);
      console.error('   Mẹo: Hãy bao quanh chuỗi JSON bằng dấu nháy đơn (\'). Ví dụ: \'[{"$count": "total"}]\'');
      process.exit(1);
    }

    try {
      console.log('\n⚡ Executing...');
      const { time, result, client } = await runMongoAggregate(collectionName, pipeline);

      console.log(`✅ Success in ${time}ms`);
      console.log(`📊 Documents returned: ${result.length}\n`);

      if (result.length > 0) {
        // Cắt ngắn bớt nếu object quá sâu để hiển thị bảng đẹp hơn
        const preview = result.map(doc => {
          const simpleDoc = { ...doc };
          // Convert ObjectId to string for display
          if (simpleDoc._id) simpleDoc._id = simpleDoc._id.toString();
          // Stringify nested objects
          Object.keys(simpleDoc).forEach(k => {
            if (typeof simpleDoc[k] === 'object' && simpleDoc[k] !== null) {
              simpleDoc[k] = JSON.stringify(simpleDoc[k]).substring(0, 50) + '...';
            }
          });
          return simpleDoc;
        });
        console.table(preview);
      } else {
        console.log('(No data returned)');
      }

      await client.close(); // Đóng kết nối sau khi chạy CLI xong
    } catch (error) {
      console.error('\n❌ Mongo Error:'.red, error.message);
    }
    process.exit(0);
  })();
}

// Export hàm để các file test khác vẫn dùng được
module.exports = { runMongoAggregate };