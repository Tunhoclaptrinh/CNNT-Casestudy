require('dotenv').config();
const { MongoClient } = require('mongodb');

// Hàm chạy Raw Aggregation Pipeline
async function runMongoAggregate(collectionName, pipeline) {
  const client = new MongoClient(process.env.DATABASE_URL);
  try {
    await client.connect();
    const db = client.db(); // Lấy DB từ connection string

    const start = Date.now();
    // Chạy lệnh native, trả về array thuần (không qua Mongoose hydration)
    const result = await db.collection(collectionName).aggregate(pipeline).toArray();
    const time = Date.now() - start;

    return { time, result, count: result.length };
  } catch (e) {
    console.error("Mongo Native Error:", e);
    return { time: 0, result: [], error: e.message };
  } finally {
    await client.close();
  }
}

module.exports = { runMongoAggregate };