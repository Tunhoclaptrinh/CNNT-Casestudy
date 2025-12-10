require('dotenv').config();
const { MongoClient } = require('mongodb');

// Tọa độ Hồ Gươm, Hà Nội
const HO_GUOM = [105.852174, 21.028511];

async function testGeo() {
  console.log("=== TEST 3: GEOSPATIAL QUERY (TÌM SHIPPER GẦN HỒ GƯƠM) ===");

  // Setup MongoDB Native
  const client = new MongoClient(process.env.DATABASE_URL);
  await client.connect();
  const db = client.db();
  const collection = db.collection('shippers_geo_test');

  // 1. Tạo dữ liệu mẫu (10 shipper rải rác quanh HN)
  await collection.deleteMany({}); // Clear cũ
  await collection.createIndex({ location: "2dsphere" }); // Bắt buộc tạo Index địa lý

  const shippers = [
    { name: "Shipper A (Gần - 500m)", location: { type: "Point", coordinates: [105.855, 21.029] } },
    { name: "Shipper B (Xa - 10km)", location: { type: "Point", coordinates: [105.780, 21.000] } },
  ];
  await collection.insertMany(shippers);

  // 2. Query tìm shipper trong bán kính 2km
  console.log("\n[MongoDB] Query: $near (tìm trong 2000 mét)...");
  const result = await collection.find({
    location: {
      $near: {
        $geometry: { type: "Point", coordinates: HO_GUOM },
        $maxDistance: 2000
      }
    }
  }).toArray();

  console.log("Kết quả tìm thấy:");
  result.forEach(s => console.log(` - ${s.name}`));

  console.log("\n[Nhận xét so với SQL]:");
  console.log(" - MongoDB: Hỗ trợ Native GeoJSON, cú pháp JSON dễ hiểu ($near).");
  console.log(" - MySQL/Postgres: Cần dùng hàm toán học (ST_Distance_Sphere) hoặc cài Extension phức tạp.");

  await client.close();
  process.exit(0);
}

testGeo();