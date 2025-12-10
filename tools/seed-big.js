// tools/seed-big.js
require('dotenv').config();
// Import đúng Adapter dựa trên biến môi trường
const dbType = process.env.DB_CONNECTION;
const adapter = require(`../utils/${dbType === 'postgresql' ? 'PostgreSQL' : dbType === 'mysql' ? 'MySQL' : 'Mongo'}Adapter`);

async function run() {
  if (adapter.initConnection) await adapter.initConnection();
  console.log(`🚀 Bắt đầu bơm 50.000 users cho ${dbType}...`);

  const BATCH_SIZE = 1000;
  const TOTAL = 50000;
  const start = Date.now();

  for (let i = 0; i < TOTAL; i += BATCH_SIZE) {
    const users = Array.from({ length: BATCH_SIZE }).map((_, idx) => ({
      name: `User ${i + idx}`,
      email: `big_${Date.now()}_${i + idx}@test.com`,
      password: 'pass',
      role: 'customer',
      is_active: true
    }));

    // Gọi hàm insertMany (như bạn đã fix) hoặc create loop
    if (adapter.insertMany) {
      await adapter.insertMany('users', users);
    } else {
      // Fallback cho MySQL nếu chưa kịp viết insertMany
      await Promise.all(users.map(u => adapter.create('users', u)));
    }
    process.stdout.write(`.`);
  }

  console.log(`\n✅ Xong! Tổng thời gian: ${(Date.now() - start) / 1000}s`);
  process.exit(0);
}
run();