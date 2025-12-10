/**
 * 🧪 Enhanced Database Comparison Test Suite (Fixed & Optimized)
 * So sánh MySQL, PostgreSQL, MongoDB trên các tiêu chí cơ bản & nâng cao
 * * Cách chạy:
 * 1. Cấu hình DB_CONNECTION trong .env (mysql | postgresql | mongodb)
 * 2. Chạy: node tests/test-db-comparison.js
 */

require('dotenv').config();
const colors = require('colors');

// ==================== TEST DATA ====================
const TEST_DATA = {
  user: {
    name: 'Test User',
    email: `test_${Date.now()}@example.com`,
    password: 'Password123',
    phone: '0987654321',
    role: 'customer',
    isActive: true
  },
  category: {
    name: `Category_${Date.now()}`,
    description: 'This is a searchable description with special characters: àáảã'
  },
  orderItems: [
    {
      productId: 1,
      quantity: 2,
      price: 50000,
      finalPrice: 45000,
      itemTotal: 90000
    },
    {
      productId: 5,
      quantity: 1,
      price: 100000,
      discount: 10,
      finalPrice: 90000,
      itemTotal: 90000
    }
  ]
};

// ==================== NEW HELPERS FOR RESEARCH ====================
const generateBulkUsers = (count) => {
  return Array.from({ length: count }).map((_, i) => ({
    name: `Bulk User ${i}`,
    email: `bulk_${Date.now()}_${i}@test.com`,
    password: 'pass',
    phone: '0000000000',
    role: 'customer'
  }));
};

// ==================== HELPERS ====================
function log(msg, type = 'info') {
  const map = { info: 'cyan', success: 'green', error: 'red', warn: 'yellow', title: 'bold' };
  const symbol = { info: 'ℹ️', success: '✅', error: '❌', warn: '⚠️', title: '📊' };
  console.log(`${symbol[type] || ''} ${msg}`[map[type]] || msg);
}

function section(title) {
  console.log(`\n${'='.repeat(60)}`.gray);
  console.log(`  ${title}`.bold.white);
  console.log(`${'='.repeat(60)}`.gray);
}

async function delay(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ==================== TEST RESULTS TRACKER ====================
class TestResults {
  constructor(dbType) {
    this.dbType = dbType;
    this.tests = [];
    this.scores = {
      performance: 0,
      dataTypes: 0,
      features: 0,
      total: 0
    };
  }

  addTest(name, passed, time, score = 0, details = '') {
    this.tests.push({ name, passed, time, score, details });
    if (passed) this.scores.total += score;
  }

  getSummary() {
    const passed = this.tests.filter(t => t.passed).length;
    const total = this.tests.length;
    const totalTime = this.tests.reduce((sum, t) => sum + (t.time || 0), 0);

    return {
      dbType: this.dbType,
      passed,
      total,
      totalTime: Math.round(totalTime),
      score: this.scores.total,
      passRate: total > 0 ? Math.round((passed / total) * 100) : 0
    };
  }

  printSummary() {
    const summary = this.getSummary();
    console.log(`\n${'='.repeat(60)}`.cyan);
    console.log(`  SUMMARY - ${this.dbType.toUpperCase()}`.bold.white);
    console.log(`${'='.repeat(60)}`.cyan);
    console.log(`  Tests Passed: ${summary.passed}/${summary.total} (${summary.passRate}%)`);
    console.log(`  Total Time: ${summary.totalTime}ms`);
    console.log(`  Score: ${summary.score}/130`);
    console.log(`${'='.repeat(60)}`.cyan);

    // Print Table for Detail View
    console.table(this.tests.map(t => ({
      Test: t.name,
      Time: `${t.time}ms`,
      Status: t.passed ? 'PASS' : 'FAIL',
      Score: t.score,
      Note: t.details
    })));
  }
}

// ==================== DATABASE TESTER ====================
class DatabaseTester {
  constructor(dbType) {
    this.dbType = dbType;
    this.db = null;
    this.results = new TestResults(dbType);
    this.testIds = {
      userId: null,
      categoryId: null,
      orderId: null
    };
    // Track IDs for new bulk tests
    this.bulkUserIds = [];
    this.aggOrderIds = [];
  }

  async connect() {
    try {
      process.env.DB_CONNECTION = this.dbType;

      // Clear cache to reload config
      ['../config/database', '../utils/MySQLAdapter', '../utils/PostgreSQLAdapter', '../utils/MongoAdapter']
        .forEach(mod => {
          try { delete require.cache[require.resolve(mod)]; } catch (e) { }
        });

      this.db = require('../config/database');

      // Warm up connection
      if (this.dbType !== 'json') await delay(1000);

      return true;
    } catch (e) {
      log(`Connection failed: ${e.message}`, 'error');
      return false;
    }
  }

  async run() {
    console.log(`\n${'█'.repeat(62)}`.bold.cyan);
    console.log(`  TESTING: ${this.dbType.toUpperCase()}`.bold.white);
    console.log(`${'█'.repeat(62)}`.bold.cyan);

    // Run original tests
    await this.test1_CreatePerformance();
    await this.test2_ReadPerformance();
    await this.test3_UpdatePerformance();
    await this.test4_CaseSensitivity();
    await this.test5_JSONHandling();
    await this.test6_FullTextSearch();
    await this.test7_ConcurrentWrites();
    await this.test8_ComplexQuery();
    await this.test9_TransactionSupport();
    // await this.test9_1_TransactionSupport();
    await this.test10_DataIntegrity();

    // Run NEW Research tests
    await this.test11_BulkInsertPerformance();
    await this.test12_AggregationPerformance();
    await this.test13_RelationQueryPerformance();

    // Cleanup
    await this.cleanup();

    // Print summary
    this.results.printSummary();
  }

  // ==================== TEST 1: CREATE PERFORMANCE ====================
  async test1_CreatePerformance() {
    section('TEST 1: Create Performance');

    try {
      const start = Date.now();
      const user = await this.db.create('users', TEST_DATA.user);
      const time = Date.now() - start;

      if (!user || !user.id) throw new Error("No ID returned");

      this.testIds.userId = user.id;
      const idType = typeof user.id;

      // Score: Fast create = high score
      const score = time < 50 ? 10 : time < 100 ? 8 : time < 200 ? 5 : 3;

      this.results.addTest('Create User', true, time, score, `ID Type: ${idType}`);
      log(`Create User: ${time}ms | ID Type: ${idType} | Score: ${score}/10`, 'success');
    } catch (e) {
      this.results.addTest('Create User', false, 0, 0, e.message);
      log(`Create User Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 2: READ PERFORMANCE ====================
  async test2_ReadPerformance() {
    section('TEST 2: Read Performance');

    try {
      const start = Date.now();
      const user = await this.db.findById('users', this.testIds.userId);
      const time = Date.now() - start;

      if (!user) throw new Error("User not found");

      const score = time < 20 ? 10 : time < 50 ? 8 : time < 100 ? 5 : 3;

      this.results.addTest('Read by ID', true, time, score);
      log(`Read by ID: ${time}ms | Score: ${score}/10`, 'success');
    } catch (e) {
      this.results.addTest('Read by ID', false, 0, 0, e.message);
      log(`Read Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 3: UPDATE PERFORMANCE ====================
  async test3_UpdatePerformance() {
    section('TEST 3: Update Performance');

    try {
      const start = Date.now();
      const updated = await this.db.update('users', this.testIds.userId, {
        name: 'Updated User',
        phone: '0999999999'
      });
      const time = Date.now() - start;

      if (!updated || updated.name !== 'Updated User') {
        throw new Error("Update failed");
      }

      const score = time < 30 ? 10 : time < 70 ? 8 : time < 150 ? 5 : 3;

      this.results.addTest('Update User', true, time, score);
      log(`Update User: ${time}ms | Score: ${score}/10`, 'success');
    } catch (e) {
      this.results.addTest('Update User', false, 0, 0, e.message);
      log(`Update Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 4: CASE SENSITIVITY ====================
  async test4_CaseSensitivity() {
    section('TEST 4: Case Sensitivity');

    try {
      const upperEmail = TEST_DATA.user.email.toUpperCase();
      const found = await this.db.findOne('users', { email: upperEmail });

      if (found) {
        this.results.addTest('Case Sensitivity', true, 0, 5, 'Case-Insensitive');
        log(`Case Sensitivity: Case-Insensitive ⚠️`, 'warn');
      } else {
        this.results.addTest('Case Sensitivity', true, 0, 10, 'Case-Sensitive');
        log(`Case Sensitivity: Case-Sensitive ✓`, 'success');
      }
    } catch (e) {
      this.results.addTest('Case Sensitivity', false, 0, 0, e.message);
      log(`Case Test Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 5: JSON HANDLING ====================
  async test5_JSONHandling() {
    section('TEST 5: JSON/Complex Data Handling');

    try {
      const orderData = {
        userId: this.testIds.userId,
        restaurantId: 1,
        items: TEST_DATA.orderItems,
        subtotal: 180000,
        deliveryFee: 15000,
        total: 195000,
        status: 'pending',
        paymentMethod: 'cash',
        deliveryAddress: 'Test Address'
      };

      const order = await this.db.create('orders', orderData);
      this.testIds.orderId = order.id;

      const fetched = await this.db.findById('orders', order.id);

      // Check if items is still an array
      const isArray = Array.isArray(fetched.items);

      // Deep check: Can we access nested properties?
      let isDeepEqual = false;
      if (isArray && fetched.items[1] && fetched.items[1].discount) {
        isDeepEqual = fetched.items[1].discount === 10;
      }

      if (isArray && isDeepEqual) {
        this.results.addTest('JSON Handling', true, 0, 10, 'Perfect (Array + Deep Props)');
        log(`JSON Handling: Perfect ✓ (Array preserved, nested props accessible)`, 'success');
      } else if (isArray) {
        this.results.addTest('JSON Handling', true, 0, 7, 'Good (Array preserved)');
        log(`JSON Handling: Good ⚠️ (Array preserved but nested access issue)`, 'warn');
      } else {
        this.results.addTest('JSON Handling', false, 0, 3, `Got type: ${typeof fetched.items}`);
        log(`JSON Handling: Poor ✗ (Converted to ${typeof fetched.items})`, 'error');
      }
    } catch (e) {
      this.results.addTest('JSON Handling', false, 0, 0, e.message);
      log(`JSON Test Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 6: FULL-TEXT SEARCH ====================
  async test6_FullTextSearch() {
    section('TEST 6: Full-Text Search');

    try {
      // Create test category
      const cat = await this.db.create('categories', TEST_DATA.category);
      this.testIds.categoryId = cat.id;

      const start = Date.now();
      const results = await this.db.findAllAdvanced('categories', {
        q: 'searchable'
      });
      const time = Date.now() - start;

      const found = results.data.find(c => c.id === cat.id);

      if (found) {
        const score = time < 50 ? 10 : time < 100 ? 8 : time < 200 ? 5 : 3;
        this.results.addTest('Full-Text Search', true, time, score);
        log(`Full-Text Search: Working ✓ (${time}ms) | Score: ${score}/10`, 'success');
      } else {
        this.results.addTest('Full-Text Search', false, time, 3, 'Record not found');
        log(`Full-Text Search: Not Working ✗`, 'error');
      }
    } catch (e) {
      this.results.addTest('Full-Text Search', false, 0, 0, e.message);
      log(`Search Test Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 7: CONCURRENT WRITES ====================
  async test7_ConcurrentWrites() {
    section('TEST 7: Concurrent Write Performance');

    try {
      const promises = [];
      const start = Date.now();

      for (let i = 0; i < 10; i++) {
        promises.push(
          this.db.create('categories', {
            name: `Concurrent_${Date.now()}_${i}`,
            description: `Test concurrent write ${i}`
          })
        );
      }

      const results = await Promise.all(promises);
      const time = Date.now() - start;

      // Check if all succeeded
      const allSuccess = results.every(r => r && r.id);

      if (allSuccess) {
        const score = time < 200 ? 10 : time < 500 ? 7 : time < 1000 ? 4 : 2;
        this.results.addTest('Concurrent Writes', true, time, score, '10 parallel writes');
        log(`Concurrent Writes: Success ✓ (${time}ms for 10 writes) | Score: ${score}/10`, 'success');

        // Cleanup
        await Promise.all(results.map(r => this.db.delete('categories', r.id)));
      } else {
        throw new Error("Some writes failed");
      }
    } catch (e) {
      this.results.addTest('Concurrent Writes', false, 0, 0, e.message);
      log(`Concurrent Writes Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 8: COMPLEX QUERY ====================
  async test8_ComplexQuery() {
    section('TEST 8: Complex Query (Filter + Sort + Pagination)');

    try {
      const start = Date.now();
      const results = await this.db.findAllAdvanced('categories', {
        filter: {
          name_like: 'Category'
        },
        sort: 'createdAt',
        order: 'desc',
        page: 1,
        limit: 5
      });
      const time = Date.now() - start;

      if (results && results.data && results.pagination) {
        const score = time < 100 ? 10 : time < 200 ? 7 : time < 400 ? 4 : 2;
        this.results.addTest('Complex Query', true, time, score,
          `Found ${results.data.length} items`);
        log(`Complex Query: Success ✓ (${time}ms) | Found: ${results.data.length} | Score: ${score}/10`, 'success');
      } else {
        throw new Error("Invalid result structure");
      }
    } catch (e) {
      this.results.addTest('Complex Query', false, 0, 0, e.message);
      log(`Complex Query Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 9: TRANSACTION SUPPORT ====================
  async test9_TransactionSupport() {
    section('TEST 9: Transaction/Consistency Support');

    try {
      // Simulate transaction: Create order + Update product
      const product = await this.db.create('products', {
        name: 'Test Product',
        restaurantId: 1,
        price: 50000,
        available: true,
        discount: 0
      });

      // Try to create order and update product atomically
      const order = await this.db.create('orders', {
        userId: this.testIds.userId,
        restaurantId: 1,
        items: [{ productId: product.id, quantity: 1, price: 50000 }],
        subtotal: 50000,
        deliveryFee: 15000,
        total: 65000,
        status: 'pending',
        paymentMethod: 'cash',
        deliveryAddress: 'Test'
      });

      // Verify both exist
      const fetchedOrder = await this.db.findById('orders', order.id);
      const fetchedProduct = await this.db.findById('products', product.id);

      if (fetchedOrder && fetchedProduct) {
        this.results.addTest('Transaction Support', true, 0, 5,
          'Basic consistency maintained');
        log(`Transaction Support: Basic ⚠️ (No native transactions, but data consistent)`, 'warn');
      } else {
        throw new Error("Data inconsistency detected");
      }

      // Cleanup
      await this.db.delete('orders', order.id);
      await this.db.delete('products', product.id);
    } catch (e) {
      this.results.addTest('Transaction Support', false, 0, 0, e.message);
      log(`Transaction Test Failed: ${e.message}`, 'error');
    }
  }

  // async test9_1_TransactionSupport() {
  //   section('TEST 9.1: Transaction Rollback Check');

  //   // 1. Tạo dữ liệu mẫu
  //   const testId = Date.now();
  //   let productId = null;

  //   try {
  //     // --- BẮT ĐẦU TRANSACTION (Giả định DB Adapter có hàm này) ---
  //     // Nếu DB Adapter không có startTransaction, coi như Fail luôn.
  //     if (typeof this.db.startTransaction === 'function') {
  //       await this.db.startTransaction();
  //     }

  //     // Bước 1: Tạo Product (Thành công)
  //     const product = await this.db.create('products', {
  //       name: `Rollback Test ${testId}`,
  //       price: 100
  //     });
  //     productId = product.id;
  //     log(`Step 1: Created Product ${productId}`, 'info');

  //     // Bước 2: CỐ TÌNH GÂY LỖI
  //     throw new Error("🔥 GIẢ LẬP LỖI SERVER 🔥");

  //     // Bước 3: Tạo Order (Sẽ không bao giờ chạy tới đây)
  //     await this.db.create('orders', { productId: productId });

  //     // Commit (Sẽ không chạy tới đây)
  //     if (typeof this.db.commit === 'function') await this.db.commit();

  //   } catch (e) {
  //     log(`Caught expected error: ${e.message}`, 'info');

  //     // --- ROLLBACK (Quan trọng nhất) ---
  //     if (typeof this.db.rollback === 'function') {
  //       await this.db.rollback();
  //       log('Executed Rollback command', 'info');
  //     }
  //   }

  //   // --- KIỂM TRA KẾT QUẢ ---
  //   // Tìm xem Product lúc nãy tạo có còn tồn tại không?
  //   const checkProduct = await this.db.findById('products', productId);

  //   if (!checkProduct) {
  //     // Product KHÔNG tìm thấy => Đã Rollback thành công!
  //     this.results.addTest('Transaction Support', true, 0, 10, 'Rollback worked perfectly');
  //     log('Transaction: PASSED (Data was rolled back correctly)', 'success');
  //   } else {
  //     // Product VẪN CÒN => Không hỗ trợ Transaction
  //     this.results.addTest('Transaction Support', false, 0, 0, 'No Rollback detected');
  //     log('Transaction: FAILED (Data remains despite error)', 'error');

  //     // Dọn rác thủ công vì rollback thất bại
  //     await this.db.delete('products', productId);
  //   }
  // }

  // ==================== TEST 10: DATA INTEGRITY ====================

  async test10_DataIntegrity() {
    section('TEST 10: Data Type Integrity');

    try {
      // Create user with various data types
      const testUser = await this.db.create('users', {
        name: 'Integrity Test',
        email: `integrity_${Date.now()}@test.com`,
        password: 'test123',
        phone: '0123456789',
        isActive: true, // Boolean
        role: 'customer'
      });

      const fetched = await this.db.findById('users', testUser.id);

      // Check data types
      const checks = {
        boolean: (typeof fetched.isActive === 'boolean' || fetched.isActive === 'true' || fetched.isActive === 1),
        string: typeof fetched.name === 'string',
        number: typeof fetched.id === 'number',
        date: fetched.createdAt && !isNaN(new Date(fetched.createdAt))
      };

      const allCorrect = Object.values(checks).every(v => v);

      if (allCorrect) {
        this.results.addTest('Data Integrity', true, 0, 10,
          'All types preserved correctly');
        log(`Data Integrity: Perfect ✓ (Boolean, String, Number, Date all correct)`, 'success');
      } else {
        const failed = Object.entries(checks)
          .filter(([k, v]) => !v)
          .map(([k]) => k)
          .join(', ');
        this.results.addTest('Data Integrity', false, 0, 5,
          `Issues with: ${failed}`);
        log(`Data Integrity: Issues ⚠️ (Problems with: ${failed})`, 'warn');
      }

      // Cleanup
      await this.db.delete('users', testUser.id);
    } catch (e) {
      this.results.addTest('Data Integrity', false, 0, 0, e.message);
      log(`Data Integrity Test Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 11: BULK INSERT PERFORMANCE ====================
  async test11_BulkInsertPerformance() {
    section('TEST 11: Bulk Insert Performance (Throughput)');
    const COUNT = 100;
    try {
      const users = generateBulkUsers(COUNT);
      const start = Date.now();

      // Execute 100 inserts concurrently
      let results;
      // Kiểm tra xem Adapter có hỗ trợ hàm insertMany không (Ghi 1 lần)
      if (typeof this.db.insertMany === 'function') {
        results = await this.db.insertMany('users', users);
      } else {
        // Fallback: Nếu không có insertMany thì đành phải ghi từng cái (Chậm)
        results = await Promise.all(users.map(u => this.db.create('users', u)));
      }

      const time = Date.now() - start;

      // Verify and store IDs for cleanup
      const successful = results.filter(r => r && r.id);
      this.bulkUserIds = successful.map(r => r.id);

      if (successful.length === COUNT) {
        // Score: < 1s for 100 records is excellent
        const score = time < 1000 ? 10 : time < 3000 ? 7 : 4;
        const throughput = Math.round((COUNT / time) * 1000);

        this.results.addTest(`Bulk Insert (${COUNT})`, true, time, score, `${throughput} req/sec`);
        log(`Bulk Insert: Success ✓ (${time}ms) | Throughput: ${throughput} req/s | Score: ${score}/10`, 'success');
      } else {
        throw new Error(`Only created ${successful.length}/${COUNT} users`);
      }
    } catch (e) {
      this.results.addTest('Bulk Insert', false, 0, 0, e.message);
      log(`Bulk Insert Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 12: AGGREGATION PERFORMANCE (FIXED) ====================
  async test12_AggregationPerformance() {
    section('TEST 12: Aggregation/Calculation Performance');
    // Scenario: Calculate total revenue from 50 new orders
    const ORDER_COUNT = 50;
    const ORDER_VALUE = 100000;

    try {
      // Setup data with subtotal/deliveryFee to satisfy MySQL constraints
      const orderPromises = Array.from({ length: ORDER_COUNT }).map((_, i) => ({
        userId: this.testIds.userId,
        subtotal: ORDER_VALUE, // REQUIRED by MySQL schema
        deliveryFee: 0,        // REQUIRED by MySQL schema
        restaurantId: 1,              //(Fix lỗi validation)
        paymentMethod: 'cash',        // (Fix lỗi validation)
        total: ORDER_VALUE,
        status: 'completed',
        deliveryAddress: `Agg Test ${i}`,
        items: [] // Will be stringified to "[]" by Adapter
      }));

      // Execute creates
      const createdOrders = await Promise.all(orderPromises.map(o => this.db.create('orders', o)));

      // Filter valid orders and track IDs
      const validOrders = createdOrders.filter(o => o && o.id);
      this.aggOrderIds = validOrders.map(o => o.id);

      if (validOrders.length !== ORDER_COUNT) {
        log(`Warning: Only created ${validOrders.length}/${ORDER_COUNT} orders for aggregation test`, 'warn');
      }

      const start = Date.now();

      // Simulating aggregation: Fetch All + Reduce in Memory 
      const allOrders = await this.db.findAll('orders');
      const revenue = allOrders
        .filter(o => this.aggOrderIds.includes(o.id))
        .reduce((sum, o) => sum + (o.total || 0), 0);

      const time = Date.now() - start;

      const expectedRevenue = validOrders.length * ORDER_VALUE;
      if (revenue === expectedRevenue && validOrders.length > 0) {
        const score = time < 100 ? 10 : time < 300 ? 7 : 4;
        this.results.addTest('Aggregation', true, time, score, `Sum verified: ${revenue}`);
        log(`Aggregation: Correct ✓ (${time}ms) | Revenue: ${revenue} | Score: ${score}/10`, 'success');
      } else {
        throw new Error(`Revenue mismatch: got ${revenue}, expected ${expectedRevenue}`);
      }
    } catch (e) {
      this.results.addTest('Aggregation', false, 0, 0, e.message);
      log(`Aggregation Failed: ${e.message}`, 'error');
    }
  }

  // ==================== TEST 13: RELATION QUERY PERFORMANCE ====================
  async test13_RelationQueryPerformance() {
    section('TEST 13: Relation/Join Performance');
    // Scenario: Fetch orders and expand User information (Join/Lookup)

    try {
      const start = Date.now();

      // Use findAllAdvanced with 'expand' option supported by adapters
      // We use the orders created in previous tests or Test 5
      const results = await this.db.findAllAdvanced('orders', {
        limit: 20,
        expand: 'user', // Requests the adapter to join/populate user data
        sort: 'createdAt',
        order: 'desc'
      });

      const time = Date.now() - start;

      if (results && results.data) {
        // Check if user expansion happened
        const expandedItems = results.data.filter(item => item.user && item.user.email);
        const expandRate = expandedItems.length;

        if (expandRate > 0) {
          const score = time < 100 ? 10 : time < 300 ? 7 : 4;
          this.results.addTest('Relation Query', true, time, score, `Expanded ${expandRate} items`);
          log(`Relation Query: Success ✓ (${time}ms) | Joined User Data | Score: ${score}/10`, 'success');
        } else {
          // Some adapters might not implement 'expand' or data missing
          log(`Relation Query: Warning - User data not expanded (Adapter limitation?)`, 'warn');
          this.results.addTest('Relation Query', true, time, 5, 'No expansion detected');
        }
      } else {
        throw new Error("Invalid result from findAllAdvanced");
      }
    } catch (e) {
      this.results.addTest('Relation Query', false, 0, 0, e.message);
      log(`Relation Query Failed: ${e.message}`, 'error');
    }
  }

  // ==================== CLEANUP ====================
  async cleanup() {
    section('Cleanup');
    try {
      // 1. Xóa bảng CON trước (Orders)
      if (this.testIds.orderId) {
        await this.db.delete('orders', this.testIds.orderId);
        log('Deleted test order', 'info');
      }
      if (this.aggOrderIds.length > 0) {
        // Xóa batch nhỏ để tránh quá tải connection
        const chunkSize = 20;
        for (let i = 0; i < this.aggOrderIds.length; i += chunkSize) {
          const chunk = this.aggOrderIds.slice(i, i + chunkSize);
          await Promise.all(chunk.map(id => this.db.delete('orders', id)));
        }
        log(`Deleted ${this.aggOrderIds.length} aggregation orders`, 'info');
      }

      // 2. Xóa bảng CHA sau (Users, Categories)
      if (this.testIds.userId) {
        await this.db.delete('users', this.testIds.userId);
        log('Deleted test user', 'info');
      }
      if (this.bulkUserIds.length > 0) {
        const chunkSize = 20;
        for (let i = 0; i < this.bulkUserIds.length; i += chunkSize) {
          const chunk = this.bulkUserIds.slice(i, i + chunkSize);
          await Promise.all(chunk.map(id => this.db.delete('users', id)));
        }
        log(`Deleted ${this.bulkUserIds.length} bulk users`, 'info');
      }

      if (this.testIds.categoryId) {
        await this.db.delete('categories', this.testIds.categoryId);
        log('Deleted test category', 'info');
      }

      if (this.db && typeof this.db.close === 'function') {
        await this.db.close();
      }
    } catch (e) {
      log(`Cleanup warning: ${e.message}`, 'warn');
    }
  }
}

// ==================== MAIN EXECUTION ====================
async function main() {
  const dbType = process.env.DB_CONNECTION || 'json';

  console.log('\n' + '█'.repeat(62).bold.cyan);
  console.log('  DATABASE COMPARISON TEST SUITE'.bold.white);
  console.log('  Testing: ' + dbType.toUpperCase().bold.yellow);
  console.log('█'.repeat(62).bold.cyan);

  if (dbType === 'mongodb' && !process.env.DATABASE_URL) {
    log('Skipping MongoDB (Missing DATABASE_URL in .env)', 'warn');
    process.exit(0);
  }

  if (dbType === 'json') {
    log('JSON adapter detected. Some tests may not be meaningful for file-based storage.', 'warn');
  }

  const tester = new DatabaseTester(dbType);

  if (await tester.connect()) {
    await tester.run();
  }

  console.log('\n✨ Test Complete!\n');
  process.exit(0);
}

main();