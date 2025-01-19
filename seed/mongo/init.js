// Switch to the BenchmarkDB database (it will be created if it doesn't exist)
db = db.getSiblingDB("BenchmarkDB");

// Drop existing collections if they exist to start fresh
db.Customers.drop();
db.Products.drop();
db.Orders.drop();

// ================================
// Step 1: Setup and Collection Creation
// ================================

// Create Customers Collection with Schema Validation
db.createCollection("Customers", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["firstName", "lastName", "email", "createdDate"],
      properties: {
        firstName: {
          bsonType: "string",
          description: "must be a string and is required"
        },
        lastName: {
          bsonType: "string",
          description: "must be a string and is required"
        },
        email: {
          bsonType: "string",
          pattern: "^.+@.+\\..+$",
          description: "must be a valid email and is required"
        },
        createdDate: {
          bsonType: "date",
          description: "must be a date and is required"
        }
      }
    }
  }
});

// Create Products Collection with Schema Validation
db.createCollection("Products", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["productName", "price", "createdDate"],
      properties: {
        productName: {
          bsonType: "string",
          description: "must be a string and is required"
        },
        price: {
          bsonType: "number",
          minimum: 0,
          description: "must be a non-negative number and is required"
        },
        createdDate: {
          bsonType: "date",
          description: "must be a date and is required"
        }
      }
    }
  }
});

// Create Orders Collection with Schema Validation
db.createCollection("Orders", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["customerId", "orderDate", "orderDetails"],
      properties: {
        customerId: {
          bsonType: "objectId",
          description: "must be an ObjectId and is required"
        },
        orderDate: {
          bsonType: "date",
          description: "must be a date and is required"
        },
        orderDetails: {
          bsonType: "array",
          minItems: 1,
          items: {
            bsonType: "object",
            required: ["productId", "quantity", "unitPrice"],
            properties: {
              productId: {
                bsonType: "objectId",
                description: "must be an ObjectId and is required"
              },
              quantity: {
                bsonType: "int",
                minimum: 1,
                description: "must be a positive integer and is required"
              },
              unitPrice: {
                bsonType: "number",
                minimum: 0,
                description: "must be a non-negative number and is required"
              }
            }
          }
        }
      }
    }
  }
});

print("Collections created with schema validation.");

// ================================
// Step 2: Index Creation
// ================================

// Customers: Unique index on email
db.Customers.createIndex({ email: 1 }, { unique: true });
print("Index created on Customers.email");

// Products: Index on productName
db.Products.createIndex({ productName: 1 });
print("Index created on Products.productName");

// Orders: Compound index on customerId and orderDate
db.Orders.createIndex({ customerId: 1, orderDate: 1 });
print("Compound index created on Orders.customerId and Orders.orderDate");

// Orders.orderDetails.productId index
db.Orders.createIndex({ "orderDetails.productId": 1 });
print("Index created on Orders.orderDetails.productId");

print("All indexes created successfully.");

// ================================
// Step 3: Data Seeding
// ================================

// Utility Functions

// Generate a random date within the past year
function getRandomDate() {
  const now = new Date();
  const past = new Date(now.getFullYear() - 1, now.getMonth(), now.getDate());
  return new Date(past.getTime() + Math.random() * (now.getTime() - past.getTime()));
}

// Generate a random price between 1 and 100
function getRandomPrice() {
  return parseFloat((Math.random() * 100 + 1).toFixed(2));
}

// Generate a random integer between min and max (inclusive)
function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// ----
// 3a. Seed Customers
// ----
const customerCount = 100000;
const customerBatchSize = 1000;
print("Seeding Customers...");
let customersBulk = db.Customers.initializeUnorderedBulkOp();
for (let i = 1; i <= customerCount; i++) {
  customersBulk.insert({
    firstName: "FirstName" + i,
    lastName: "LastName" + i,
    email: "customer" + i + "@example.com",
    createdDate: getRandomDate()
  });

  if (i % customerBatchSize === 0) {
    customersBulk.execute();
    customersBulk = db.Customers.initializeUnorderedBulkOp();
    if (i % 10000 === 0) {
      print(`Inserted ${i} customers...`);
    }
  }
}
if (customersBulk.length > 0) {
  customersBulk.execute();
  print(`Inserted ${customerCount} customers.`);
}

// ----
// 3b. Seed Products
// ----
const productCount = 1000;
const productBatchSize = 1000;
print("Seeding Products...");
let productsBulk = db.Products.initializeUnorderedBulkOp();
for (let j = 1; j <= productCount; j++) {
  productsBulk.insert({
    productName: "Product" + j,
    price: getRandomPrice(),
    createdDate: getRandomDate()
  });

  if (j % productBatchSize === 0) {
    productsBulk.execute();
    productsBulk = db.Products.initializeUnorderedBulkOp();
    if (j % 200 === 0) {
      print(`Inserted ${j} products...`);
    }
  }
}
if (productsBulk.length > 0) {
  productsBulk.execute();
  print(`Inserted ${productCount} products.`);
}

// Fetch all product IDs for reference (note: no .valueOf() call)
// const productIds = db.Products.find({}, { _id: 1, productName: 1 }).map(prod => prod.productName);

// ----
// 3c. Seed Orders (with embedded orderDetails)
// ----
const orderCount = 10000;
const orderBatchSize = 10;
print("Seeding Orders and OrderDetails...");
let ordersBulk = db.Orders.initializeUnorderedBulkOp();
for (let k = 1; k <= orderCount; k++) {
  // Retrieve a random customer using aggregation $sample
  const randomCustomer = db.Customers.aggregate([{ $sample: { size: 1 } }]).next();
  const customerId = randomCustomer._id;
  const orderDate = getRandomDate();

  // Generate between 1 and 5 order details
  const detailCount = getRandomInt(1, 5);
  const orderDetails = [];

  for (let d = 0; d < detailCount; d++) {
    // Get the product to retrieve its price
    const randomProduct = db.Products.aggregate([{ $sample: { size: 1 } }]).next();
    const quantity = getRandomInt(1, 10);
    orderDetails.push({
      productId: randomProduct._id,
      quantity: quantity,
      unitPrice: randomProduct.price
    });
  }

  ordersBulk.insert({
    customerId: customerId,
    orderDate: orderDate,
    orderDetails: orderDetails
  });

  if (k % orderBatchSize === 0) {
    ordersBulk.execute();
    print(`Inserted ${k} orders with order details...`);
    ordersBulk = db.Orders.initializeUnorderedBulkOp();
    if (k % 50000 === 0) {
      print(`Inserted ${k} orders with order details...`);
    }
  }
}
if (ordersBulk.length > 0) {
  ordersBulk.execute();
  print(`Inserted ${orderCount} orders with order details.`);
}

print("Data seeding completed.");

// ================================
// Step 4: Sample Complex Query for Benchmark Testing
// ================================
// print("Executing sample aggregation query...");
//
const pipeline = [
  // Join Orders with Customers
  {
    $lookup: {
      from: "Customers",
      localField: "customerId",
      foreignField: "_id",
      as: "customer"
    }
  },
  { $unwind: "$customer" },

  // Filter Orders in the last 180 days and Customer email domain
  {
    $match: {
      orderDate: { $gte: new Date(new Date().setDate(new Date().getDate() - 180)) },
      "customer.email": /@example\.com$/
    }
  },

  // Unwind orderDetails to aggregate
  { $unwind: "$orderDetails" },

  // Join orderDetails with Products
  {
    $lookup: {
      from: "Products",
      localField: "orderDetails.productId",
      foreignField: "_id",
      as: "product"
    }
  },
  { $unwind: "$product" },

  // Group by Customer to calculate OrdersCount and TotalSpent
  {
    $group: {
      _id: "$customer._id",
      firstName: { $first: "$customer.firstName" },
      lastName: { $first: "$customer.lastName" },
      email: { $first: "$customer.email" },
      OrdersCount: { $addToSet: "$_id" },
      TotalSpent: { $sum: { $multiply: ["$orderDetails.quantity", "$orderDetails.unitPrice"] } }
    }
  },

  // Project the desired fields
  {
    $project: {
      CustomerID: "$_id",
      firstName: 1,
      lastName: 1,
      email: 1,
      OrdersCount: { $size: "$OrdersCount" },
      TotalSpent: 1
    }
  },

  // Sort by TotalSpent descending
  { $sort: { TotalSpent: -1 } },

  // Limit to top 100 for demonstration
  { $limit: 100 }
];
//
// // Execute the aggregation and measure execution time
const start = new Date();
const results = db.Orders.aggregate(pipeline).toArray();
const end = new Date();
print(`Aggregation completed in ${end - start} ms.`);
//
// Optionally, print the results
printjson(results);
