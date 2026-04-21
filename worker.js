// worker.js
const AWS = require("aws-sdk");
const { MongoClient } = require("mongodb");

// ---------- CONFIGURATION ----------
const REGION = process.env.AWS_REGION || "us-east-1";
const MONGO_URL = process.env.MONGO_URL;
const BATCH_SIZE = 500;       // MongoDB bulkWrite batch size
const TABLE_PARALLELISM = 5;  // Number of tables migrated simultaneously

// ---------- AWS SDK ----------
AWS.config.update({
  region: REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const dynamo = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

// ---------- MongoDB ----------
const mongo = new MongoClient(MONGO_URL);

// ---------- HELPER: Fetch all DynamoDB tables ----------
async function listAllTables() {
  let tables = [];
  let params = {};
  do {
    const data = await dynamo.listTables(params).promise();
    tables = tables.concat(data.TableNames);
    params.ExclusiveStartTableName = data.LastEvaluatedTableName;
  } while (params.ExclusiveStartTableName);
  return tables;
}

// ---------- HELPER: Migrate a single table ----------
async function migrateTable(tableName) {
  console.log(`Starting migration for table: ${tableName}`);
  const db = mongo.db("mydb");
  const collection = db.collection(tableName);

  let params = { TableName: tableName };
  let total = 0;

  do {
    const data = await docClient.scan(params).promise();
    const items = data.Items;

    // Batch insert/update
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      const batch = items.slice(i, i + BATCH_SIZE);
      const bulkOps = batch.map(item => ({
        updateOne: {
          filter: { _id: item.customer_id || item.hari || item.id },
          update: { $set: item },
          upsert: true,
        },
      }));
      await collection.bulkWrite(bulkOps);
      total += batch.length;
      console.log(`Inserted/updated ${total} items into ${tableName}`);
    }

    params.ExclusiveStartKey = data.LastEvaluatedKey;
  } while (params.ExclusiveStartKey);

  console.log(`Finished migration for table: ${tableName}, total items: ${total}`);
}

// ---------- MAIN WORKER ----------
async function runMigration() {
  try {
    await mongo.connect();
    console.log("MongoDB connected");

    const tables = await listAllTables();
    console.log(`Found ${tables.length} tables in DynamoDB`);

    // Process tables in parallel with concurrency limit
    const queue = [...tables];
    const workers = new Array(TABLE_PARALLELISM).fill(null).map(async () => {
      while (queue.length > 0) {
        const table = queue.shift();
        try {
          await migrateTable(table);
        } catch (err) {
          console.error(`Error migrating table ${table}:`, err);
        }
      }
    });

    await Promise.all(workers);

    console.log("All tables migrated successfully");
  } catch (err) {
    console.error("Migration failed:", err);
  } finally {
    await mongo.close();
  }
}

runMigration();
