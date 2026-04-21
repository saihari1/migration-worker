const AWS = require("aws-sdk");
const { MongoClient } = require("mongodb");

// AWS config
AWS.config.update({
  region: "us-east-1",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});

const dynamo = new AWS.DynamoDB.DocumentClient();

// MongoDB (Railway)
const mongo = new MongoClient(process.env.MONGO_URL);

async function migrate(tableName, collectionName) {
  const db = mongo.db("mydb");
  const collection = db.collection(collectionName);

  let params = { TableName: tableName };

  do {
    const data = await dynamo.scan(params).promise();

    const docs = data.Items.map(item => ({
      ...item,
      _id: item.customer_id || item.hari || item.id
    }));

    if (docs.length > 0) {
      await collection.bulkWrite(
        docs.map(doc => ({
          updateOne: {
            filter: { _id: doc._id },
            update: { $set: doc },
            upsert: true
          }
        }))
      );
    }

    params.ExclusiveStartKey = data.LastEvaluatedKey;

  } while (params.ExclusiveStartKey);
}

async function run() {
  try {
    await mongo.connect();
    console.log("MongoDB connected");

    await migrate("customer_name", "customer_name");
    await migrate("customer_id", "customer_id");

    console.log("Migration completed");
  } catch (err) {
    console.error("Migration failed:", err);
  } finally {
    await mongo.close();
  }
}

run();
