require("dotenv").config();
const assert = require("assert");
const dbName = "abitmes";
const { clientConnect, clientClose, clientConnectTest } = require("./dbMongo");
const ObjectId = require("mongodb").ObjectId;
const CollectName = "users";
const limit = 10000;

const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER IS RUNNING...");
let m = process.argv[2];

let startTime = new Date().getTime();

async function bulkLargeRecord(data) {
  return new Promise(async (resolve, reject) => {
    try {
      let db = clientTest.db("dbName");
      let collection = db.collection("CollectName");
      let bulk = collection.initializeUnorderedBulkOp();
      data.map((e, i) => {
        bulk.insert(e);
      });
      bulk
        .execute()
        .then(res => {
          console.log("nInserted", res.result.nInserted);
        })
        .catch(err => {
          console.log("E11000 duplicate key", err.result.nInserted);
        });

      /*   if (BulkHasOperations(bulk)) {
        let res = await bulk.execute();
        logger.success(`Total Record Inserted: ${res.result.nInserted}`);
        resolve(res.result.nInserted);
      } else {
        reject(0);
      } */
      resolve(1);
    } catch (err) {
      console.log("Loi catch roi ne", err);
      resolve(1);
    }
  });
}

async function getLastIdOld() {
  const db = client.db(dbName);
  const collection = db.collection("config");
  let data = await collection.findOne({
    key: "last_id_user"
  });
  logger.warn(`NextId: ${JSON.stringify(data)}`);
  if (data) return data.value;
  return false;
}
async function getData(nextId) {
  try {
    const db = client.db(dbName);
    const collection = db.collection(CollectName);
    let query = {};
    if (nextId) {
      query._id = {
        $gt: new ObjectId(nextId)
      };
    }
    console.log(`==========================================`);
    logger.warn(`Query: ${JSON.stringify(query)}`);
    console.time(`get1000record`);
    let data = await collection
      .find(query)
      .sort({ _id: 1 })
      .limit(limit)
      .toArray();
    logger.info(`Total Record ${data.length}`);
    console.timeEnd(`get1000record`);
    if (data.length) {
      await bulkLargeRecord(data);
      setTimeout(() => {
        let nextId = data[data.length - 1]._id;
        getData(nextId);
      }, 1000);
    } else {
      let endTime = new Date().getTime();
      let totalTime = endTime - startTime;
      logger.success(`${m || ""} Done... at time ${totalTime}`);
      // process.exit();
    }
  } catch (error) {
    logger.error(error);
  }
}

async function main() {
  await clientConnect();
  await clientConnectTest();
  let nextId = null; //await getLastIdOld();`
  console.log("TCL: main -> nextId", nextId);
  getData(nextId);
}

main();
