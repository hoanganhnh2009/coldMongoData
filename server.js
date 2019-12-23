require("dotenv").config();
const assert = require("assert");
const dbName = "abitmes";
const { clientConnect, clientClose, clientConnectCold } = require("./dbMongo");
const ObjectId = require("mongodb").ObjectId;
const CollectName = "vpb_pms";
const limit = 10000;

const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER IS RUNNING...");
let pmsDeletedCount = 0;
let m = process.argv[2];

let startTime = new Date().getTime();
console.log("TCL: startTime", startTime);

const months = {
  7: {
    min: null,
    max: new ObjectId("5d3cb28270cc432ea082a37c")
  },
  8: {
    min: new ObjectId("5d3cb28270cc432ea082a37c"),
    max: new ObjectId("5d6a3c62d2fae53a704f3c67")
  },
  9: {
    min: new ObjectId("5d6a3c62d2fae53a704f3c67"),
    max: new ObjectId("5d918d3c85f8d6237a18a707")
  },
  10: {
    min: new ObjectId("5d918d3c85f8d6237a18a707"),
    max: new ObjectId("5db673725e038a65f071fced")
  },
  11: {
    min: new ObjectId("5db673725e038a65f071fced"),
    max: null
  }
};

// console.log("TCL: process.argv[0] vpb_pms", months[m]&&months[m].end);

async function bulkLargeRecord(data) {
  return new Promise(async (resolve, reject) => {
    try {
      const db = clientCold.db(dbName);
      const collection = db.collection(CollectName);
      var bulk = collection.initializeUnorderedBulkOp();
      data.map((e, i) => {
        bulk.insert(e);
      });
      if (BulkHasOperations(bulk)) {
        let res = await bulk.execute();
        logger.success(`Total Record Inserted: ${res.result.nInserted}`);
        resolve(res.result.nInserted);
      } else {
        reject(0);
      }
    } catch (err) {
      console.log("TCL: bulkLargeRecord -> err", err);
      reject(0);
    }
  });
}

async function getLastIdOld() {
  const db = clientCold.db(dbName);
  const collection = db.collection(CollectName);
  let data = await collection
    .find({})
    .project({ _id: 1 })
    .sort({ _id: -1 })
    .limit(1)
    .toArray();
  logger.warn(`NextId: ${JSON.stringify(data)}`);
  if (data.length) return data[0]._id;
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
      console.time("Total Time Insert");
      await bulkLargeRecord(data);
      console.timeEnd("Total Time Insert");
      setTimeout(() => {
        let nextId = data[data.length - 1]._id;
        getData(nextId);
      }, 100);
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
  await clientConnectCold();
  let nextId = await getLastIdOld();
  console.log("TCL: main -> nextId", nextId);
  getData(nextId);
}

main();
