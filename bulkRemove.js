require("dotenv").config();
const assert = require("assert");
const dbName = "abitmes";
const { clientConnect, clientClose, clientConnectCold } = require("./dbMongo");
const ObjectId = require("mongodb").ObjectId;
const CollectName = "vpb_pms";
const limit = 500;

const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER IS RUNNING...");
let m = process.argv[2];

async function bulkLargeRecord(data) {
  return new Promise(async (resolve, reject) => {
    try {
      const db = client.db(dbName);
      const collection = db.collection(CollectName);
      var bulk = collection.initializeUnorderedBulkOp();
      data.map((e, i) => {
        // bulk.find({ _id: new ObjectId(e._id) }).remove();
        bulk.find({ _id: new ObjectId(e._id) }).remove();
      });
      if (BulkHasOperations(bulk)) {
        let res = await bulk.execute();
        logger.success(`deleteCount: ${res.result.nRemoved}`);
        resolve(res.result.nRemoved);
      } else {
        reject(0);
      }
    } catch (err) {
      console.log("TCL: bulkLargeRecord -> err", err);
      reject(0);
    }
  });
}

async function getAllPmsOld() {
  const db = client.db(dbName);
  const collection = db.collection("vpb_pms");
  // db.getCollection('vpb_pms').find({"_id" :{$lte:ObjectId("5d41c90b07b7042d91716879")}}).sort({_id:-1}).count()
  const query = {
    _id: { $lte: new ObjectId("5d7d1c96168a6a22a2a405d5") } //05/09/2019 //5d41c90b07b7042d91716879 01/08/2019 vpb_pms
  };
  let data = await collection
    .find(query)
    .project({ _id: 1 })
    .sort({ _id: 1 })
    .limit(limit)
    .skip(0)
    .toArray();

  if (data.length) {
    logger.warn(`data: ${JSON.stringify(data[0]._id)}`);
    console.time("bulkLargeRecord");
   /*  let res = await bulkLargeRecord(data);
    console.timeEnd("bulkLargeRecord");
    setTimeout(() => {
      getAllPmsOld();
    }, 100); */
  } else {
    logger.success("Done...");
    return false;
  }
  // next array record
}

async function main() {
  await clientConnect();
  getAllPmsOld();
}

main();
