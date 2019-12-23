require("dotenv").config();
const assert = require("assert");
const dbName = "abitmes";
const { clientConnect, clientClose, clientConnectCold } = require("./dbMongo");
const ObjectId = require("mongodb").ObjectId;
const CollectName = "users";
const limit = 100;
let deletedCount = 0;
const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER REMOVE OLD_DATA IS RUNNING...");
// let m = process.argv[2];

// Remove
async function getAllUsersOld() {
  const db = client.db(dbName);
  const collection = db.collection("users");
  const query = {
    last_visited: { $lte: 1564592400000 } //01/08/2019
  };
  let data = await collection
    .find(query)
    .project({ id_fb: 1, pagescopeid: 1, type: 1, id_conversations: 1 })
    .sort({ last_visited: 1 })
    .limit(limit)
    .skip(0)
    .toArray();
  // logger.warn(`data: ${JSON.stringify(data)}`);
  await Promise.all(
    data.map(obj => {
      removeUserData(obj);
    })
  );
  if (data.length) {
    setTimeout(() => {
      getAllUsersOld();
    }, 1000);
    // return true;
  } else {
    logger.error("COUNT DELETED " + deletedCount);
    process.exit();
    return false;
  }
  // next array record
}
async function removeUserData(obj) {
  try {
    const db = client.db(dbName);
    const collection = db.collection("users"); //users
    let query = { _id: new ObjectId(obj._id) };
    let data = await collection.deleteOne(query);
    await removePmsData(obj);
    logger.success(`users _id deleted: ${obj._id}`);
    return data.result.ok;
  } catch (error) {
    console.log(error);
    return false;
  }
}

async function removePmsData(obj) {
  try {
    const db = client.db(dbName);
    const collection = db.collection("vpb_pms"); //vpb_pms
    let query = {};
    if (obj.type) {
      query = {
        id_conversations: obj.id_conversations,
        idprofile: obj.id_fb
      };
    } else {
      query = {
        pagescopeid: obj.pagescopeid
      };
    }
    if (!(Object.entries(query).length === 0 && query.constructor === Object)) {
      let data = await collection.deleteMany(query);
      logger.success(
        `vpb_pms query: ${JSON.stringify(query)} - deletedCount: ${
          data.deletedCount
        }`
      );
      deletedCount = deletedCount + data.deletedCount;
      console.warn("deletedCount: ", deletedCount);
      return data.result.ok;
    }
    return false;
  } catch (error) {
    console.log(error);
    return false;
  }
}
// #end Remove

async function main() {
  await clientConnect();
  getAllUsersOld();
}

main();
