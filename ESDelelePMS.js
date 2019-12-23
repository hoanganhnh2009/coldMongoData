require("dotenv").config();
const assert = require("assert");
const dbName = "abitmes";
const { clientConnect, clientClose, clientConnectCold } = require("./dbMongo");
const ObjectId = require("mongodb").ObjectId;
const CollectName = "vpb_pms";
const SIZE = 1000;
const TIME_OUT = 100;
var request = require("request");
var deleteCount = 0;
const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER IS RUNNING...");
let m = process.argv[2];

async function deletePmsById(hits) {
  return new Promise((resolve, reject) => {
    let terms = [];
    hits.map(e => {
      query = {
        term: {
          _id: e._id
        }
      };
      terms = terms.concat(query);
    });
    let body = {
      query: {
        bool: {
          should: terms
        }
      }
    };
    return request(
      {
        headers: { "Content-Type": "application/json" },
        uri: `http://search.abitmes.vn/abitmes.vpb_pms/_delete_by_query`,
        body: JSON.stringify(body),
        method: "POST"
      },
      function(error, response, body) {
        if (error) return resolve(0);
        let responseJson = JSON.parse(body);
        let { deleted, total, failures } = responseJson;
        if (responseJson.error) {
          logger.error(responseJson.error.reason);
        }
        deleteCount = deleteCount + deleted;
        logger.success(`Đã xoá ${deleted} / ${total} [${deleteCount}] record`);
        failures.length && logger.error(failures[0].cause.reason);
        resolve(true);
      }
    );
  });
}

async function getUserQuery() {
  let body = {
    query: {
      bool: {
        must: [
          {
            range: {
              create_at: {
                lte: 1572368400000 //15-09-2019
              }
            }
          }
        ]
      }
    },
    size: SIZE,
    from: 0,
    _source: ["_id"],
    sort: [{ _id: { order: "desc" } }]
  };
  return request(
    {
      headers: { "Content-Type": "application/json" },
      uri: `http://search.abitmes.vn/abitmes.vpb_pms/_search`,
      body: JSON.stringify(body),
      method: "POST"
    },
    async function(error, response, body) {
      if (error) return;
      let responseJson = JSON.parse(body);
      let { hits } = responseJson;
      if (responseJson.error) {
        logger.success("...End Game");
        return;
      }
      logger.info("Số abitmes.vpb_pms còn lại: " + hits.total);
      if (hits.total) {
        let data = hits.hits;
        console.time("START");
        await deletePmsById(data);
        console.timeEnd("START");
        logger.warn(`Last _id ${data[0]._id}`);
        setTimeout(() => {
          getUserQuery();
        }, TIME_OUT);
        return;
      } else {
        logger.success("...End Game");
      }
    }
  );
}

async function main() {
  await clientConnect();
  getUserQuery();
}

main();
