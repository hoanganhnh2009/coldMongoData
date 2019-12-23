require("dotenv").config();
const assert = require("assert");
const { clientConnect, clientClose } = require("./dbMongo");
const SIZE = 100;
const TIME_OUT = 100;
var request = require("request");
var deleteCount = 0;
const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER IS RUNNING...");
let m = process.argv[2];

async function removePmsQuery(hits) {
  console.time("vpb_pms" + hits[0]._id);
  let terms = [];
  hits.map(e => {
    let { type, pagescopeid, id_conversations, id_fb: idprofile } = e._source;
    let query = {
      bool: {
        must: [
          {
            term: {
              idprofile
            }
          },
          {
            term: {
              id_conversations
            }
          }
        ]
      }
    };
    if (!type) {
      query = {
        term: {
          pagescopeid
        }
      };
    }
    terms = terms.concat(query);
  });
  let body = {
    query: {
      bool: {
        should: terms
      }
    }
  };
  return new Promise((resolve, reject) => {
    return request(
      {
        headers: { "Content-Type": "application/json" },
        uri: `http://search.abitmes.vn/abitmes.vpb_pms/_delete_by_query`,
        body: JSON.stringify(body),
        method: "POST"
      },
      function(error, response, body) {
        console.timeEnd("vpb_pms" + hits[0]._id);
        if (error) return resolve(false);
        let responseJson = JSON.parse(body);
        let { deleted, total } = responseJson;
        deleteCount = deleteCount + deleted;
        if (responseJson.error) {
          logger.error(responseJson.error.reason);
          return resolve(false);
        }
        logger.warn(
          `Đã xoá ${deleted} / ${total} [${deleteCount}] record vpb_pms`
        );
        return resolve(true);
      }
    );
  });
}
async function removeUserQuery(hits) {
  logger.warn("Removed abitmes.users");
  console.time("users" + hits[0]._id);
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
  return new Promise((resolve, reject) => {
    return request(
      {
        headers: { "Content-Type": "application/json" },
        uri: `http://search.abitmes.vn/abitmes.users/_delete_by_query`,
        body: JSON.stringify(body),
        method: "POST"
      },
      function(error, response, body) {
        console.timeEnd("users" + hits[0]._id);
        if (error) return resolve(false);
        let responseJson = JSON.parse(body);
        let { deleted, total } = responseJson;
        if (responseJson.error) {
          logger.error(responseJson.error.reason);
          return resolve(false);
        }
        logger.success(`Đã xoá ${deleted} / ${total} record users`);
        return resolve(true);
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
              last_visited: {
                lte: 1570640400000 //15-09-2019
              }
            }
          }
        ]
      }
    },
    size: SIZE,
    from: 0,
    _source: [
      "id_from_page",
      "pagescopeid",
      "id_fb",
      "id_conversations",
      "type",
      "_id",
      "last_visited"
    ],
    sort: [{ _id: { order: "asc" } }]
  };
  return request(
    {
      headers: { "Content-Type": "application/json" },
      uri: `http://search.abitmes.vn/abitmes.users/_search`,
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
      logger.info("Số abitmes.users còn lại: " + hits.total);
      if (hits.total) {
        let data = hits.hits;
        console.time("START");
        await removePmsQuery(data);
        await removeUserQuery(data);
        // await deletePmsById(data);
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
