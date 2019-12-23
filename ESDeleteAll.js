require("dotenv").config();
const { clientConnect } = require("./dbMongo");
var request = require("request");
var deleteCount = 0;
const { logger, BulkHasOperations } = require("./utils");
console.log("SERVER IS RUNNING...");
let m = process.argv[2];

async function deletePmsById(hits) {
  return new Promise((resolve, reject) => {
    let body = {
      query: {
        bool: {
          must: [
            {
              range: {
                create_at: {
                  lte: 1570640400000 //10-10-2019
                }
              }
            }
          ]
        }
      }
    };
    return request(
      {
        headers: { "Content-Type": "application/json" },
        uri: `http://search.abitmes.vn/abitmes.vpb_pms/_delete_by_query?conflicts=proceed`,
        body: JSON.stringify(body),
        method: "POST"
      },
      function(error, response, body) {
      console.log("TCL: deletePmsById -> body", body)
        if (error) return resolve(0);
        let responseJson = JSON.parse(body);
        let { deleted, total } = responseJson;
        console.log("TCL: deletePmsById -> responseJson", responseJson.failures);
        if (responseJson.error) {
          logger.error(responseJson.error.reason);
        }
        deleteCount = deleteCount + deleted;
        logger.success(`Đã xoá ${deleted} / ${total} [${deleteCount}] record`);
        resolve(true);
      }
    );
  });
}

async function main() {
  deletePmsById();
}

main();
