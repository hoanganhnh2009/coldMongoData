require("dotenv").config();
const assert = require("assert");
const url =
  "mongodb://43.239.223.6:27017,43.239.223.7:27017,43.239.223.8:27017";
const urlCold = "mongodb://43.239.223.49:27017";
const urlTest = "mongodb://103.7.43.37:27017";
const MongoClient = require("mongodb").MongoClient;

module.exports = {
  /*
   * Mongo Utility: Connect to client create by MR. Thành */

  clientConnect: async () =>
    (client = await (() =>
      new Promise((resolve, reject) =>
        MongoClient.connect(
          url,
          {
            replicaSet: "mongo_rep",
            useNewUrlParser: true
          },
          (err, client) => {
            assert.equal(null, err);
            resolve(client);
          }
        )
      ))()),
  clientConnectCold: async () =>
    (clientCold = await (() =>
      new Promise((resolve, reject) =>
        MongoClient.connect(
          urlCold,
          {
            useNewUrlParser: true
          },
          (err, clientCold) => {
            assert.equal(null, err);
            resolve(clientCold);
          }
        )
      ))()),
  clientConnectTest: async () =>
    (clientTest = await (() =>
      new Promise((resolve, reject) =>
        MongoClient.connect(
          urlTest,
          {
            useNewUrlParser: true
          },
          (err, clientCold) => {
            assert.equal(null, err);
            resolve(clientCold);
          }
        )
      ))()),

  /*
   * Mongo Utility: Close client */

  clientClose: async client => {
    await client.close();
    return true;
  }
};
