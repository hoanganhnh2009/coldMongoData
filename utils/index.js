const logger = require("./logger");

const BulkHasOperations = b =>
  b &&
  b.s &&
  b.s.currentBatch &&
  b.s.currentBatch.operations &&
  b.s.currentBatch.operations.length > 0;

module.exports = { logger, BulkHasOperations };
