'use strict';
const storeInit = require('./lib/dynamoStore');
/**
 * The DynamoDB Store wrapper handles connections to DynamoDB.
 * It provides a simple abstraction layer of connecting to DynamoDB in a Thorin-esque way.
 */
module.exports = function init(thorin, opt) {
  // Attach the Mongo error parser to thorin.
  const ThorinDynamoStore = storeInit(thorin, opt);

  return ThorinDynamoStore;
};
module.exports.publicName = 'dynamo';
