'use strict';
const AWS = require('aws-sdk');

module.exports = (thorin) => {

  const config = Symbol('config'),
    db = Symbol('db');

  const DB_METHODS = [
    'batchGetItem', 'batchWriteItem',
    'createTable', 'deleteTable', 'describeTable', 'listTables', 'updateTable',
    'putItem', 'getItem', 'deleteItem', 'updateItem',
    'query', 'scan'
  ];

  class ThorinDynamoStore extends thorin.Interface.Store {

    static publicName() {
      return "dynamo";
    }

    constructor() {
      super();
      this.TYPE = PARAM_TYPES;
    }

    init(storeConfig) {
      if (storeConfig.key) {
        storeConfig.accessKeyId = storeConfig.key;
        delete storeConfig.key;
      }
      if (storeConfig.secret) {
        storeConfig.secretAccessKey = storeConfig.secret;
        delete storeConfig.secret;
      }
      this[config] = thorin.util.extend({
        debug: false,
        signatureVersion: 'v4',
        maxRetries: 5,
        accessKeyId: null,
        secretAccessKey: null,
        region: null,
        timeout: 5,
        endpoint: null,
        options: {}
      }, storeConfig);
      if (this[config].region === 'local' && !this[config].endpoint) {
        this[config].endpoint = 'http://localhost:8000';
      }
      if (this[config].endpoint) {
        this[config].endpoint = new AWS.Endpoint(this[config].endpoint);
      }
    }

    run(done) {
      if (this[db]) return done();
      if (this[config].region !== 'local') {
        if (!this[config].accessKeyId) return done(thorin.error('DYNAMO.IAM', 'Missing access key id'));
        if (!this[config].secretAccessKey) return done(thorin.error('DYNAMO.IAM', 'Missing secret access key'));
      }
      if (!this[config].region) return done(thorin.error('DYNAMO.CONFIG', 'Please specify a region'));

      let opt = {
        accessKeyId: this[config].accessKeyId,
        secretAccessKey: this[config].secretAccessKey,
        maxRetries: this[config].maxRetries,
        region: this[config].region,
        httpOptions: {
          connectTimeout: this[config].timeout * 1000
        }
      };
      if (this[config].options) {
        opt = thorin.util.extend(opt, this[config].options);
      }
      if (this[config].endpoint) opt.endpoint = this[config].endpoint;
      this[db] = new AWS.DynamoDB(opt);
      // Check our connection
      this.logger.trace(`Connecting to Dynamo`);
      this[db].listTables({
        Limit: 1
      }, (err) => {
        if (err) {
          this.logger.error(`Could not connect to DynamoDB [${err.code} - ${err.message}]`);
          return done(thorin.error('DYNAMO.RUN', err.message, err));
        }
        done();
      });
    }

    /**
     * The store exposes all DynamoDB methods by using prototype function attachments (below)
     * NOTE: all exposed functions will ALWAYS return PROMISES.
     * */


    get client() {
      return this[db];
    }

    get logger() {
      return thorin.logger(this.name);
    }
  }

  ThorinDynamoStore.prototype.waitFor = function (state, params, fn) {
    if (typeof state !== 'string' || !state) throw thorin.error('DYNAMO.DATA', 'A state name is required');
    if (typeof params !== 'object' || !params) params = {};
    let isDebug = this[config].debug,
      logger = this.logger;
    if (isDebug) {
      logger.trace(`Performing [${method}] -> ${JSON.stringify(params)}`);
    }
    // Handle callback.
    if (typeof fn === 'function') {
      return this[db].waitFor(state, params, function (err, res) {
        if (err) {
          return fn(parseError(err, isDebug, logger));
        }
        fn(null, res);
      });
    }
    // Handle promise
    return new Promise((resolve, reject) => {
      this[db].waitFor(params, function (err, res) {
        if (err) {
          return reject(parseError(err, isDebug, logger));
        }
        resolve(res);
      });
    });
  };

  DB_METHODS.forEach((method) => {
    ThorinDynamoStore.prototype[method] = function (params, fn) {
      if (typeof params !== 'object' || !params) params = {};
      let isDebug = this[config].debug,
        logger = this.logger;
      if (isDebug) {
        logger.trace(`Performing [${method}] -> ${JSON.stringify(params)}`);
      }
      // Handle callback.
      if (typeof fn === 'function') {
        return this[db][method](params, function (err, res) {
          if (err) {
            return fn(parseError(err, isDebug, logger));
          }
          fn(null, res);
        });
      }
      // Handle promise
      return new Promise((resolve, reject) => {
        this[db][method](params, function (err, res) {
          if (err) {
            return reject(parseError(err, isDebug, logger));
          }
          resolve(res);
        });
      });
    }
  });

  function parseError(err, isDebug, logger) {
    if (err.errors instanceof Array) {
      err.errors = err.errors.map(e => {
        delete e.stack;
        return e;
      });
    }
    if (isDebug) {
      err.stack = [];
      logger.debug(err);
    }
    let terr = thorin.error(err.code, err.message, err.errors || undefined);
    terr.ns = 'DYNAMO';
    return terr;
  }

  const PARAM_TYPES = {
    STRING: 'S',
    NUMBER: 'N',
    OBJECT: 'M',
    BOOLEAN: 'BOOL',
    ARRAY: 'L',
    ARRAY_NUMBER: 'NS',
    ARRAY_STRING: 'SS',
    ARRAY_BINARY: 'BS',
    BINARY: 'B',
    NULL: 'NULL'
  };

  return ThorinDynamoStore;
};
