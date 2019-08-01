'use strict';

module.exports = (thorin, opt) => {

  const config = Symbol('config'),
    _logger = thorin.logger('store.' + opt.name || 'dynamo');

  class ThorinDynamoStore extends thorin.Interface.Store {

    static publicName() {
      return "dynamo";
    }

    constructor() {
      super();
      this.type = 'dynamo';
    }

    init(storeConfig) {
      this[config] = thorin.util.extend({}, storeConfig);

    }

    run(done) {
      // TODO
      done();
    }


    get logger() {
      return _logger;
    }
  }

  return ThorinDynamoStore;
};
