var _ = require('lodash');
var cradle = require('cradle');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var fs = require('fs');

var when = require('when');
var node = require('when/node');
var fn = require('when/function');
var callbacks = require('when/callbacks');

var INCLUDE_DOCS = {include_docs: true};
var DB_CHANGE = {since: 'now', include_docs: true};

var CouchConfig = function (options) {

  // setup defaults
  var requiredProps = ['host', 'port', 'databaseName', 'view'];
  _.each(options || requiredProps, function (v, k) {
    if (!options[k]) {
      throw new Error('CouchDB configuration error.  Missing options.' + k);
    }
  });

  this.db = null;
  this.options = options;
};

util.inherits(CouchConfig, EventEmitter);

/**
 * Initializes config, sets up listener for site changes.  If init succeeds, then 'site' on a single change, 'sites' when all are updated, and 'error' events are emitted.
 * @param callback - function(err, sites) where sites is an array of all sites.
 **/
CouchConfig.prototype.init = function (callback) {
  var options = this.options;
  var liftedCallback = node.liftCallback(callback);

  // Create cradle connection
  var db = this.db = new (cradle.Connection)(options.host, options.port, options.extraConf).database(options.databaseName);
  var that = this;

  var dbExists = node.lift(db.exists.bind(db));
  var getDbChangeHandler = db.changes.bind(db, DB_CHANGE, null);
  var getService = node.lift(this.getServices.bind(that));
  var failIfNoDb = function (exists) {
    if (!exists) throw new Error('Database ' + options.databaseName + ' does not exist.\n');
  };

  function emitUpdate (sites) {
    that.emit('update', sites);
  }

  function subscribeToChanges (feed) {
    feed.on('error', function (err) {
      logger.error("Subscription to couchdb failed");
      logger.error(err);
    });

    feed.on('change', function () {
      getService()
        .then(emitUpdate)
        .catch(function (err) {
          logger.error("Can't fetch couchdb or config changes");
          logger.error(err);
        });
    });
  }

  var dbExistsResult = dbExists()
      .tap(failIfNoDb)
      .then(getDbChangeHandler)
      .tap(subscribeToChanges)
      .catch(function(err) {
        logger.error(err.stack);  
      });

  liftedCallback(dbExistsResult);
};

CouchConfig.prototype.getServices = function (callback) {

  var passConfiguration = node.liftCallback(callback);
  var readCouchDb = node.lift(this.db.view.bind(this.db));
  var fileToJson = fn.compose(node.lift(fs.readFile), fn.lift(JSON.parse));

  var files = this.options.files || [];

  var readAllFiles = when.map(files, function (v, i) { return fileToJson(v); });

  var rowsToDocs = when.lift(function ensureIds(rows) {
    return _.map(rows, function (row) {
      row.doc.id = row.doc._id;
      return row.doc;
    });
  });

  var results = when.join(
    readCouchDb(this.options.view, INCLUDE_DOCS).then(rowsToDocs),
    readAllFiles
  )
    .then(function (configs) {
      return _.flatten(configs, true);
    })
    .catch(onError);

  passConfiguration(results);

  function onError(reason) {
    logger.error("Reading configuration failed: ", reason);
  }
};

var CouchPlugin = function () {
};

CouchPlugin.prototype.attach = function (options) {
  this.externalConfig = new CouchConfig(options);
};

CouchPlugin.prototype.init = function (done) {
  this.externalConfig.init(done);
};

module.exports = CouchPlugin;
