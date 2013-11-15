var _ = require('lodash');
var cradle = require('cradle');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

var CouchConfig = function(options) {

  // setup defaults
  var requiredProps = ['host', 'port', 'databaseName', 'view'];
  _.each(options || {}, function(v,k) {
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
CouchConfig.prototype.init = function(callback) {
  var options = this.options;

  // Create cradle connection
  var db = this.db = new(cradle.Connection)(options.host, options.port, options.extraConf).database(options.databaseName);
  var that = this;

  // check that database exists
  db.exists(function (err, exists) {

    if (!exists && !err) {
      err = new Error('Database ' + options.databaseName + ' does not exist.');
    }

    if (err) {
      _.isFunction(callback) ? callback(err) : null;
    }
    else {
      that.getServices(function(err, sites) {

        that.emit('update', sites);

        _.isFunction(callback) ? callback(err, sites) : null;
      });
    }

    // setup follow and event emitters.
    if (!err && exists) {
      var feed = db.changes({ since: 'now', include_docs: true });

      // emitthe changed site.
      feed.on('change', function(change) {
        var site = change.doc;
        site.id = site._id;
        that.emit('update', [site]);
      });

      feed.on('error', function(err) {
        // this is a serious error.  We may need a timeout before retrying.
        // For now, we just stop listening to changes.
        that.emit('error', err);
      });
    }

  });
};

CouchConfig.prototype.getServices = function(callback) {
  if (!this.db) {
    throw new Error('Couch configuration not initialized.');
  }

  // get all site configs for this app
  this.db.view(this.options.view, { include_docs: true }, function(err, rows) {
    if (!err) {
      var sites = _.map(rows, function(row) {
        row.doc.id = row.doc._id;
        return row.doc;
      });
    }

    _.isFunction(callback) ? callback(err, sites) : null;
  });
};

var CouchPlugin = function() {};

CouchPlugin.prototype.attach = function(options) {
  this.externalConfig = new CouchConfig(options);
};

CouchPlugin.prototype.init = function(done) {
  this.externalConfig.init(done);
};

module.exports = CouchPlugin;
