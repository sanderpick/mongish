/*
 * mongish.js: MongoDB wrapper.
 *
 */

// Module dependencies.
var mongodb = require('mongodb');
var _ = require('underscore');
var _s = require('underscore.string');
var util = require('util');
var Step = require('step');
var oid = exports.oid = mongodb.BSONPure ? mongodb.BSONPure.ObjectID : mongodb.ObjectID;

// Establish a db connection wrapper (constructor).
exports.Connection = function (uri, options, dbOptions, cb) {
  if (typeof options === 'function') {
    cb = options;
    options = {};
    dbOptions = {};
  }
  if (typeof dbOptions === 'function') {
    cb = dbOptions;
    dbOptions = {};
  }
  this.options = options;

  // Adds a collection to this db instance.
  this.add = function (name, conf, cb) {
    if (!this.db) {
      return cb('Connect to a db instance before adding a collection');
    }
    var self = this;
    var col;
    Step(
      function () {
        self.db.collection(name, _.bind(function (err, _col) {
          if (err) return this(err);
          col = _col;
          if (_s.endsWith(col.collectionName, 'y') &&
              !_s.endsWith(col.collectionName, 'ey')) {
            exports[_s.capitalize(_s.strLeftBack(col.collectionName, 'y')) + 'ies'] = col;
          } else {
            exports[_s.capitalize(col.collectionName) + 's'] = col;
          }
          if (options.ensureIndexes && conf.indexes && conf.uniques) {
            util.log('Ensuring `' + col.collectionName + '` collection indexes');
            col.dropAllIndexes(_.bind(function () {
              if (conf.indexes.length === 0) {
                return this();
              }
              var next = _.after(conf.indexes.length, this);
              _.each(conf.indexes, function (x, i) {
                col.ensureIndex(x, {
                  unique: conf.uniques[i],
                  sparse: conf.sparses ? conf.sparses[i]: false,
                  dropDups: true
                }, next);
              });
            }, this));
          } else {
            this();
          }
        }, this));
      },
      function (err) { cb(err, col); }
    );
  };

  // Connect
  mongodb.connect(uri, dbOptions, _.bind(function (err, db) {
    this.db = db;
    cb(err, this);
  }, this));
};

/*
 * Inflate (replace) +_ids with documents.
 */
var inflate = exports.inflate = function (docs, conf, cb) {
  var _cb;
  if (_.isArray(docs)) {
    if (docs.length === 0)
      return cb(null, docs);
    _cb = _.after(docs.length, cb);
    _.each(docs, handle);
  } else {
    _cb = cb;
    handle(docs);
  }
  function handle(doc) {
    var __cb = _.after(_.size(conf), _cb);
    _.each(conf, function (map, k) {
      var collection = exports[_s.capitalize(map.collection) + 's'];
      var id = doc[k + '_id'];
      delete doc[k + '_id'];
      if (!id) {
        if (doc.missing) {
          doc.missing.push(k);
        } else {
          doc.missing = [k];
        }
        return __cb(null, docs);
      }
      collection.read({_id: id}, function (err, d) {
        if (err) return cb(err);
        if (!d) {
          doc[k] = 404;
          if (doc.missing) {
            doc.missing.push(k);
          } else {
            doc.missing = [k];
          }
          return __cb(null, docs);
        }
        if (map['*']) doc[k] = d;
        else {
          doc[k] = {_id: d._id};
          _.each(map, function (val, att) {
            if (att === 'collection') return;
            doc[k][att] = typeof val === 'function' ? val(d, doc): d[att];
          });
        }
        __cb(null, docs);
      });
    });
  }
};

/*
 * Fill document +_id lists with corresponding documents.
 */
var fill = exports.fill = function (docs, source, key, opts, query, cb) {
  if (typeof opts === 'function') {
    cb = opts;
    opts = {};
    query = {};
  }
  if (typeof query === 'function') {
    cb = query;
    query = {};
  }
  var reverse = opts.reverse;
  delete opts.reverse;

  var collection = exports[source];
  if (!collection) return cb('Source collection not found');

  var isArray = _.isArray(docs);
  if (!isArray) docs = [docs];

  Step(
    function () {
      if (docs.length === 0) return this();
      var next = _.after(docs.length, this);
      _.each(docs, function (doc) {
        query[key] = doc._id;
        Step(
          function () {
            collection.list(query, _.clone(opts), this.parallel());
            collection.count(query, this.parallel());
          },
          function (err, list, count) {
            if (err) return cb(err);
            doc[source.toLowerCase()] = reverse ? list.reverse(): list;
            doc[source.toLowerCase() + '_cnt'] = count;
            next();
          }
        );
      });
    },
    function (err) {
      cb(err, isArray ? docs: _.first(docs));
    }
  );
};

/*
 * Insert a document adding `created` and `updated` keys if
 * they doesn't exist in the given props.
 */
mongodb.Collection.prototype.create = function (props, opts, cb) {
  if ('function' === typeof opts) {
    cb = opts;
    opts = {};
  }
  if (!opts) opts = {};
  var _inflate = opts.inflate;
  delete opts.inflate;
  var _force = opts.force;
  delete opts.force;
  if (!props.created) {
    props.created = new Date();
  }
  if (!props.updated) {
    props.updated = new Date();
  }
  var attempt = 1;
  (function insert() {
    this.insert(props, opts, _.bind(function (err, ins) {
      if (err && err.code === 11000) {
        if (!_force || !_.isObject(_force)) {
          return cb(err);
        }
        var dups = [];
        var m = err.message.match(/index: ([^\s]+)/);
        if (m && m[1]) {
          dups = _.compact(_.map(m[1].split('1'), function (s) {
            return _s.trim(s, '_');
          }));
        }
        var k;
        _.each(_force, function (_v, _k) {
          if (dups.indexOf(_k) !== -1) {
            k = _k;
          }
        });
        if (!k) {
          return cb(err);
        }
        props[k] += '-' + attempt;
        ++attempt;
        return insert.call(this);
      }
      if (err) return cb(err);
      var doc = ins.ops[0];
      if (_inflate && cb) {
        inflate(doc, _inflate, function (err, doc) {
          if (err) return cb(err);
          cb(null, doc);
        });
      } else if (cb) {
        cb(null, doc);
      }
    }, this));
  }).call(this);
};

/*
 * Get a document by query.
 */
mongodb.Collection.prototype.read = function (query, opts, cb) {
  if ('function' === typeof opts) {
    cb = opts;
    opts = {};
  }

  var _inflate = opts.inflate;
  delete opts.inflate;
  var _inc = opts.inc;
  delete opts.inc;

  this.findOne(query, _.bind(function (err, doc) {
    if (err || !doc) return cb(err, doc);
    if (_inflate) {
      inflate(doc, _inflate, function (err, doc) {
        if (err) return cb(err);
        cb(null, doc);
      });
    } else cb(null, doc);
    if (_inc) {
      this._update(query, {$inc: {vcnt: 1}}, function (err) {
        if (err) util.error(err);
      });
    }
  }, this));
};

/*
 * Update a document by query with the given props.
 */
mongodb.Collection.prototype._update = mongodb.Collection.prototype.updateOne;
mongodb.Collection.prototype.update = function (query, props, opts, cb) {
  if ('function' === typeof opts) {
    cb = opts;
    opts = {};
  }
  if (!opts) opts = {};
  var _force = opts.force;
  delete opts.force;

  props.$set = props.$set || {};
  props.$set.updated = new Date();
  var attempt = 1;
  (function update() {
    this._update(query, props, opts, _.bind(function (err, doc) {
      if (err && err.code === 11000) {
        var k = err.message.match(/index: ([^\s]+)/);
        if (k && k[1]) {
          k = _.first(_.map(k[1].split('1'), function (s) {
            return _s.trim(s, '_');
          }));
          if (!_force || !_force[k]) return cb(err);
          props.$set[k] += '-' + attempt;
          ++attempt;
          return update.call(this);
        }
      }
      cb(err, doc);
    }, this));
  }).call(this);
};

/*
 * Delete a document by query.
 */
mongodb.Collection.prototype.delete = function (query, cb) {
  this.remove(query, cb);
};

/*
 * Determine if a doc exists with a specified
 * key/val pair for the collection.
 */
mongodb.Collection.prototype.available = function (query, cb) {
  this.findOne(query, function (err, doc) {
    if (err) return cb(err);
    cb(err, !doc);
  });
};

/*
 * List documents and optionally
 * replace *_ids with the document
 * from the cooresponding collection
 * specified by given _id.
 */
mongodb.Collection.prototype.list = function (query, opts, cb) {
  if ('function' === typeof opts) {
    cb = opts;
    opts = {};
  }

  var _inflate = opts.inflate;
  delete opts.inflate;
  var _inc = opts.inc;
  delete opts.inc;

  this.find(query, opts).toArray(_.bind(function (err, docs) {
    if (err) return cb(err);
    if (!_inflate)
      cb(null, docs);
    else
      inflate(docs, _inflate, function (err, docs) {
        if (err) return cb(err);
        cb(null, _.reject(docs, function (doc) {
          return doc.missing;
        }));
      });
    if (_inc)
      _.each(docs, _.bind(function (d) {
        this._update({_id: d._id}, {$inc: {vcnt: 1}}, function (err) {
          if (err) util.error(err);
        });
      }, this));
  }, this));
};
