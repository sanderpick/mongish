var _ = require('underscore');
var Step = require('step');
var mongish = require('../lib/mongish');
var assert = require('assert');
var uuid = require('uuid');

var config = {};
_.each(require('./config.json'), function (v, k) {
  config[k] = v;
});

describe('mongish', function() {
  this.timeout(5000);
  var connection;
  var collection;

  before(function(done) {
    new mongish.Connection(config.MONGO_URI, {ensureIndexes: true}, function(err, con) {
      if (err) done(err);
      assert.ok(con);
      connection = con;
      done();
    });
  });

  describe('#add()', function() {
    it('should add a new collection', function(done) {
      connection.add('user', {
        indexes: [{primaryEmail: 1}, {username: 1}],
        uniques: [true, true],
        sparses: [true, false]
      }, function(err, col) {
        if (err) done(err);
        assert.ok(col);
        collection = col;
        collection.indexInformation({full: true}, function(err, info) {
          if (err) done(err);
          assert.equal(info.length, 3);
          var primaryEmail = _.find(info, function(i) {
            return i.name == 'primaryEmail_1'
          });
          assert.ok(primaryEmail);
          assert.equal(primaryEmail.unique, true);
          assert.equal(primaryEmail.sparse, true);
          var username = _.find(info, function(i) {
            return i.name == 'username_1'
          });
          assert.ok(username);
          assert.equal(username.unique, true);
          assert.equal(username.sparse, false);
          done()
        });
      });
    });
  });

  describe('#create()', function() {
    it('should create a new doc', function(done) {
      var email = uuid.v1();
      var username = uuid.v1();
      var _id;

      // Test create doc
      mongish.Users.create({primaryEmail: email, username: username}, function(err, doc) {
        if (err) done(err);
        assert.ok(doc);
        assert.ok(doc._id);
        assert.ok(doc.created);
        assert.ok(doc.updated);
        assert.equal(doc.primaryEmail, email);
        assert.equal(doc.username, username);
        _id = doc._id;

        // Test create duplicate
        mongish.Users.create({primaryEmail: email, username: uuid.v1()}, function(err, doc) {
          assert.equal(err.code, 11000); // duplicate email

          // Test force insert (adds attempts to end of duplicate param)
          mongish.Users.create({primaryEmail: email, username: uuid.v1()}, {force: {primaryEmail: 1}}, function(err, doc) {
            if (err) done(err);
            assert.ok(doc);

            // Test create and inflate
            mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1(), friend_id: _id}, {inflate: {friend: { collection: 'user', primaryEmail: 1, username: 1}}}, function(err, doc) {
              if (err) done(err);
              assert.ok(doc.friend);
              assert.ok(doc.friend._id);
              assert.ok(doc.friend.primaryEmail);
              assert.ok(doc.friend.username);
              done();
            });
          });
        });
      });
    });
  });

  describe('#read()', function() {
    it('should read an existing doc', function(done) {
      // Create doc
      mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1()}, function(err, doc) {
        if (err) done(err);

        // Test read doc
        mongish.Users.read({_id: doc._id}, function(err, doc) {
          if (err) done(err);
          assert.ok(doc);
          done();
        });
      });
    });
  });

  describe('#fill()', function() {
    it('should fill an existing doc', function(done) {
      // Create users
      mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1()}, function(err, parent) {
        if (err) done(err);
        Step(
          function() {
            var group = this.group();
            for (var i = 0; i < 5; ++i) {
              mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1(), parent_id: parent._id}, group());
            }
          },
          function(err, kids) {
            if (err) done(err);
            assert.ok(kids);
            assert.equal(kids.length, 5);

            // Test fill
            mongish.fill(parent, 'Users', 'parent_id', this);
          },
          function(err, parentFilled) {
            if (err) done(err);
            // assert.ok(parentFilled);
            // assert.ok(parentFilled.users);
            assert.equal(parentFilled.users.length, 5);
            assert.equal(parentFilled.users_cnt, 5);

            // Test fill with inflate (test creates circular ref. but functionally same)
            mongish.fill(parent, 'Users', 'parent_id', {inflate: {parent: { collection: 'user', primaryEmail: 1, username: 1}}}, this);
          },
          function(err, parentFilled) {
            if (err) done(err);
            assert.ok(parentFilled);
            assert.ok(parentFilled.users);
            assert.ok(parentFilled.users[0]);
            assert.ok(parentFilled.users[0].parent);
            assert.ok(parentFilled.users[0].parent._id);
            assert.ok(parentFilled.users[0].parent.primaryEmail);
            assert.ok(parentFilled.users[0].parent.username);
            done();
          }
        );
      });
    });
  });

  describe('#update()', function() {
    it('should update an existing doc', function(done) {
      // Create docs
      mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1()}, function(err, doc) {
        if (err) done(err);
        var existingUsername = doc.username;

        mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1()}, function(err, doc) {
          if (err) done(err);

          // Test update doc
          var update = {username: uuid.v1()};
          mongish.Users.update({_id: doc._id}, {$set: update}, function(err, status) {
            if (err) done(err);
            assert.ok(status);

            // Test update conflict
            update = {username: existingUsername};
            mongish.Users.update({_id: doc._id}, {$set: update}, function(err, status) {
              assert.equal(err.code, 11000); // duplicate username

              // Test force username
              mongish.Users.update({_id: doc._id}, {$set: update}, {force: {username: 1}}, function(err, status) {
                if (err) done(err);
                assert.ok(status);

                done();
              });
            });
          });
        });
      });
    });
  });

  describe('#delete()', function() {
    it('should delete an existing doc', function(done) {
      // Create doc
      mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1()}, function(err, doc) {
        if (err) done(err);

        // Read doc
        mongish.Users.read({_id: doc._id}, function(err, doc) {
          if (err) done(err);
          assert.ok(doc);

          // Test delete doc
          mongish.Users.delete({_id: doc._id}, function(err, status) {
            if (err) done(err);
            assert.ok(status);

            // Re-read doc
            mongish.Users.read({_id: doc._id}, function(err, doc) {
              if (err) done(err);
              assert.equal(doc, undefined);

              done();
            });
          });
        });
      });
    });
  });

  describe('#list()', function() {
    it('should list existing docs', function(done) {
      // Create users
      mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1()}, function(err, parent) {
        if (err) done(err);
        Step(
          function() {
            var group = this.group();
            for (var i = 0; i < 5; ++i) {
              mongish.Users.create({primaryEmail: uuid.v1(), username: uuid.v1(), parent_id: parent._id}, group());
            }
          },
          function(err, kids) {
            if (err) done(err);
            assert.ok(kids);
            assert.equal(kids.length, 5);

            // Test list
            mongish.Users.list({parent_id: parent._id}, this);
          },
          function(err, kids) {
            if (err) done(err);
            assert.ok(kids);
            assert.equal(kids.length, 5);

            // Test list with inflate
            mongish.Users.list({parent_id: parent._id}, {inflate: {parent: { collection: 'user', primaryEmail: 1, username: 1}}}, this);
          },
          function(err, kids) {
            if (err) done(err);
            assert.ok(kids);
            assert.ok(kids[0]);
            assert.ok(kids[0].parent);
            assert.ok(kids[0].parent._id);
            assert.ok(kids[0].parent.primaryEmail);
            assert.ok(kids[0].parent.username);
            done();
          }
        );
      });
    });
  });

  describe('#available()', function() {
    it('should check if key/val is available', function(done) {
      var email = uuid.v1();
      var username = uuid.v1();

      // Create doc
      mongish.Users.create({primaryEmail: email, username: username}, function(err, doc) {
        if (err) done(err);

        // Test available
        mongish.Users.available({primaryEmail: email}, function(err, available) {
          if (err) done(err);
          assert.ok(!available);
          done();
        });
      });
    });
  });

});
