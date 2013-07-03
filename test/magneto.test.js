"use strict";

var assert = require("assert"),
    async = require('async'),
    util = require('util'),
    debug = require('debug')('magneto:test'),
    helpers = require('./helpers');

function createTable(done){
    var params = {
        'TableName': 'users',
        'KeySchema': [
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            }
        ],
        'LocalSecondaryIndexes': [],
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    };

    helpers.dynamo.createTable(params, done);
}

function createUser(username, email, done){
    var params = {
        'TableName': 'users',
        'Item': {
            'username': {
                'S': username
            },
            'email': {
                'S': email
            }
        }
    };
    helpers.dynamo.putItem(params, done);
}

function getUser(username, done){
    var params = {
        'TableName': 'users',
        'Key': {
            'username': {
                'S': username
            }
        }
    };
    helpers.dynamo.getItem(params, done);
}

describe('Magneto @func', function(){
    beforeEach(helpers.beforeEach);
    afterEach(helpers.afterEach);

    describe('Table', function(){
        it('should create tables', function(done){
            createTable(function(err, data){
                if(err){
                    return done(err);
                }
                assert(data.TableDescription.CreationDateTime, 'should have a creation date');
                assert.equal(data.TableDescription.ItemCount, 0, 'should be empty');
                assert.equal(data.TableDescription.ProvisionedThroughput.ReadCapacityUnits, 5);
                assert.equal(data.TableDescription.ProvisionedThroughput.WriteCapacityUnits, 5);
                assert.equal(data.TableDescription.TableStatus, 'ACTIVE');
                done();
            });
        });

        it('should list tables', function(done){
            createTable(function(err, data){
                if(err){
                    return done(err);
                }
                helpers.dynamo.listTables({}, function(err, data){
                    if(err){
                        return done(err);
                    }
                    assert.deepEqual(data.TableNames, ['users']);
                    done();
                });
            });
        });
        it('should describe a table', function(done){
            createTable(function(err, data){
                if(err){
                    return done(err);
                }
                helpers.dynamo.describeTable({'TableName': 'users'}, function(err, data){
                    if(err){
                        return done(err);
                    }
                    assert.equal(data.Table.TableName, 'users');
                    done();
                });
            });
        });



        it('should put an item and get it back', function(done){
            async.series([
                createTable,
                function put(callback){
                    createUser('lucas', 'wombats@imlucas.com', callback);
                },
                function get(callback){
                    getUser('lucas', function(err, data){
                        assert(data.Item !== undefined);
                        assert.equal(data.Item.username.S, 'lucas');
                        assert.equal(data.Item.email.S, 'wombats@imlucas.com');
                        callback(err);
                    });
                }
            ], done);
        });

        it('should update an item', function(done){
            async.series([
                createTable,
                function put(callback){
                    createUser('lucas', 'wombats@imlucas.com', callback);
                },
                function update(callback){
                    var params = {
                        'TableName': 'users',
                        'Key': {
                            'username': {
                                'S': 'lucas'
                            }
                        },
                        'AttributeUpdates': {
                            'email': {
                                'Action': 'PUT',
                                'Value': {
                                    'S': 'kangaroos@imlucas.com'
                                }
                            }
                        }
                    };
                    helpers.dynamo.updateItem(params, callback);
                },
                function get(callback){
                    getUser('lucas', function(err, data){
                        assert(data.Item !== undefined);
                        assert.equal(data.Item.username.S, 'lucas');
                        assert.equal(data.Item.email.S, 'kangaroos@imlucas.com');
                        callback(err);
                    });
                }
            ], done);
        });

        it('should delete an item', function(done){
            async.series([
                createTable,
                function put(callback){
                    createUser('lucas', 'wombats@imlucas.com', callback);
                },
                function get(callback){
                    getUser('lucas', function(err, data){
                        assert(data.Item !== undefined);
                        assert.equal(data.Item.username.S, 'lucas');
                        assert.equal(data.Item.email.S, 'wombats@imlucas.com');
                        callback(err);
                    });
                },
                function remove(callback){
                    var params = {
                        'TableName': 'users',
                        'Key': {
                            'username': {
                                'S': 'lucas'
                            }
                        }
                    };
                    helpers.dynamo.deleteItem(params, callback);
                },
                function getAgain(callback){
                    getUser('lucas', function(err, data){
                        assert(data.Item === undefined);
                        callback(err);
                    });
                }
            ], done);
        });
    });





    // describe('Write Batch', function(){
    //     it('should handle batch write', function(done){
    //         sequence().then(function(next){
    //             // Create a table
    //             db.add({
    //                 'name': "myTable",
    //                 'schema': {
    //                     'username': String
    //                 },
    //                 'throughput': {
    //                     'read': 1000,
    //                     'write': 10
    //                 }
    //             }).save(function(err, table){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.put("myTable", [
    //                     {
    //                         'username': 'lucas',
    //                         'email': 'lucas@ex.fm'
    //                     },
    //                     {
    //                         'username': 'jm',
    //                         'email': 'jm@ex.fm'
    //                     }
    //                 ]);
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.get("myTable", [{
    //                     'username': 'lucas'
    //                 }, {
    //                     'username': 'jm'
    //                 }]);
    //             }).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.myTable[0].username, 'lucas');
    //                 assert.equal(data.myTable[0].email, 'lucas@ex.fm');
    //                 assert.equal(data.myTable[1].username, 'jm');
    //                 assert.equal(data.myTable[1].email, 'jm@ex.fm');
    //                 done();
    //             });
    //         });
    //     });
    //     it('should handle batch deletes with batch item', function(done){
    //         sequence().then(function(next){
    //             // Create a table
    //             db.add({
    //                 'name': "myTable",
    //                 'schema': {
    //                     'username': String
    //                 },
    //                 'throughput': {
    //                     'read': 1000,
    //                     'write': 10
    //                 }
    //             }).save(function(err, table){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.put("myTable", [
    //                     {
    //                         'username': 'lucas',
    //                         'email': 'lucas@ex.fm'
    //                     },
    //                     {
    //                         'username': 'jm',
    //                         'email': 'jm@ex.fm'
    //                     }
    //                 ]);
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.get("myTable", [{
    //                     'username': 'lucas'
    //                 }, {
    //                     'username': 'jm'
    //                 }]);
    //             }).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.myTable[0].username, 'lucas');
    //                 assert.equal(data.myTable[0].email, 'lucas@ex.fm');
    //                 assert.equal(data.myTable[1].username, 'jm');
    //                 assert.equal(data.myTable[1].email, 'jm@ex.fm');
    //                 done();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.destroy("myTable", [{
    //                     'username': 'lucas'
    //                 }, {
    //                     'username': 'jm'
    //                 }]);
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.put("myTable", [
    //                     {
    //                         'username': 'lucas',
    //                         'email': 'lucas@ex.fm'
    //                     },
    //                     {
    //                         'username': 'jm',
    //                         'email': 'jm@ex.fm'
    //                     }
    //                 ]);
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.myTable.length, 0);
    //                 next();
    //             });
    //         });
    //     });
    // });

    // describe('Table', function(){
    //     it('should support all basic operations', function(done){
    //         sequence().then(function(next){
    //             // Create a table
    //             db.add({
    //                 'name': "myTable",
    //                 'schema': {
    //                     'id': String,
    //                     'date': Number
    //                 },
    //                 'throughput': {
    //                     'read': 10,
    //                     'write': 10
    //                 }
    //             }).save(function(err, table){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             // list tables
    //             db.listTables({}, function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             // Put new object
    //             db.get('myTable').put({
    //                 'id': 'lucas',
    //                 'date': 1
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });

    //         }).then(function(next){
    //             // Make sure creation worked
    //             db.get('myTable').get({
    //                 'id': 'lucas',
    //                 'date': 1
    //             }).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             // Update object with new properties
    //             db.get('myTable').get({
    //                 'id': 'lucas',
    //                 'date': 1
    //             }).update(function(){
    //                 this.put("email", "lucas@ex.fm");
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').get({
    //                 'id': 'lucas',
    //                 'date': 1
    //             }).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.email, "lucas@ex.fm");
    //                 next();
    //             });
    //         }).then(function(next){
    //             // Update object and use adds
    //             db.get('myTable').get({'id': 'lucas', 'date': 1}).update(function(){
    //                 this.add("followers", 100000);
    //                 this.add("tags", ["electronic", "house"]);
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             // Delete item
    //             db.get('myTable').get({
    //                 'id': 'lucas',
    //                 'date': 1
    //             }).destroy(function(err){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             // Make sure it's deleted.
    //             db.get('myTable').get({'id': 'lucas', 'date': 1}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data, undefined,
    //                     "Called delete item but still got item");
    //                 done();
    //             });
    //         });
    //     });




    // });

    // describe('Query', function(){
    //     it('should return 2 items for a basic hash + range table',
    //             function(done){
    //         sequence().then(function(next){
    //             // Create a table
    //             db.add({
    //                 'name': "myTable",
    //                 'schema': {
    //                     'id': String,
    //                     'date': Number
    //                 },
    //                 'throughput': {
    //                     'read': 10,
    //                     'write': 10
    //                 }
    //             }).save(function(err, table){
    //                 assert.ifError(err);

    //                 var keySchema = table.KeySchema,
    //                     throughput = table.ProvisionedThroughput;

    //                 assert.equal(table.TableName, "myTable");
    //                 assert.equal(table.TableStatus, "ACTIVE");

    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').put({
    //                 'id': 'lucas',
    //                 'date': 1
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').put({
    //                 'id': 'lucas',
    //                 'date': 2
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').query({
    //                 'id': 'lucas'
    //             }).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 2);

    //                 assert.equal(data[0].date, 1);
    //                 assert.equal(data[0].id, 'lucas');

    //                 assert.equal(data[1].date, 2);
    //                 assert.equal(data[1].id, 'lucas');
    //                 done();
    //             });
    //         });
    //     });
    // });

    // describe('Get Batch', function(){
    //     it('should handle batch get item', function(done){
    //         sequence().then(function(next){
    //             // Create a table
    //             db.add({
    //                 'name': "myTable",
    //                 'schema': {
    //                     'username': String
    //                 },
    //                 'throughput': {
    //                     'read': 1000,
    //                     'write': 10
    //                 }
    //             }).save(function(err, table){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').put({
    //                 'username': 'dan',
    //                 'email': 'dan@ex.fm'
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').put({
    //                 'username': 'lucas',
    //                 'email': 'lucas@ex.fm'
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get('myTable').put({
    //                 'username': 'jm',
    //                 'email': 'jm@ex.fm'
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.get("myTable", [{
    //                     'username': 'lucas'
    //                 }, {
    //                     'username': 'jm'
    //                 }]);
    //             }).fetch(function(err, data){
    //                 assert.ifError(err);

    //                 assert.equal(data.myTable[0].username, 'lucas');
    //                 assert.equal(data.myTable[0].email, 'lucas@ex.fm');
    //                 assert.equal(data.myTable[1].username, 'jm');
    //                 assert.equal(data.myTable[1].email, 'jm@ex.fm');

    //                 done();
    //             });
    //         });
    //     });
    // });


    // describe('Scan', function(){
    //     var createScanData = function(){
    //         var d = when.defer();
    //         sequence().then(function(next){
    //             db.add({
    //                 'name': "myTable",
    //                 'schema': {
    //                     'username': String
    //                 },
    //                 'throughput': {
    //                     'read': 1000,
    //                     'write': 10
    //                 }
    //             }).save(function(err, table){
    //                 assert.ifError(err);
    //                 next();
    //             });
    //         }).then(function(next){
    //             db.get(function(){
    //                 this.put("myTable", [
    //                     {
    //                         'username': 'lucas',
    //                         'email': 'lucas@ex.fm',
    //                         'admin': 1,
    //                         'followers': 0,
    //                         'loved': [1,2]
    //                     },
    //                     {
    //                         'username': 'jm',
    //                         'email': 'jm@ex.fm',
    //                         'followers': 10,
    //                         'loved': [3]
    //                     }
    //                 ]);
    //             }).save(function(err, data){
    //                 assert.ifError(err);
    //                 d.resolve();
    //             });
    //         });
    //         return d.promise;
    //     };
    //     it('should return all with no conditions', function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan().fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 2, "Should return 2 users");
    //                 done();
    //             });
    //         });
    //     });

    //     it('should return all with NOT_NULL on hash', function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'username': {"!=": null}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 2, "Should return 2 users");
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport EQ", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'email': {"==": 'lucas@ex.fm'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport GT", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'followers': {">": '0'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport GE", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'followers': {">=": '10'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport LT", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'followers': {"<": '10'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport LE", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'followers': {"<=": '0'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport NE", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'followers': {"!=": '0'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 assert.equal(data[0].username, 'jm');
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport BEGINS_WITH", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'username': {'startsWith': 'lu'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 assert.equal(data[0].username, 'lucas');
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport CONTAINS", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'username': {'contains': 'ucas'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport NOT_CONTAINS", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'username': {'!contains': 'ucas'}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 assert.equal(data[0].username, 'jm');
    //                 done();
    //             });
    //         });
    //     });

    //     it("should suppport BETWEEN", function(done){
    //         createScanData().then(function(next){
    //             db.get("myTable").scan({'followers': {">=": [5, 15]}}).fetch(function(err, data){
    //                 assert.ifError(err);
    //                 assert.equal(data.length, 1);
    //                 assert.equal(data[0].username, 'jm');
    //                 done();
    //             });
    //         });
    //     });
    // });
});