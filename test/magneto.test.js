"use strict";

var assert = require("assert"),
    sequence = require('sequence'),
    when = require('when'),
    plog = require('plog');

var dynamo = require('dynamo'),
    client = dynamo.createClient();

client.useSession = false;

var connected = false,
    magneto = require('../');

var db = client.get('us-east-1');
    db.host = 'localhost';
    db.port = 8080;

plog.find(/magneto*/)
    .file('magneto-test.log')
    .level('silly')
    .remove('console');


describe('Magneto @func', function(){
    beforeEach(function(done){
        if(connected === false){
            magneto.listen(8080, function(){
                connected = true;
                done();
            });
        }
        else{
            done();
        }
    });
    afterEach(function(done){
        db.get('myTable').destroy(function(err){
            done();
        });
    });

    describe('Table', function(){
        it('should support all basic operations', function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'id': String,
                        'date': Number
                    },
                    'throughput': {
                        'read': 10,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                // list tables
                db.listTables({}, function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                // Put new object
                db.get('myTable').put({
                    'id': 'lucas',
                    'date': 1
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });

            }).then(function(next){
                // Make sure creation worked
                db.get('myTable').get({
                    'id': 'lucas',
                    'date': 1
                }).fetch(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                // Update object with new properties
                db.get('myTable').get({
                    'id': 'lucas',
                    'date': 1
                }).update(function(){
                    this.put("email", "lucas@ex.fm");
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').get({
                    'id': 'lucas',
                    'date': 1
                }).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.email, "lucas@ex.fm");
                    next();
                });
            }).then(function(next){
                // Update object and use adds
                db.get('myTable').get({'id': 'lucas', 'date': 1}).update(function(){
                    this.add("followers", 100000);
                    this.add("tags", ["electronic", "house"]);
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                // Delete item
                db.get('myTable').get({
                    'id': 'lucas',
                    'date': 1
                }).destroy(function(err){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                // Make sure it's deleted.
                db.get('myTable').get({'id': 'lucas', 'date': 1}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data, undefined,
                        "Called delete item but still got item");
                    done();
                });
            });
        });

        it("should raise a validation error trying to insert duplicates in a string set", function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'id': String,
                        'date': Number
                    },
                    'throughput': {
                        'read': 10,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'id': 'lucas', 'date': 1,
                    "tags": ["electronic", "electronic"]
                }).save(function(err, data){
                    assert.equal(err.name, 'com.amazonaws.dynamodb.v20111205#ValidationException');
                    assert.equal(err.statusCode, 400);
                    done();
                });
            });
        });

        it("should raise a validation error trying to update a string set with duplicates", function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'id': String,
                        'date': Number
                    },
                    'throughput': {
                        'read': 10,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'id': 'lucas',
                    'date': 1,
                    "tags": ["electronic", "rock", "pop"]
                }).save(function(err, data){
                    next();
                });
            })
            .then(function(next){
                db.get('myTable').get({'id': 'lucas', 'date': 1}).update(function(){
                    this.add("tags", ["electronic"]);
                }).save(function(err, data){
                    assert.equal(err.name, 'com.amazonaws.dynamodb.v20111205#ValidationException');
                    assert.equal(err.statusCode, 400);
                    done();
                });
            });
        });


    });

    describe('Query', function(){
        it('should return 2 items for a basic hash + range table',
                function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'id': String,
                        'date': Number
                    },
                    'throughput': {
                        'read': 10,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);

                    var keySchema = table.KeySchema,
                        throughput = table.ProvisionedThroughput;

                    assert.equal(table.TableName, "myTable");
                    assert.equal(table.TableStatus, "ACTIVE");

                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'id': 'lucas',
                    'date': 1
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'id': 'lucas',
                    'date': 2
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').query({
                    'id': 'lucas'
                }).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 2);

                    assert.equal(data[0].date, 1);
                    assert.equal(data[0].id, 'lucas');

                    assert.equal(data[1].date, 2);
                    assert.equal(data[1].id, 'lucas');
                    done();
                });
            });
        });
    });

    describe('Get Batch', function(){
        it('should handle batch get item', function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'username': String
                    },
                    'throughput': {
                        'read': 1000,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'username': 'dan',
                    'email': 'dan@ex.fm'
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'username': 'lucas',
                    'email': 'lucas@ex.fm'
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get('myTable').put({
                    'username': 'jm',
                    'email': 'jm@ex.fm'
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.get("myTable", [{
                        'username': 'lucas'
                    }, {
                        'username': 'jm'
                    }]);
                }).fetch(function(err, data){
                    assert.ifError(err);

                    assert.equal(data.myTable[0].username, 'lucas');
                    assert.equal(data.myTable[0].email, 'lucas@ex.fm');
                    assert.equal(data.myTable[1].username, 'jm');
                    assert.equal(data.myTable[1].email, 'jm@ex.fm');

                    done();
                });
            });
        });
    });

    describe('Write Batch', function(){
        it('should handle batch write', function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'username': String
                    },
                    'throughput': {
                        'read': 1000,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.put("myTable", [
                        {
                            'username': 'lucas',
                            'email': 'lucas@ex.fm'
                        },
                        {
                            'username': 'jm',
                            'email': 'jm@ex.fm'
                        }
                    ]);
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.get("myTable", [{
                        'username': 'lucas'
                    }, {
                        'username': 'jm'
                    }]);
                }).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.myTable[0].username, 'lucas');
                    assert.equal(data.myTable[0].email, 'lucas@ex.fm');
                    assert.equal(data.myTable[1].username, 'jm');
                    assert.equal(data.myTable[1].email, 'jm@ex.fm');
                    done();
                });
            });
        });
        it('should handle batch deletes with batch item', function(done){
            sequence().then(function(next){
                // Create a table
                db.add({
                    'name': "myTable",
                    'schema': {
                        'username': String
                    },
                    'throughput': {
                        'read': 1000,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.put("myTable", [
                        {
                            'username': 'lucas',
                            'email': 'lucas@ex.fm'
                        },
                        {
                            'username': 'jm',
                            'email': 'jm@ex.fm'
                        }
                    ]);
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.get("myTable", [{
                        'username': 'lucas'
                    }, {
                        'username': 'jm'
                    }]);
                }).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.myTable[0].username, 'lucas');
                    assert.equal(data.myTable[0].email, 'lucas@ex.fm');
                    assert.equal(data.myTable[1].username, 'jm');
                    assert.equal(data.myTable[1].email, 'jm@ex.fm');
                    done();
                });
            }).then(function(next){
                db.get(function(){
                    this.destroy("myTable", [{
                        'username': 'lucas'
                    }, {
                        'username': 'jm'
                    }]);
                }).save(function(err, data){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.put("myTable", [
                        {
                            'username': 'lucas',
                            'email': 'lucas@ex.fm'
                        },
                        {
                            'username': 'jm',
                            'email': 'jm@ex.fm'
                        }
                    ]);
                }).save(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.myTable.length, 0);
                    next();
                });
            });
        });
    });
    describe('Scan', function(){
        var createScanData = function(){
            var d = when.defer();
            sequence().then(function(next){
                db.add({
                    'name': "myTable",
                    'schema': {
                        'username': String
                    },
                    'throughput': {
                        'read': 1000,
                        'write': 10
                    }
                }).save(function(err, table){
                    assert.ifError(err);
                    next();
                });
            }).then(function(next){
                db.get(function(){
                    this.put("myTable", [
                        {
                            'username': 'lucas',
                            'email': 'lucas@ex.fm',
                            'admin': 1,
                            'followers': 0,
                            'loved': [1,2]
                        },
                        {
                            'username': 'jm',
                            'email': 'jm@ex.fm',
                            'followers': 10,
                            'loved': [3]
                        }
                    ]);
                }).save(function(err, data){
                    assert.ifError(err);
                    d.resolve();
                });
            });
            return d.promise;
        };
        it('should return all with no conditions', function(done){
            createScanData().then(function(next){
                db.get("myTable").scan().fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 2, "Should return 2 users");
                    done();
                });
            });
        });

        it('should return all with NOT_NULL on hash', function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'username': {"!=": null}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 2, "Should return 2 users");
                    done();
                });
            });
        });

        it("should suppport EQ", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'email': {"==": 'lucas@ex.fm'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    done();
                });
            });
        });

        it("should suppport GT", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'followers': {">": '0'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    done();
                });
            });
        });

        it("should suppport GE", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'followers': {">=": '10'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    done();
                });
            });
        });

        it("should suppport LT", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'followers': {"<": '10'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    done();
                });
            });
        });

        it("should suppport LE", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'followers': {"<=": '0'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    done();
                });
            });
        });

        it("should suppport NE", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'followers': {"!=": '0'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    assert.equal(data[0].username, 'jm');
                    done();
                });
            });
        });

        it("should suppport BEGINS_WITH", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'username': {'startsWith': 'lu'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    assert.equal(data[0].username, 'lucas');
                    done();
                });
            });
        });

        it("should suppport CONTAINS", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'username': {'contains': 'ucas'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    done();
                });
            });
        });

        it("should suppport NOT_CONTAINS", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'username': {'!contains': 'ucas'}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    assert.equal(data[0].username, 'jm');
                    done();
                });
            });
        });

        it("should suppport BETWEEN", function(done){
            createScanData().then(function(next){
                db.get("myTable").scan({'followers': {">=": [5, 15]}}).fetch(function(err, data){
                    assert.ifError(err);
                    assert.equal(data.length, 1);
                    assert.equal(data[0].username, 'jm');
                    done();
                });
            });
        });



    });
});