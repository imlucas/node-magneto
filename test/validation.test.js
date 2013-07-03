"use strict";

var assert = require('assert'),
    async = require('async'),
    helpers = require('./helpers');


describe('Validation', function(){
    beforeEach(helpers.beforeEach);
    afterEach(helpers.afterEach);

    var username = 'lucas';

    it("should raise a validation error trying to insert duplicates in a string set", function(done){
        async.waterfall([
            helpers.createTable,
            function(err, callback){
                var params = {
                    'TableName': 'users',
                    'Item': {
                        'username': {
                            'S': username
                        },
                        'devices': {
                            'SS': ['ios', 'ios']
                        }
                    }
                };
                helpers.dynamo.putItem(params, function(err, data){
                    assert.equal(err.name, 'ValidationException');
                    assert.equal(err.statusCode, 400);
                    callback();
                });
            }
        ], done);

    });

    it("should raise a validation error trying to update a string set with duplicates", function(done){
        async.waterfall([
            helpers.createTable,
            function(err, callback){
                var params = {
                    'TableName': 'users',
                    'Item': {
                        'username': {
                            'S': username
                        },
                        'devices': {
                            'SS': ['ios']
                        }
                    }
                };
                helpers.dynamo.putItem(params, callback);
            },
            function(err, callback){
                var params = {
                    'TableName': 'users',
                    'Key': {
                        'username': {
                            'S': username
                        }
                    },
                    'AttributeUpdates': {
                        'devices': {
                            'Value': {
                                'SS': ['ios']
                            },
                            'Action': 'ADD'
                        }
                    }
                };
                helpers.dynamo.updateItem(params, function(err, data){
                    assert.equal(err.name, 'ValidationException');
                    assert.equal(err.statusCode, 400);
                    callback();
                });
            }
        ], done);
    });
});