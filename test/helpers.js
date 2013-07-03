"use strict";
var assert = require("assert"),
    magneto = require('../'),
    aws = require('aws-sdk');

magneto.patchClient(aws);

var dynamo = module.exports.dynamo = new aws.DynamoDB();

module.exports.createTable = function(fn){
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
        'ProvisionedThroughput': {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    };

    dynamo.createTable(params, fn);
};
var connected = false;

module.exports.beforeEach = function(done){
    if(connected === false){
        magneto.listen(8080, function(){
            connected = true;
            done();
        });
    }
    else{
        done();
    }
};

module.exports.afterEach = function(done){
    dynamo.deleteTable({'TableName': 'users'}, function(err){
        done();
    });
};
