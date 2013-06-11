"use strict";

var server = require('./lib/server');
module.exports = server;

module.exports.patchClient = function(aws){
    aws.config.update({
        'accessKeyId': 'AKID',
        'secretAccessKey': 'SECRET',
        'region': 'us-east-1',
        'endpoint': 'http://localhost:8080'
    });
    return aws;
};