"use strict";

var server = require('./lib/server');
module.exports = server;

module.exports.patchClient = function(aws, port){
    port = port || 8080;

    aws.config.update({
        'accessKeyId': 'AKID',
        'secretAccessKey': 'SECRET',
        'region': 'us-east-1',
        'endpoint': 'http://localhost:' + port
    });
    return aws;
};