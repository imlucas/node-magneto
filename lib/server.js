"use strict";

var express = require('express'),
    DB = require('./db');

var app = express();

var Logger = require('bunyan');
var log = new Logger({
  name: 'magneto',
  streams: [
    {
      stream: process.stdout,
      level: 'debug'
    }
  ],
  serializers: {
    req: Logger.stdSerializers.req
  }
});

app.post('/', function(req, res, next){
    req.body = '';

    req.on('data', function (chunk) {
      req.body += chunk.toString('utf8');
    });

    req.on('error', function (err) {
      return next(err);
    });

    req.on('end', function () {
        var data = (req.body) ? JSON.parse(req.body) : {},
        opRegex = /DynamoDB_\d+\.([a-zA-z]+)/,
        operation = opRegex.exec(req.header('x-amz-target'))[1],
        response;

        try{
            response = DB.process(operation, data);
            log.debug({
                'data': data,
                'operation': operation,
                'response': response
            });

            res.send(response);
        }
        catch(e){
            console.error(e);
            next(e);
        }
    });
});

app.get('/shutdown', function(){
    app.close();
});

module.exports = app;
module.exports.log = log;
module.exports.setLogLevel = function(level){
    log._level = level;
    return log;
};