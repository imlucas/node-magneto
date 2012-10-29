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

app.use(function(err, req, res, next){
    log.error(err.stack);
    res.send(err.code, err.message);
});

app.post('/', function(req, res, next){
    req.body = '';

    req.on('data', function (chunk) {
      req.body += chunk.toString('utf8');
    });

    req.on('error', function (err) {
      return next(err);
    });

    req.on('end', function (){
        var data = (req.body) ? JSON.parse(req.body) : {},
        opRegex = /DynamoDB_\d+\.([a-zA-z]+)/,
        operation = opRegex.exec(req.header('x-amz-target'))[1],
        response;

        log.debug({'data': data});

        response = DB.process(operation, data);
        log.debug({
            'data': data,
            'operation': operation,
            'response': response
        });

        res.send(response);
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