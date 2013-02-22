"use strict";

var express = require('express'),
    DB = require('./db'),
    app = express(),
    util = require('util'),
    log = require('plog')('magneto.server').level('silly');

app.use(function(err, req, res, next){
    log.error(err.message);
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

        log.debug(operation + ' => ' + util.inspect(data, false, 10));
        response = DB.process(operation, data);
        log.debug(operation + ' <= ' + util.inspect(response, false, 10));
        res.send(response);
    });
});

app.get('/:tableName', function(req, res){
    res.send(DB.tables[req.param('tableName')]);
});

app.get('/:tableName/items', function(req, res){
    res.send(DB.tables[req.param('tableName')].items);
});

app.get('/:tableName/stats', function(req, res){
    res.send(DB.tables[req.param('tableName')].stats);
});

app.get('/', function(req, res){
    res.send(DB.tables);
});

app.get('/shutdown', function(){
    app.close();
});

module.exports = app;
module.exports.log = log;

// Deprecated.
module.exports.setLogLevel = function(level){
    // log._level = level;
    return log;
};