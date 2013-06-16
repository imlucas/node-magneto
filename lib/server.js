"use strict";

var express = require('express'),
    DB = require('./db'),
    app = express(),
    util = require('util'),
    debug = require('debug')('magneto:server');


var handleError = function(err, req, res, next){
    debug('Exception raised: ' +  err.message + '. \nOp: ' + req.header('x-amz-target') + '\n' + req.body);
    if(err.stack){
        debug(err.stack);
    }
    var body = {
        '__type': 'com.amazonaws.dynamodb.v20111205#' + err.name,
        'message': err.message
    };
    res.send(err.code || 400, body);
};

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

        debug(operation + ' => ' + util.inspect(data, false, 10));
        try{
            response = DB.process(operation, data);
            debug(operation + ' <= ' + util.inspect(response, false, 10));
            res.send(response);
        }
        catch(err){
            handleError(err, req, res, next);
        }
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
module.exports.DB = DB;