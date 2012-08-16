"use strict";

var restify = require('restify'),
    DB = require('./DB');

var server = restify.createServer();

server.post('/', function(req, res, next){
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

            req.log.debug({
                'data': data,
                'operation': operation,
                'response': response
            });

            res.send(response);
        }
        catch(e){
            next(e);
        }
    });
});

server.get('/shutdown', function(){
    server.close();
});

module.exports = server;