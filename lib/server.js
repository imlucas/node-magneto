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
        req.log.error({'data': data,
            'operation': operation});
        try{
            response = DB.process(operation, data);

            req.log.error({'response': response});

            res.send(response);
        }
        catch(e){
            console.error(e);
            next(e);
        }
    });
});

server.get('/shutdown', function(){
    server.close();
});

module.exports = server;