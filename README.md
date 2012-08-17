# Magneto

A port of [fake dynamo](https://github.com/SaturnPolly/fake_dynamo) to node.js.

Still very much a work in progress, but functional for our purposes.

## Todo
 * More tests

## Install

    npm install -g magneto

## Usage

    ./magneto <port>


Use [exfm's dynamo fork](https://github.com/exfm/dynamo).

    var dynamo = require('dynamo'),
    client = dynamo.createClient();
    client.useSession = false;

    var db = client.get('us-east-1');
        db.host = 'localhost';
        db.port = 8080;

For more examples, have a look at the [tests](https://github.com/exfm/node-magneto/blob/master/test/magneto.test.js).


