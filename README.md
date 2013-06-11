# Magneto

[![Build Status](https://secure.travis-ci.org/exfm/node-magneto.png)](http://travis-ci.org/exfm/node-magneto)

A port of [fake dynamo](https://github.com/ananthakumaran/fake_dynamo) to node.js.

Very handy for just getting to know dynamo or making your tests easier to deal with.

## Install

    npm install -g magneto

## Test

    npm test

## Usage

    magneto <port>

## Todo
[see issues](https://github.com/exfm/node-magneto/issues)


To use in testing with the official AWS SDK

    var aws = require('aws-sdk'),
        magneto = require('magneto');

    magneto.patchClient(aws);



For more examples, have a look at the [tests](https://github.com/exfm/node-magneto/blob/master/test/magneto.test.js).


