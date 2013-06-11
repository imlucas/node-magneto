"use strict";

var Attribute = require('./attribute');

function KeySchema(data){
    this.description = data.KeySchema;
}

module.exports = KeySchema;