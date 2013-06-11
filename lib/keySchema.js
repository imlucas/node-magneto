"use strict";

var Attribute = require('./attribute');

function KeySchema(data){
    this.description = data;
    this.hashKey = undefined;
    this.rangeKey = undefined;
    for(var i =0; i < data.length; i++){
        if(data[i].KeyType === 'HASH'){
            this.hashKey = {
                'name': data[i].AttributeName
            };
        }
        else {
            this.rangeKey = {
                'name': data[i].AttributeName
            };
        }
    }
}

module.exports = KeySchema;