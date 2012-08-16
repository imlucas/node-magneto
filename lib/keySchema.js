"use strict";

var Attribute = require('./attribute');

function KeySchema(data){
    this.hashKey = Attribute.fromData(data.HashKeyElement);
    this.rangeKey = undefined;

    if(data.RangeKeyElement){
        this.rangeKey = Attribute.fromData(data.RangeKeyElement);
    }
}

Object.defineProperty(KeySchema.prototype, "description", {
    'get': function(){
        var desc = {'HashKeyElement': this.hashKey.description};
        if(this.rangeKey){
            desc.RangeKeyElement = this.rangeKey.description;
        }
        return desc;
    }
});

module.exports = KeySchema;