"use strict";

var nub = require('nub'),
    common = require('./common'),
    isSetType = common.isSetType;

function Attribute(name, value, type){
    this.name = name;
    this.value = value;
    this.type = type;

    if(isSetType(this.type) && (nub(this.value).length !== this.value.length)){
        throw new Error('Input collection contains dupes');
    }
    // if numeric type, make sure its a sumber

    // if string type make sure its a string

    if(this.name === ''){
        throw new Error("Empty attribute name");
    }
}

Object.defineProperty(Attribute.prototype, "description", {
    'get': function(){
        return {
            'AttributeName': this.name,
            'AttributeType': this.type
        };
    }
});

Attribute.fromData = function(data){
    return new Attribute(data.AttributeName, data.AttributeValue, data.AttributeType);
};

Attribute.fromHash = function(name, obj){
    if(typeof obj === 'object'){
        var k = Object.keys(obj);
        return new Attribute(name, obj[k[0]], k[0]);
    }
};

Attribute.prototype.asHash = function(){
    var name = this.name,
        type = this.type,
        value = this.value,
        result = {};

    result[name] = {};
    result[name][type] = value;
    return result;
};


Attribute.prototype.equals = function(otherObject){
    return Object.keys(otherObject).every(function(key){
        return this[key] === otherObject[key];
    }.bind(this));
};

module.exports = Attribute;