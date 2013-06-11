"use strict";

var nub = require('nub'),
    common = require('./common'),
    isSetType = common.isSetType,
    errors = require('./errors'),
    debug = require('debug')('magneto:attribute');

function Attribute(name, value, type){
    this.name = name;
    this.value = value;
    this.type = type;

    if(isSetType(this.type) && this.value && (nub(this.value).length !== this.value.length)){
        throw new errors.ValidationException('Input collection '+ name +' contains duplicates ' + value);
    }
    if(isSetType(this.type) && this.value.length === 0){
        // More helpful message but just so you can find it easily
        // An AttributeValue may not contain an empty set.
        throw new errors.ValidationException('AttributeValue '+this.name+' may not contain an empty set.');
    }

    if(type === 'N'){
        this.value = Number(value);
    }
    else if(type === 'S'){
        this.value = String(value);
    }
    else if(type === 'B'){
        this.value = new Buffer(value, 'base64').toString('value');
    }

    if(this.name === ''){
        throw new errors.ValidationException("Empty attribute name");
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
        // @todo (lucas) GEWWEWEEWWWW
        if(typeof obj[k[0]] === 'object'){
            return Attribute.fromHash(name, obj[k[0]]);
        }
        return new Attribute(name, obj[k[0]], k[0]);
    }
};

Attribute.prototype.asHash = function(){
    var name = this.name,
        type = this.type,
        value = this.value,
        result = {};

    result[name] = {};

    if(this.type === 'B'){
        value = new Buffer(value, 'base64').toString('binary');
    }

    result[name][type] = value;
    return result;
};


Attribute.prototype.equals = function(otherObject){
    return Object.keys(otherObject).every(function(key){
        return this[key] === otherObject[key];
    }.bind(this));
};

module.exports = Attribute;