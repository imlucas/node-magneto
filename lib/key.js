"use strict";

var Attribute = require('./attribute'),
    common = require('./common'),
    merge = common.merge;

function Key(){
    this.primary = null;
    this.range = null;
}

Key.fromData = function(keyData, keySchema){
    var key = new Key();
    // validate_key_data(key_data, key_schema)
    key.primary = Attribute.fromHash(keySchema.hashKey.name,
        keyData);

    if(keySchema.rangeKey){
        key.range = Attribute.fromHash(keySchema.rangeKey.name,
            keyData);
    }
    return key;
};

Key.fromSchema = function(data, keySchema){
    var key = new Key();
    //validate_key_schema(data, key_schema)
    key.primary = Key.createAttribute(keySchema.hashKey, data);

    if(keySchema.rangeKey){
        key.range = Key.createAttribute(keySchema.rangeKey, data);
    }
    return key;
};

Key.createAttribute = function(key, data){
    return Attribute.fromHash(key.name, data[key.name]);
};

Key.prototype.asHash = function(){
    var result = this.primary.asHash();
    if(this.range){
        merge(result, this.range.asHash());
    }
    return result;
};

Key.prototype.asKeyHash = function(){
    var pt = this.primary.type,
        pv = this.primary.value, rt, rv, result = {
        'HashKeyElement': {pt: pv}
    };

    if(this.range){
        rt = this.range.type;
        rv = this.range.value;
        merge(result, {'RangeKeyElement': {rt: rv}});
    }
    return result;
};

Key.prototype.toString = function(){
    var out = "Key(" + this.primary.type+ "=" +this.primary.value;
    if(this.range){
        out += ", " + this.range.type + "=" + this.range.value;
    }
    out += ")";
    return out;
};

module.exports = Key;