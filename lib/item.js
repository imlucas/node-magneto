"use strict";
var Key = require('./key'),
    Attribute = require('./attribute'),
    common = require('./common'),
    merge = common.merge,
    errors = require('./errors'),
    nub = require('nub');


function Item(){
    this.key = null;
    this.attributes = {};
}

Item.fromData = function(data, keySchema){
    var item = new Item();
    item.key = Key.fromSchema(data, keySchema);
    Object.keys(data).forEach(function(name){
        var value = data[name];
        if(!item.key.hasOwnProperty(name)){
            item.attributes[name] = Attribute.fromHash(name, value);
        }
    });
    return item;
};

Item.fromKey = function(key){
    var item = new Item();
    item.key = key;
    return item;
};

Item.prototype.asHash = function(){
    var result = {};
    merge(result, this.key.asHash());
    Object.keys(this.attributes).forEach(function(name){
        var attribute = this.attributes[name];
        merge(result, attribute.asHash());
    }.bind(this));
    return result;
};

Item.prototype.update = function(name, data){
    if(this.key.hasOwnProperty(name)){
        throw new errors.ValidationException("Cannot update attribute #{name}. This attribute is part of the key");
    }

    var newValue = data.Value,
        action = data.Action || 'PUT',
        methodName = action.toLowerCase();

    if(!newValue && action !== 'DELETE'){
        throw new errors.ValidationException("Only DELETE action is allowed when no attribute value is specified");
    }

    return this[methodName].apply(this, [name, newValue]);
};

Item.prototype.put = function(name, value){
    this.attributes[name] = Attribute.fromHash(name, value);
};

Item.prototype.delete = function(name, value){
    if(!value){
        delete this.attrbutes[name];
        return;
    }
    // elsif old_attribute = attributes[name]
    // validate_type(value, old_attribute)
    // unless ["SS", "NS"].include? old_attribute.type
    //   raise ValidationException, "Action DELETE is not supported for type #{old_attribute.type}"
    // end
    // attribute = Attribute.from_hash(name, value)
    // old_attribute.value -= attribute.value
    // end
};

Item.prototype.add = function(name, value){
    var attribute = Attribute.fromHash(name, value),
        oldAttribute = this.attributes[name],
        newValue,
        preMerged;

    if(['N', 'SS', 'NS'].indexOf(attribute.type) === -1){
        throw new errors.ValidationException("Action ADD is not supported for type "+attribute.type);
    }

    if(!oldAttribute){
        this.attributes[name] = attribute;
        return;
    }

    // validate_type(value, old_attribute)
    if(attribute.type === 'N'){
        this.attributes[name].value = Number(oldAttribute.value) + Number(attribute.value);
    }
    else{
        preMerged = this.attributes[name].value.slice().concat(attribute.value);
        if(nub(preMerged).length !== preMerged.length){
            throw new errors.ValidationException('Input collection ' + name +
                    ' contains duplicates [' + preMerged + '].');
        }

        attribute.value.forEach(function(v){
            this.attributes[name].value.push(v);
        }.bind(this));
    }

};

module.exports = Item;