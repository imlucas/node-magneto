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
        delete this.attributes[name];
        return;
    }

    if(["SS", "NS", "BS"].indexOf(this.attributes[name].type) === -1){
        throw new errors.ValidationException("Action DELETE is not supported for type " + this.attributes[name].type);
    }

    this.attributes[name].value.splice(this.attributes[name].value.indexOf(value), 1);
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
// @see https://bitbucket.org/Ludia/dynamodb-mock/src/7e532aee8fe528176ea83f95baa940ceea93a43e/ddbmock/database/item.py?at=default
// Should also include indexing overhead?
// https://bitbucket.org/Ludia/dynamodb-mock/src/7e532aee8fe528176ea83f95baa940ceea93a43e/ddbmock/config.py?at=default
Item.prototype.getSize = function(){};


// def _internal_item_size(self, base_type, value):
//     """
//     Internal DynamoDB field size computation. ``base_type`` is assumed to
//     be valid as it went through Onctous before and this helper is only
//     supposed to be called internally.

//     :param base_type: valid base type. Must be in ``['N', 'S', 'B']``.
//     :param value: compute the size of this value.
//     """
//     if base_type == 'N': return 8 # assumes "double" internal type on ddb side
//     if base_type == 'S': return len(value.encode('utf-8'))
//     if base_type == 'B': return len(value.encode('utf-8'))*3/4 # base64 overhead

// def get_field_size(self, fieldname):
//     """
//     Compute field size in bytes.

//     :param fieldname: Valid field name

//     :return: Size of the field in bytes or 0 if the field was not found. Remember that empty fields are represented as missing values in DynamoDB.
//     """
//     if not fieldname in self:
//         return 0

//     typename, value = _decode_field(self[fieldname])
//     base_type = typename[0]

//     if len(typename) == 1:
//         value_size = self._internal_item_size(base_type, value)
//     else:
//         value_size = 0
//         for v in value:
//             value_size += self._internal_item_size(base_type, v)

//     return value_size

// def get_size(self):
//     """
//     Compute Item size as DynamoDB would. This is especially useful for
//     enforcing the 64kb per item limit as well as the capacityUnit cost.

//     .. note:: the result is cached for efficiency. If you ever happend to
//         directly edit values for any reason, do not forget to invalidate the
//         cache: ``self.size=None``

//     :return: :py:class:`ItemSize` DynamoDB item size in bytes
//     """

//     # Check cache and compute
//     if self.size is None:
//         size = 0

//         for key in self.keys():
//             size += self._internal_item_size('S', key)
//             size += self.get_field_size(key)

//         self.size = size

//         return ItemSize(self.size)

module.exports = Item;