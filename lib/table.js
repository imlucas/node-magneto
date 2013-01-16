"use strict";

var common = require('./common'),
    merge = common.merge,
    Key = require('./key'),
    Attribute = require('./attribute'),
    KeySchema = require('./keySchema'),
    Item = require('./item'),
    errors = require('./errors'),
    util = require('util');

function Table(data){
    this.creationDateTime = Date.now();
    this.status = 'CREATING';
    this.items = {};
    this.sizeBytes = 0;
    this.name = data.TableName;
    this.keySchema = new KeySchema(data.KeySchema);
    this.setThroughput(data.ProvisionedThroughput);
    this.stats = {};
}

Table.prototype.logStat = function(op, key){
    if(!this.stats.hasOwnProperty(op)){
        this.stats[op] = [];
    }
    this.stats[op].push({'k': key, 't': Date.now()});
};

Table.prototype.describeTable = function(){
    return {
        'Table': this.description.TableDescription,
        'ItemCount': Object.keys(this.items).length,
        'TableSizeBytes': this.sizeBytes
    };
};

Table.prototype.putItemData = function(item){
    return {
        'TableName': this.name,
        'Item': item.asHash()
    };
};

Table.prototype.activate = function(){
    this.status = "ACTIVE";
};

Table.prototype.drop = function(){
    this.status = "DELETING";
};

Table.prototype.update = function(readCapacityUnits, writeCapacityUnits){
    if(this.readCapacityUnits > readCapacityUnits){
        this.lastDecreasedTime = Date.now();
    }
    else if(this.readCapacityUnits < readCapacityUnits){
        this.lastIncreasedTime = Date.now();
    }

    if(this.writeCapacityUnits > writeCapacityUnits){
        this.lastDecreasedTime = Date.now();
    }
    else if(this.writeCapacityUnits < writeCapacityUnits){
        this.lastIncreasedTime = Date.now();
    }

    this.readCapacityUnits = readCapacityUnits;
    this.writeCapacityUnits = writeCapacityUnits;

    var response = this.description;
    merge(response, this.sizeDescription);

    if(this.lastIncreasedTime){
        response.TableDescription.ProvisionedThroughput.LastIncreaseDateTime = this.lastIncreasedTime;
    }

    if(this.lastDecreasedTime){
        response.TableDescription.ProvisionedThroughput.LastDecreaseDateTime = this.lastDecreasedTime;
    }
    response.TableDescription.TableStatus = "UPDATING";
    return response;
};

Table.prototype.putItem = function(data){
    var item = Item.fromData(data.Item, this.keySchema),
        oldItem = this.items[item.key];
    this.checkConditions(oldItem, data.Expected);
    this.items[item.key] = item;
    var response = {
        'ConsumedCapacityUnits': 1
    };

    this.logStat('PUT_ITEM', item.key);

    merge(response, this.returnValues(data, this.items[item.key]));
    return response;
};

Table.prototype.batchPutRequest = function(data){
    return Item.fromData(data.Item, this.keySchema);
};

Table.prototype.batchPut = function(item){
    return this.items[item.key] = item;
};

Table.prototype.getItem = function(data){
    var response = {
        'ConsumedCapacityUnits': 1
    }, itemHash = this.getRawItem(data.Key, data.AttributesToGet);
    this.logStat('GET_ITEM', data.Key);
    merge(response, {'Item': (itemHash instanceof Item) ? itemHash.asHash(): itemHash});
    return response;

};

Table.prototype.getRawItem = function(keyData, attributesToGet){
    var key = Key.fromData(keyData, this.keySchema),
        item = this.items[key];

    if(item){
        return this.filterAttributes(item, attributesToGet);
    }
    return item;
};

Table.prototype.filterAttributes = function(item, attributesToGet){
    var hash = item.asHash(),
        filtered = {};

    if(!attributesToGet){
        return hash;
    }

    Object.keys(hash).forEach(function(key){
        if(attributesToGet.indexOf(key) !== -1){
            filtered[key] = hash[key];
        }
    });
    return filtered;
};

Table.prototype.deleteItem = function(data){
    var key = Key.fromData(data.Key, this.keySchema),
        item = this.items[key];

    this.checkConditions(item, data.Expected);
    this.logStat('DELETE_ITEM', item.key);
    delete this.items[key];
    var response = {
        'ConsumedCapacityUnits': 1
    };

    merge(response, this.returnValues(data, item));
    return response;
};

Table.prototype.batchDeleteRequest = function(data){
    return Key.fromData(data.Key, this.keySchema);
};

Table.prototype.batchDelete = function(key){
    delete this.items[key];
};

Table.prototype.updateItem = function(data){
    var key = Key.fromData(data.Key, this.keySchema),
        item = this.items[key],
        itemCreated = false,
        oldItem,
        oldHash;

    this.checkConditions(item, data.Expected);

    if(!item){
        item = this.items[key] = Item.fromKey(key);
        itemCreated = true;
    }

    oldItem = item;
    oldHash = item.asHash();

    Object.keys(data.AttributeUpdates).forEach(function(name){
        item.update(name, data.AttributeUpdates[name]);
    });

    var response = {
        'ConsumedCapacityUnits': 1
    };
    this.logStat('UPDATE_ITEM', item.key);
    merge(response, this.returnValues(data, oldHash, item));
    return response;
};

Table.prototype.validateCountAndAttributesToGet = function(data){
    if(data.hasOwnProperty('Count') && data.hasOwnProperty('AttributesToGet')){
        throw new errors.ValidationException("Cannot specify the AttributesToGet when choosing to get only the Count");
    }
};

Table.prototype.query = function(data){
    if(!this.keySchema.rangeKey){
        throw new errors.ValidationException("Query can be performed only on a table with a HASH,RANGE key schema");
    }
    this.validateCountAndAttributesToGet(data);
    this.validateLimit(data);

    var rangeKeyName = this.keySchema.rangeKey.name,
        hashAttribute = Attribute.fromHash(this.keySchema.hashKey.name, data.HashKeyValue),
        matchedItems = this.getItemsByHashKey(hashAttribute),
        forward = data.hasOwnProperty('ScanIndexForward') ?
            data.ScanIndexForward : true,
        conditions = {};

    matchedItems = this.dropTillStart(matchedItems, data.ExclusiveStartKey,
        forward);

    if(data.RangeKeyCondition){
        conditions[rangeKeyName] = data.RangeKeyCondition;
    }

    var filtered = this.filter(matchedItems, conditions, data.Limit, true),
        result = filtered[0],
        lastEvaluatedItem = filtered[1],
        response = {
            'Count': result.length,
            'ConsumedCapacityUnits': 1
        };

    if(!data.hasOwnProperty('Count')){
        response.Items = result.map(function(item){
            return this.filterAttributes(item, data.AttributesToGet);
        }.bind(this));
    }

    if(lastEvaluatedItem){
        response.LastEvaluatedKey = lastEvaluatedItem.key.asKeyHash();
    }
    return response;
};

Table.prototype.allValues = function(){
    return Object.keys(this.items).map(function(key){
        return this.items[key];
    }, this);
};

Table.prototype.scan = function(data){
    this.validateCountAndAttributesToGet(data);
    this.validateLimit(data);
    var conditions = data.ScanFilter || {},
        allItems = this.dropTillStart(this.allValues(),
            data.ExclusiveStartKey, true),
        parts = this.filter(allItems, conditions, data.Limit, false),
        result = parts[0],
        lastEvaluatedItem = parts[1],
        scannedCount = parts[2],
        response = {
            'Count': result.length,
            'ScannedCount': scannedCount,
            'ConsumedCapacityUnits': 1
        };

    if(!data.Count){
        response.Items = result.map(function(r){
            return this.filterAttributes(r, data.AttributesToGet);
        }, this);
    }

    if(lastEvaluatedItem){
        response.LastEvaluatedKey = lastEvaluatedItem.key.asKeyHash();
    }
    return response;
};


Table.prototype.validateLimit = function(data){
    if(data.Limit && data.Limit <= 0){
        throw new errors.ValidationError("Limit failed to satisfy constraint: Member must have value greater than or equal to 1");
    }
};

Table.prototype.dropTillStart = function(allItems, startKeyHash, forward){
    var sortFunction = function(a, b) {
        return a.key.toString() < b.key.toString() ? 1 : -1;
    },
    aValue,
    bValue;

    if(this.keySchema.rangeKey){
        if(this.keySchema.rangeKey.type === 'N'){
            // sort numerically by range, rather than alphabetically by the whole key
            sortFunction = function(a, b){
                aValue = !a.key.range ? 0 : a.key.range.value;
                bValue = !b.key.range ? 0 : b.key.range.value;
                return aValue > bValue ? 1 : -1;
            };
        }
    }

    allItems.sort(sortFunction);

    if(!forward){
        allItems.reverse();
    }
    if(startKeyHash){
        var startKey = Key.fromData(startKeyHash, this.keySchema);
        Object.keys(allItems).forEach(function(key){
            var item = allItems[key],
                drop = false;

            if(forward){
                drop = (item.key <= startKey);
            }
            else{
                drop = (item.key >= startKey);
            }

            if(drop === true){
                delete allItems[key];
            }
        });
    }
    return allItems;
};

Table.prototype.filter = function(items, conditions, limit, failOnTypeMismatch){
    limit = limit || -1;
    var result = [],
        lastEvaluatedItem = null,
        scannedCount = 0,
        select,
        keys = Object.keys(items),
        key,
        item;

    for(var i = 0; i < keys.length; i++){
        key = keys[i];
        item = items[key];
        select = true;
        Object.keys(conditions).every(function(key){
            var condition = conditions[key],
                value = condition.AttributeValueList,
                comparisonOp = condition.ComparisonOperator,
                method = common.toCamelCase(comparisonOp.toLowerCase()) + 'Filter';

            if(!this[method].apply(this, [value, item.attributes[key],
                    failOnTypeMismatch])){
                select = false;
            }
        }.bind(this));

        if(select){
            result.push(item);
            limit = limit - 1;

            if(limit === 0){
                lastEvaluatedItem = item;
                scannedCount++;
                return [result, lastEvaluatedItem, scannedCount];
            }
        }
        scannedCount++;
    }

    return [result, lastEvaluatedItem, scannedCount];
};

Table.prototype.getItemsByHashKey = function(hashKey){
    var matchingItems = [];
    Object.keys(this.items).forEach(function(key){
        if(this.items[key].key.primary.equals(hashKey)){
            matchingItems.push(this.items[key]);
        }
    }.bind(this));
    return matchingItems;
};

Table.prototype.shouldCreateItem = function(data){
     var result = false;
     Object.keys(data.AttributeUpdates).forEach(function(key){
        var action = data.AttributeUpdates[key].Action;
        if(['PUT', 'ADD', null].indexOf(action) > -1){
            result = true;
            return true;
        }
    });
    return result;
};

Table.prototype.updatedAttributes = function(data){
    return Object.keys(data.AttributeUpdates);
};

Table.prototype.returnValues = function(data, oldItem, newItem){
    var updated, updatedKeys, oldHash, newHash;

    oldItem = oldItem || {};
    oldHash = (oldItem instanceof Item) ? oldItem.asHash() : oldItem;

    newItem = newItem || {};
    newHash = (newItem instanceof Item) ? newItem.asHash() : newItem;

    var returnValue = data.ReturnValues,
        result;

    switch(returnValue){
        case 'ALL_OLD':
            result = oldHash;
        break;
        case 'ALL_NEW':
            result = newHash;
        break;
        case 'UPDATED_OLD':
            updated = this.updatedAttributes(data),
                updatedKeys = Object.keys(updated);
            result = Object.keys(oldHash).map(function(key){
                if(updatedKeys.indexOf(key) > -1){
                    return key;
                }
            });
        break;
        case 'UPDATED_NEW':
            updated = this.updatedAttributes(data),
                updatedKeys = Object.keys(updated);

            result = Object.keys(newHash).map(function(key){
                if(updatedKeys.indexOf(key) > -1){
                    return key;
                }
            });
        break;
        case 'NONE':
            result = {};
        break;
        default:
            result = (newHash) ? newHash : oldHash;
        break;

    }
    return {'Attributes': result};
};

Table.prototype.checkConditions = function(oldItem, conditions){
    if(!conditions){
        return;
    }

    Object.keys(conditions).forEach(function(name){
        var predicate = conditions[name],
            exist = predicate.Exists,
            value = predicate.Value;
        if(!value){
            if(exist === undefined){
                throw new errors.ValidationException("'Exists' is set to null. 'Exists' must be set to false when no Attribute value is specified");
            }
            else if(exist === true){
                throw new errors.ValidationException("'Exists' is set to true. 'Exists' must be set to false when no Attribute value is specified");
            }
            else if(exist === false){
                if(oldItem && oldItem[name]){
                    throw new errors.ConditionalCheckFailedException();
                }
            }
        }
        else{
            var expectedAttr = Attribute.fromHash(name, value);
            if(exist === true && oldItem[name] !== expectedAttr){
                throw new errors.ConditionalCheckFailedException();
            }
            else if(exist === false){
                throw new errors.ValidationException("Cannot expect an attribute to have a specified value while expecting it to not exist");
            }
        }

    });
};

Table.prototype.extractValues = function(data){
    this.name = data.TableName;
    this.keySchema = new KeySchema(data['KeySchema']);
    this.setThroughput(data.ProvisionedThroughput);
};

Table.prototype.setThroughput = function(throughput){
    this.readCapacityUnits = throughput.ReadCapacityUnits;
    this.writeCapacityUnits = throughput.WriteCapacityUnits;
};

Object.defineProperty(Table.prototype, "description", {
    'get': function(){
        return {
            'TableDescription': {
                'CreationDateTime': this.creationDateTime,
                'KeySchema': this.keySchema.description,
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': this.readCapacityUnits,
                    'WriteCapacityUnits': this.writeCapacityUnits
                },
                'TableName': this.name,
                'TableStatus': this.status
            }
        };
    }
});

Object.defineProperty(Table.prototype, "sizeDescription", {
    'get': function(){
        return {
            'ItemCount': Object.keys(this.items).length,
            'TableSizeBytes': this.sizeBytes
        };
    }
});

// Object.defineProperty(Table.prototype, "createTableData", {
//     'get': function(){
//         return {
//             'TableName': this.name,
//             'KeySchema': this.keySchema.description,
//             'ProvisionedThroughput': {
//                 'ReadCapacityUnits': this.readCapacityUnits,
//                 'WriteCapacityUnits': this.writeCapacityUnits
//             }
//       };
//     }
// });

// Filters for query and scan
Table.prototype.inFilter = function(valueList, targetAttribute, failOnTypeMismatch){
    if(!targetAttribute){
        return false;
    }

    // validate_size(value_list, (1..INF))
    var matches = valueList.map(function(value){
        var va = Attribute.fromHash(targetAttribute.name, value);
        // validate_supported_types(value_attribute, ['N', 'S'])
        return va;
    }).filter(function(va){
        return va.equals(targetAttribute);
    });

    return (matches.length > 0);
};

Table.prototype.notNullFilter = function(valueList, targetAttribute, failOnTypeMismatch){
    return !!targetAttribute;
};

Table.prototype.nullFilter = function(valueList, targetAttribute, failOnTypeMismatch){
    return targetAttribute === undefined;
};

Table.prototype.comparisonFilter = function(valueList, size, targetAttribute, failOnTypeMismatch, supportedTypes, comparator){
    if(targetAttribute === undefined){
        return false;
    }
    // validate_size(value_list, size)

    // if fail_on_type_mismatch
    //     value_list.each do |value|
    //         validate_type(value, target_attribute)
    //     end
    // end
    var valueAttributeList = valueList.map(function(value){
        var valueAttribute = Attribute.fromHash(targetAttribute.name, value);
        // validate_supported_types(value_attribute, supported_types)
        return valueAttribute;
    });
    return comparator.apply(this, [targetAttribute, valueAttributeList]);
};
var queryOperators = [
    'EQ',
    'LE',
    'LT',
    'GE',
    'GT',
    'BEGINS_WITH',
    'BETWEEN'
];

var scanOperators = [
    'EQ',
    'LE',
    'LT',
    'GE',
    'GT',
    'BEGINS_WITH',
    'BETWEEN',
    'NULL',
    'NOT_NULL',
    'CONTAINS',
    'NOT_CONTAINS',
    'IN'
];

var comparisonFilters = {
    'gt': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value > attr.value;
            }).length > 0;
        },
        'types': ['N', 'S', 'B']
    },
    'ge': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value >= attr.value;
            }).length > 0;
        },
        'types': ['N', 'S', 'B']
    },
    'lt': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value < attr.value;
            }).length > 0;
        },
        'types': ['N', 'S', 'B']
    },
    'le': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value <= attr.value;
            }).length > 0;
        },
        'types': ['N', 'S', 'B']
    },
    'ne': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value != attr.value;
            }).length > 0;
        },
        'types': ['N', 'S', 'B']
    },
    'eq': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value == attr.value;
            }).length > 0;
        },
        'types': ['N', 'S', 'B']
    },
    'between': {
        'size': 2,
        'cmp': function(targetAttribute, inputs){
            return targetAttribute.value >= inputs[0].value &&
                targetAttribute.value <= inputs[1].value;
        },
        'types': ['N', 'S', 'B']
    },
    'beginsWith': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return new RegExp('^' + attr.value).test(targetAttribute.value);
            }).length > 0;
        },
        'types': ['S', 'B']
    },
    'contains': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value.indexOf(attr.value) > -1;
            }).length > 0;
        },
        'types': ['SS', 'NS', 'BS']
    },
    'notContains': {
        'size': 1,
        'cmp': function(targetAttribute, inputs){
            return inputs.filter(function(attr){
                return targetAttribute.value.indexOf(attr.value) === -1;
            }).length > 0;
        },
        'types': ['SS', 'NS', 'BS']
    }
};

Object.keys(comparisonFilters).forEach(function(name){
    var filter = comparisonFilters[name];
    Table.prototype[name+'Filter'] = function(valueList, targetAttribute, failOnTypeMismatch){
        return this.comparisonFilter(valueList, filter.size, targetAttribute,
            failOnTypeMismatch, filter.types, filter.cmp);
    };
});

module.exports = Table;