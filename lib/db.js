"use strict";

var errors = require('./errors'),
    Table = require('./table'),
    common = require('./common');

var DB = {
    'tables': {},
    'process': function(operation, data){
        // validate payload
        var methodName = common.toCamelCase(operation);
        return DB[methodName].apply(DB, [data]);
    },
    'createTable': function(data){
        var tableName = data.TableName,
            table;
        if(DB.tables[tableName]){
            throw new errors.ResourceInUseException("Duplicate Table Name: " + tableName);
        }
        table = new Table(data);
        DB.tables[tableName] = table;
        table.activate();
        return table.description;
    },
    'describeTable': function(data){
        return DB.findTable(data.TableName).describeTable();
    },
    'deleteTable': function(data){
        DB.findTable(data.TableName).drop();
        var description =  DB.findTable(data.TableName).description;
        delete DB.tables[data.TableName];
        return description;
    },
    'listTables': function(data){
        return {'TableNames': Object.keys(DB.tables)};
    },
    'updateTable': function(data){
        DB.findTable(data.TableName).update(
            data.ProvisionedThroughput.ReadCapacityUnits,
            data.ProvisionedThroughput.WriteCapacityUnits
        );
    },
    'batchGetItem': function(data){
        console.log('batchGetItem called');
        var response = {};

        Object.keys(data.RequestItems).forEach(function(tableName){
            var tableData = data.RequestItems[tableName],
                table = this.findTable(tableName),
                itemHash;

            if(!response.hasOwnProperty(tableName)){
                response[tableName] = {
                    'ConsumedCapacityUnits': 1,
                    'Items': []
                };
            }
            tableData.Keys.forEach(function(key){
                itemHash = table.getRawItem(key, tableData.AttributesToGet);
                if(itemHash){
                    response[tableName].Items.push(itemHash);
                }
            }.bind(this));
        }.bind(this));

        return {
            'Responses': response
            // 'UnprocessedKeys': {}
        };
    },
    'batchWriteItem': function(data){

        var response = {},
            items = {},
            requestCount = 0,
            requests,
            table,
            item,
            key;

        Object.keys(data.RequestItems).forEach(function(tableName){
            requests = data.RequestItems[tableName];
            table = this.findTable(tableName);
            items[table.name] = items[table.name] || {};
            requests.forEach(function(request){
                if(request.PutRequest){
                    item = table.batchPutRequest(request.PutRequest);
                    // this.checkItemConflict(items, table.name, item.key);
                    items[table.name][item.key] = item;
                }
                else{
                    key = table.batchDeleteRequest(request.DeleteRequest);
                    //this.checkItemConflict(items, table.name, key);
                    items[table.name][key] = 'DELETE';
                }
                requestCount++;
            }.bind(this));

            // this.checkMaxRequestCount(requestCount);
        }.bind(this));

        Object.keys(items).forEach(function(tableName){
            requests = items[tableName],
                table = this.findTable(tableName);
            Object.keys(requests).forEach(function(key){
                var val = requests[key];
                if(val === 'DELETE'){
                    table.batchDelete(key);
                    console.log('delete', key);
                }
                else{
                    console.log('put', val);
                    table.batchPut(val);
                }
            }.bind(this));

            response[tableName] = {
                'ConsumedCapacityUnits': 1
            };

        }.bind(this));
        return {
            'Responses': response,
            'UnprocessedItems': {}
        };
    },
    'findTable': function(tableName){
        if(!DB.tables[tableName]){
            throw new errors.ResourceNotFoundException("Table "+tableName+" not found");
        }
        return DB.tables[tableName];
    },
    'checkMaxRequest': function(count){
        if(count > 100){
            throw new Error('Too many items for BatchWrite item.');
        }
    }
};

['putItem', 'getItem', 'deleteItem',
    'updateItem', 'query', 'scan'].forEach(function (method) {
  DB[method] = function (data){
    var table = DB.findTable(data.TableName),
      c = table[method];
    return c.apply(table, [data]);
  };
});

module.exports = DB;