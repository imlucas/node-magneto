"use strict";

var errors = require('./errors'),
    Table = require('./table'),
    common = require('./common'),
    fs = require('fs');


var tableInitData = {}; // tableName => data passed to createTable
var tableItems = {}; // tableName => items dict.

var DB = {
    'saveTo': null,
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

        table.activate();

        tableInitData[tableName] = data;
        if(tableInitData[tableName]){
            table.items = tableInitData[tableName];
        }

        if(DB.saveTo){
            ['put', 'delete', 'update'].map(function(e){
                table.on(e, maybePersist);
            });
        }

        DB.tables[tableName] = table;

        return table.description;
    },
    'describeTable': function(data){
        return DB.findTable(data.TableName).describeTable();
    },
    'deleteTable': function(data){

        DB.findTable(data.TableName).drop();
        var description =  DB.findTable(data.TableName).description;
        delete DB.tables[data.TableName];
        delete tableInitData[data.TableName];
        delete tableItems[data.TableName];
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
                    table.logStat('BATCH_GET_ITEM', itemHash.key);
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
                    table.logStat('BATCH_PUT_ITEM', item.key);
                }
                else{
                    key = table.batchDeleteRequest(request.DeleteRequest);
                    table.logStat('BATCH_DELETE_ITEM', key);
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
                }
                else{
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

module.exports.saveToDisk = function(dest, done){
    // write tableInitData and tableItems to disk or something.
    var buf = JSON.stringify({
        'tables': tableInitData,
        'items': tableItems
    });
    fs.writeFile(dest, buf, done);
};

module.exports.loadFromDisk = function(src, done){
    // read from src and initialize tableInitData and tableItems
    fs.readFile(src, function(err, buf){
        if(err){
            return done(err);
        }
        var data = JSON.parse(buf);
        tableInitData = data.tables;
        tableItems = data.items;
        Object.keys(tableInitData).map(function(tableData){
            DB.createTable(tableData);
        });
        done();
    });
};

var scheduled = false;
function maybePersist(){
    if(scheduled){
        return;
    }

    scheduled = true;
    setTimeout(function(){
        module.exports.saveToDisk(DB.persist, function(){
            scheduled = false;
        });
    }, 100);
}