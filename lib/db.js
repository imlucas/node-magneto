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
        delete DB.tables[data.TableName];
        DB.findTable(data.TableName).delete();
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
      //   var response = {};
      //   response = {}

      // data['RequestItems'].each do |table_name, table_data|
      //   table = find_table(table_name)

      //   unless response[table_name]
      //     response[table_name] = { 'ConsumedCapacityUnits' => 1, 'Items' => [] }
      //   end

      //   table_data['Keys'].each do |key|
      //     if item_hash = table.get_raw_item(key, table_data['AttributesToGet'])
      //       response[table_name]['Items'] << item_hash
      //     end
      //   end
      // end

      // { 'Responses' => response, 'UnprocessedKeys' => {}}
    },
    'batchWriteItem': function(data){
      //   response = {}
      // items = {}
      // request_count = 0

      // # validation
      // data['RequestItems'].each do |table_name, requests|
      //   table = find_table(table_name)

      //   items[table.name] ||= {}

      //   requests.each do |request|
      //     if request['PutRequest']
      //       item = table.batch_put_request(request['PutRequest'])
      //       check_item_conflict(items, table.name, item.key)
      //       items[table.name][item.key] = item
      //     else
      //       key = table.batch_delete_request(request['DeleteRequest'])
      //       check_item_conflict(items, table.name, key)
      //       items[table.name][key] = :delete
      //     end

      //     request_count += 1
      //   end
      // end

      // check_max_request(request_count)

      // # real modification
      // items.each do |table_name, requests|
      //   table = find_table(table_name)
      //   requests.keach do |key, value|
      //     if value == :delete
      //       table.batch_delete(key)
      //     else
      //       table.batch_put(value)
      //     end
      //   end
      //   response[table_name] = { 'ConsumedCapacityUnits' => 1 }
      // end

      // { 'Responses' => response, 'UnprocessedItems' => {} }
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