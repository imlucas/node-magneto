"use strict";

var assert = require('assert'),
    Attribute = require('../lib/attribute');

describe('Attribute', function(){
    it('should make from hash variant 1', function(){
        var attr = Attribute.fromHash('devices', { SS: [ 'ios' ] });
        assert.equal(attr.name, 'devices');
        assert.deepEqual(attr.value, ['ios']);
        assert.equal(attr.type, 'SS');
    });

    it('should make from hash variant 2', function(){
        var attr = Attribute.fromHash('username',  { username: { S: 'lucas' } });
        assert.equal(attr.name, 'username');
        assert.deepEqual(attr.value, 'lucas');
        assert.equal(attr.type, 'S');
    });
});