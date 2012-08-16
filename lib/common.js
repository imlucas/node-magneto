"use strict";

module.exports.merge = function merge(to, from){
    var keys = Object.keys(from),
        i = keys.length,
        key;

    while (i--) {
      key = keys[i];
      if ('undefined' === typeof to[key]) {
        to[key] = from[key];
      } else {
        // merge(to[key], from[key]);
      }
    }
};

module.exports.isSetType = function isSetType(type){
    return (['NS', 'SS'].indexOf(type) > -1);
};

module.exports.isNumberType = function isNumberType(type){
    return (['NS', 'N'].indexOf(type) > -1);
};

module.exports.toCamelCase = function (s) {
    if(s.charAt(0).match(/[A-Z]/)){
        return s.charAt(0).toLowerCase() + s.substr(1, s.length - 1);
    }
    return s;
};
