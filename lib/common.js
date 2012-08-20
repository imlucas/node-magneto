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

var STRING_TITLEIZE_REGEXP = (/([\s|\-|\_|\n])([^\s|\-|\_|\n]?)/g);

module.exports.toCamelCase = function (s){
    var ret = s.replace(STRING_TITLEIZE_REGEXP,
     function(str, separater, character) {
      return (character) ? character.toUpperCase() : '' ;
     }) ;
    var first = ret.charAt(0),
        lower = first.toLowerCase();

    return (first !== lower) ? (lower + ret.slice(1)) : ret;
};
