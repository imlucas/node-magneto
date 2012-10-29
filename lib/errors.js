"use strict";

var util = require('util');

function RestError(code, name, message, type){
    this.code = code;
    this.name = name;
    this.message = message;

    RestError.super_.call(this, message);
}
util.inherits(RestError, Error);


function ResourceInUseException(message){
    ResourceInUseException.super_.call(this, 400, 'ResourceInUseException',
        message, ResourceInUseException);
    this.name = 'ResourceInUseException';
}
util.inherits(ResourceInUseException, RestError);
module.exports.ResourceInUseException = ResourceInUseException;

function ResourceNotFoundException(message){
    ResourceNotFoundException.super_.call(this, 400, 'ResourceNotFoundException',
        message, ResourceNotFoundException);
    this.name = 'ResourceNotFoundException';
}
util.inherits(ResourceNotFoundException, RestError);
module.exports.ResourceNotFoundException = ResourceNotFoundException;

function ValidationException(message){
    ValidationException.super_.call(this, 400, 'ValidationException',
        message, ValidationException);
    this.name = 'ValidationException';
}
util.inherits(ValidationException, RestError);
module.exports.ValidationException = ValidationException;


function ConditionCheckFailedException(message){
    ConditionCheckFailedException.super_.call(this, 400, 'ConditionCheckFailedException',
        message, ConditionCheckFailedException);
    this.name = 'ConditionCheckFailedException';
}
util.inherits(ConditionCheckFailedException, RestError);
module.exports.ConditionCheckFailedException = ConditionCheckFailedException;