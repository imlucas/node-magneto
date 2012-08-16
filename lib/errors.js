"use strict";

var util = require('util'),
    restify = require('restify');

function ResourceInUseException(message){
    restify.RestError.call(this, 400, 'ResourceInUseException',
        message, ResourceInUseException);
    this.name = 'ResourceInUseException';
}
util.inherits(ResourceInUseException, restify.RestError);
module.exports.ResourceInUseException = ResourceInUseException;

function ResourceNotFoundException(message){
    restify.RestError.call(this, 400, 'ResourceNotFoundException',
        message, ResourceNotFoundException);
    this.name = 'ResourceNotFoundException';
}
util.inherits(ResourceNotFoundException, restify.RestError);
module.exports.ResourceNotFoundException = ResourceNotFoundException;

function ValidationException(message){
    restify.RestError.call(this, 400, 'ValidationException',
        message, ValidationException);
    this.name = 'ValidationException';
}
util.inherits(ValidationException, restify.RestError);
module.exports.ValidationException = ValidationException;


function ConditionCheckFailedException(message){
    restify.RestError.call(this, 400, 'ConditionCheckFailedException',
        message, ConditionCheckFailedException);
    this.name = 'ConditionCheckFailedException';
}
util.inherits(ConditionCheckFailedException, restify.RestError);
module.exports.ConditionCheckFailedException = ConditionCheckFailedException;