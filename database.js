/*!
 * Copyright (c) 2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// exceptions
const MDBE_ERROR = 'MongoError';
const MDBE_DUPLICATE = 11000;
const MDBE_DUPLICATE_ON_UPDATE = 11001;

/**
 * Returns true if the given error is a MongoDB duplicate key error.
 *
 * @param err the error to check.
 *
 * @return true if the error is a duplicate key error, false if not.
 */
exports.isDuplicateError = err => (exports.isDatabaseError(err) &&
  (err.code === MDBE_DUPLICATE || err.code === MDBE_DUPLICATE_ON_UPDATE));

/**
 * Returns true if the given error is a MongoDB error.
 *
 * @param err the error to check.
 *
 * @return true if the error is a duplicate key error, false if not.
 */
exports.isDatabaseError = err => (err && err.name === MDBE_ERROR);
