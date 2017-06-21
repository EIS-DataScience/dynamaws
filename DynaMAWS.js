"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

const _ = require("lodash");

function handleQueryResponse(queryResponse, params, delay) {
  let items = []

  function handleQueryResponseHelper(queryResponse, params) {
    items = items.concat(queryResponse.Items);

    if (queryResponse.LastEvaluatedKey) {
      const clonedParams = _.cloneDeep(params);

      clonedParams["ExclusiveStartKey"] = queryResponse.LastEvaluatedKey;

      let delayedReq = new Promise((resolve) => {
        setTimeout(() => {
          resolve(docClient.query(clonedParams).promise());
        }, delay)
      });

      return delayedReq
        .then((dynamoData) => {
          return handleQueryResponseHelper(dynamoData, params);
        });
    }

    return Promise.resolve();
  }

  return handleQueryResponseHelper(queryResponse, params)
    .then(() => {
      return items;
    });
}

function batchWriteSync(tableName, primaryKeyName, items, buffer, writeDelay) {

  const BATCH_LIMIT = 25;

  // helper that sends the actual Dynamo requests
  function doBatchWrite(batch) {

    let requestBatch = batch.map(function (item) {
      return {
        PutRequest: {
          Item: item
        }
      };
    });

    let params = {
      RequestItems: {}
    };

    params.RequestItems[tableName] = requestBatch;

    return new Promise((resolve, reject) => {
      setTimeout(() => {
        const toResolve = docClient.batchWrite(params).promise()
          .then(data => {
            return data;
          });

        resolve(toResolve)
      }, writeDelay);
    })
  }

  function runBatchWrites(batch) {

    // attempt a batch write
    return doBatchWrite(batch)
      .then(data => {

        // check if any failed
        let failed = data.UnprocessedItems[tableName] ?
          data.UnprocessedItems[tableName].map(item => {
            return item.PutRequest.Item;
          }) : [];

        // move successful items from failed to success 
        let success = _.differenceBy(batch, failed, primaryKeyName);
        _.pullAllBy(buffer.failed, success, primaryKeyName);
        buffer.successful.push(...success);

        // retry failed items if any
        if (failed.length > 0) {
          return runBatchWrites(failed);
        }

        // return if no more batches
        if (buffer.failed.length <= 0) {
          return buffer;
        }

        // else run next batch
        return runBatchWrites(buffer.failed.slice(0, BATCH_LIMIT));

      })
      .catch(err => {
        buffer.err = err;
        return buffer;
      });
  }

  let firstBatch = buffer.failed.slice(0, BATCH_LIMIT);
  return runBatchWrites(firstBatch);
}
function batchQuerySync(queries, queryFunction, filterFunction, buffer, limit = 0) {

  const runBatchQueries = query => {

    return queryFunction(query)

      .then(items => {

        buffer.items.push(...filterFunction(items));

        queries.shift();

        if (buffer.items.length >= limit) {
          buffer.nextItem = buffer.items[limit - 1];
          buffer.items = buffer.items.slice(0, limit - 1);
          return buffer;
        }

        if (queries.length === 0) {
          return buffer;
        }

        return runBatchQueries(queries[0]);

      });
  }

  return runBatchQueries(queries[0])
}

/**
 * Runs batchGet requests syncronously 
 * @param {string} tableName
 * @param {string} keyString 
 * @param {object} filterFunction 
 * @param {object} buffer 
 * @param {number} limit 
 */
function batchGetSync(tableName, keyString, filterFunction, buffer, limit = 0) {

  const BATCH_LIMIT = 100; // Set by AWS

  // Sends the actual Dynamo requests
  function doBatchGet(batch) {

    const keys = batch.map((keyValue) => {
      let keyObj = {};

      keyObj[keyString] = keyValue;

      return keyObj;
    });

    const params = {
      RequestItems: {
        [tableName]: {
          Keys: keys
        }
      }
    };

    return docClient.batchGet(params).promise();
  }

  // Runs batch requests recursively  
  function runBatchGets(batch) {

    return doBatchGet(batch)
      .then(data => {

        let success = data.Responses[tableName];
        let failed = data.UnprocessedKeys[tableName];

        buffer.items.push(...filterFunction(success));
        _.pullAll(buffer.nextKeys, success.map(item => item.id))

        if (failed && failed.length > 0) {
          return runBatchGets(failed.map(item => item[keyString]));
        }

        if (buffer.nextKeys.length <= 0) {
          return buffer;
        }

        return runBatchGets(buffer.nextKeys.slice(0, BATCH_LIMIT));
      });

  }

  return runBatchGets(buffer.nextKeys.slice(0, BATCH_LIMIT));

}

function batchDeleteSync(tableName, keyString, buffer, writeDelay) {

  const BATCH_LIMIT = 100; // Set by AWS

  // Sends the actual Dynamo requests
  function doBatchDelete(batch) {

    const requestBatch = buffer.nextKeys.map((keyVal) => {
      return {
        DeleteRequest: {
          Key: { [keyString]: keyVal }
        }
      };
    });

    let params = {
      RequestItems: {}
    };

    params.RequestItems[tableName] = requestBatch;

    return new Promise((resolve, reject) => {
      setTimeout(() => {
        const toResolve = docClient.batchWrite(params).promise()
          .then(data => {
            return data;
          });

        resolve(toResolve)
      }, writeDelay);
    })
  }

  function runBatchDeletes(batch) {

    // attempt a batch delete
    return doBatchDelete(batch)
      .then(data => {

        // check if any failed
        let failed = data.UnprocessedItems[tableName] ?
          data.UnprocessedItems[tableName].map(item => {
            return item.DeleteRequest.Key;
          }) : [];

        // move successful items from failed to success 
        let success = _.difference(batch, failed);
        _.pullAllBy(buffer.nextKeys, success);

        // retry failed items if any
        if (failed.length > 0) {
          return runBatchDeletes(failed);
        }

        // return if no more batches
        if (buffer.nextKeys.length <= 0) {
          return buffer;
        }

        // else run next batch
        return runBatchDeletes(buffer.nextKeys.slice(0, BATCH_LIMIT));

      })
      .catch(err => {
        buffer.err = err;
        return buffer;
      });
  }

  return runBatchDeletes(buffer.nextKeys.slice(0, BATCH_LIMIT));
}

module.exports = {
  get: function (tableName) {
    const dynamoUsersPromise = docClient.scan({
      TableName: tableName,
    })
      .promise();


    return dynamoUsersPromise.then(function (dynamoData) {
      return dynamoData.Items;
    });
  },

  getOne: function (tableName, key) {
    const params = {
      TableName: tableName,
      Key: key
    };

    return docClient.get(params)
      .promise()
      .then((dynamoData) => {
        return dynamoData.Item;
      });
  },

  create: function (tableName, newItem) {
    const params = {
      TableName: tableName,
      Item: newItem,
    };

    return docClient.put(params)
      .promise()
      .then(() => {
        return newItem;
      });
  },

  update: function (tableName, key, newItem) {
    const expressions = _.reduce(newItem, (acc, itemVal, itemKey) => {
      let updateExpressionArr = acc.updateExpressionArr;
      let expressionAttributeValues = acc.expressionAttributeValues;
      let expressionAttributeNames = acc.expressionAttributeNames;

      let itemAttributeName = `#${itemKey}`;
      let itemAttributeValue = `:${itemKey}`;

      expressionAttributeNames[itemAttributeName] = itemKey;
      expressionAttributeValues[itemAttributeValue] = itemVal;

      const newAttr = `${itemAttributeName} = ${itemAttributeValue}`;
      updateExpressionArr.push(newAttr);

      return {
        updateExpressionArr: updateExpressionArr,
        expressionAttributeNames: expressionAttributeNames,
        expressionAttributeValues: expressionAttributeValues
      };
    }, {
        updateExpressionArr: [],
        expressionAttributeNames: {},
        expressionAttributeValues: {}
      });

    let updateExpression = "set " + expressions.updateExpressionArr.join(", ");

    var params = {
      TableName: tableName,
      Key: key,
      UpdateExpression: updateExpression,
      ExpressionAttributeValues: expressions.expressionAttributeValues,
      ExpressionAttributeNames: expressions.expressionAttributeNames,
      ReturnValues: "ALL_NEW"
    };

    return docClient.update(params)
      .promise()
      .then((dynamoData) => {
        const newItem = dynamoData.Attributes;

        return newItem;
      });
  },

  /**
   * Runs a batch of writes synchonously 
   * @param {string} tableName 
   * @param {object[]} items 
   * @param {number} timeout - optional, causes early return 
   * @param {number} writeDelay
   * @return {object} successful/failed items and err if any
   */
  batchCreate: function (tableName, primaryKeyName, items, timeout = 0, writeDelay = 0) {

    // response is passed by ref to batchWrite
    let responseBuffer = {
      successful: [],
      failed: _.cloneDeep(items),
      err: null
    };

    // if we hit timeout, whatever is in the buffer is returned immediately
    if (timeout) {
      var timeoutPromise = new Promise(resolve => {
        setTimeout(() => {
          responseBuffer.err = {
            code: "BatchCreateTimeout",
            message: `Dynamaws batch create operation hit given timeout of ${timeout}ms`
          };
          resolve(responseBuffer);
        }, timeout);
      });
    }

    let finishedWritePromise = batchWriteSync(tableName, primaryKeyName, items, responseBuffer, writeDelay)

    return timeout ? Promise.race([timeoutPromise, finishedWritePromise]) : finishedWritePromise;
  },

  delete: function (tableName, key) {
    var params = {
      TableName: tableName,
      Key: key,
      ReturnValues: "ALL_OLD"
    };

    return docClient.delete(params)
      .promise()
      .then((dynamoData) => {
        return dynamoData.Attributes;
      });
  },

  batchDelete: function (tableName, keyString, keyValues, timeout = 0, writeDelay = 0) {

    // response is passed by ref to batchWrite
    let responseBuffer = {
      nextKeys: _.clone(keyValues)
    }

    // if we hit timeout, whatever is in the buffer is returned immediately
    if (timeout) {
      var timeoutPromise = new Promise(resolve => {
        setTimeout(() => {
          responseBuffer.err = {
            code: "BatchDeleteTimeout",
            message: `Dynamaws batch delete operation hit given timeout of ${timeout}ms`
          };
          resolve(responseBuffer);
        }, timeout);
      });
    }

    let finishedDeletePromise = batchDeleteSync(tableName, keyString, responseBuffer, writeDelay);

    return timeout ? Promise.race([timeoutPromise, finishedDeletePromise]) : finishedDeletePromise;
  },

  queryIndex: function (tableName, indexName, queryKey, queryVal, limit = 0, delay = 0) {
    const conditionKey = `#${queryKey}`;
    const conditionVal = `:${queryKey}`;

    let expressionAttributeNames = {};
    expressionAttributeNames[conditionKey] = queryKey;

    let expressionAttributeValues = {};
    expressionAttributeValues[conditionVal] = queryVal;

    const params = {
      TableName: tableName,
      IndexName: indexName,
      KeyConditionExpression: `${conditionKey} = ${conditionVal}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues,
    };

    if (limit > 0) {
      params.Limit = limit;
    }

    return docClient.query(params)
      .promise()
      .then((dynamoData) => {
        return handleQueryResponse(dynamoData, params, delay);
      });
  },

  /**
   * Runs batch of get requests synchronously, returns early if timeout
   * @param {string} tableName -
   * @param {string} keyString - name of primary key of table
   * @param {string[]} keyValues - primary keys
   * @param {object} filterFunction - optional filtering
   * @param {number} limit - max items to get
   * @param {number} timeout - optional timeout
   */
  batchGet: function (tableName, keyString, keyValues, filterFunction, limit = 0, timeout = 0) {

    if (!filterFunction) {
      filterFunction = items => items;
    }

    let responseBuffer = {
      items: [],
      nextKeys: _.clone(keyValues)
    }

    // If we hit timeout, whatever is in the buffer is returned immediately
    if (timeout) {
      var timeoutPromise = new Promise(resolve => {
        setTimeout(() => {
          responseBuffer.err = {
            code: "BatchGetTimeout",
            message: `Dynamaws batch get operation hit given timeout of ${timeout}ms`
          };

          resolve(responseBuffer);
        }, timeout);
      });
    }

    let finishedGetsPromise = batchGetSync(tableName,
      keyString,
      filterFunction,
      responseBuffer,
      limit);

    return timeout ? Promise.race([timeoutPromise, finishedGetsPromise]) : finishedGetsPromise;

  },

  /**
   * Runs a batch of queries synchonously, used in combination with queryIndex
   * @param  {string[]} queries - array of queries to send
   * @param  {object} queryFunction - function to run queries through
   * @param  {object} filterFunction - optional filtering after each request
   * @param  {number} limit - max items to get
   * @param  {number} timeout - optional timeout, causes early return
   * @return {object} items retrieved and next item to continue from
   */
  batchQuery: function (queries, queryFunction, filterFunction, limit = 0, timeout = 0) {

    if (!filterFunction) {
      filterFunction = items => items;
    }

    let responseBuffer = {
      nextItem: null,
      items: []
    }

    // If we hit timeout, whatever is in the buffer is returned immediately
    if (timeout) {
      var timeoutPromise = new Promise(resolve => {
        setTimeout(() => {
          responseBuffer.err = {
            code: "BatchQueryTimeout",
            message: `Dynamaws batch query operation hit given timeout of ${timeout}ms`
          };

          let len = responseBuffer.items.length;
          responseBuffer.nextItem = responseBuffer.items[len - 1] || null;

          if (responseBuffer.nextItem) {
            responseBuffer.items.pop();
          }

          resolve(responseBuffer);
        }, timeout);
      });
    }

    let finishedQueriesPromise = batchQuerySync(_.clone(queries),
      queryFunction,
      filterFunction,
      responseBuffer,
      limit);

    return timeout ? Promise.race([timeoutPromise, finishedQueriesPromise]) : finishedQueriesPromise;

  }
}

