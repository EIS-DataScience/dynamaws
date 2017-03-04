"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

const _ = require("lodash");

const DEFAULT_BATCH_WRITE_DELAY = 300; // ms

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

function batchWriteSync(tableName, items, buffer, writeDelay) {

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
        let success = _.differenceBy(batch, failed, 'id');
        _.pullAllBy(buffer.failed, success, 'id');
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

        // Note: we assume here that items returned 
        // from dynamo are sorted by original query 
        // key, therefore last item becomes nextItem
        if (buffer.items.length >= limit || queries.length === 0) {
          buffer.nextItem = items.length > limit ? items[limit] : null;
          buffer.items = buffer.items.slice(0, limit);
          return buffer;
        }

        return runBatchQueries(queries[0]);

      });
  }

  return runBatchQueries(queries[0])
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

  // TODO: Deal with UnprocessedKeys, exponential backoff.
  batchGet: function (tableName, keyString, keyValues) {
    const keys = keyValues.map((keyValue) => {
      let keyObj = {};

      keyObj[keyString] = keyValue;

      return keyObj;
    });

    const BATCH_LIMIT = 100;
    const batches = keys.reduce((acc, key) => {
      let lastIndex = acc.length === 0 ? 0 : acc.length - 1;

      if (!acc[lastIndex]) {
        acc[lastIndex] = [];
      }

      if (acc[lastIndex].length >= BATCH_LIMIT) {
        acc.push([]);

        lastIndex = acc.length - 1;
      }

      acc[lastIndex].push(key);

      return acc;
    }, []);

    const batchesDonePromises = batches.map((keysBatch) => {
      const params = {
        RequestItems: {
          [tableName]: {
            Keys: keysBatch
          }
        }
      };

      return docClient.batchGet(params)
        .promise()
        .then((dynamoData) => {
          if (Object.keys(dynamoData.UnprocessedKeys).length > 0) {
            console.warn("There were unprocessed keys in batchGet. The keys are:", dynamoData.UnprocessedKeys);
          }

          return dynamoData.Responses[tableName];
        });
    });

    return Promise.all(batchesDonePromises)
      .then((resultArr) => {
        return _.flatten(resultArr);
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
  batchCreate: function (tableName, items, timeout = 0, writeDelay = DEFAULT_BATCH_WRITE_DELAY) {

    // response is passed by ref to batchWrite on purpose
    let responseBuffer = {
      successful: [],
      failed: items,
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

    // batchWrite() fills the responseBuffer with write results
    let finishedWritePromise = batchWriteSync(tableName, items, responseBuffer, writeDelay)

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

  batchDelete: function (tableName, keyString, keyValues) {
    var deleteRequestArr = keyValues.map((keyVal) => {
      return {
        DeleteRequest: {
          Key: { [keyString]: keyVal }
        }
      };
    });

    return batchWrite(tableName, deleteRequestArr);
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
          responseBuffer.nextItem = responseBuffer.items[len - 1] ?
            responseBuffer.items[len - 1] :
            null;

          if (responseBuffer.nextItem) {
            responseBuffer.items.pop();
          }

          resolve(responseBuffer);
        }, timeout);
      });
    }

    let finishedQueriesPromise = batchQuerySync(queries,
      queryFunction,
      filterFunction,
      responseBuffer,
      limit);

    return timeout ? Promise.race([timeoutPromise, finishedQueriesPromise]) : finishedQueriesPromise;

  }
}

