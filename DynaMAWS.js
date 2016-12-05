"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();
const docClient = new AWS.DynamoDB.DocumentClient();

const _ = require("lodash");

function batchWrite(tableName, requestArr) {
  const BATCH_LIMIT = 25;

  let itemsLength = requestArr.length;
  let batches = [];

  for(let i=0; i<itemsLength; i+=BATCH_LIMIT) {
    let endIndex = i + BATCH_LIMIT;
    let sliceEndIndex = endIndex <= itemsLength? endIndex : itemsLength;

    let batch = requestArr.slice(i, sliceEndIndex);

    batches.push(batch);
  }

  function doBatchWrite(batch) {
    let params = {
      RequestItems: {}
    };

    params.RequestItems[tableName] = batch;

    return docClient.batchWrite(params).promise();
  }

  let sequence = Promise.resolve({});

  for(let i=0; i<batches.length; i++) {
    let batch = batches[i];

    sequence = sequence.then(function(sequenceItem) {
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          const toResolve = doBatchWrite(batch)
            .then((dynamoData) => {
              return _.merge(sequenceItem, dynamoData);
            });

          resolve(toResolve);
        }, 300)
      })
    });
  }

  return sequence;
}

function handleQueryResponse(queryResponse, params) {
  let items = []

  function handleQueryResponseHelper(queryResponse, params) {
    items = items.concat(queryResponse.Items);

    if(queryResponse.LastEvaluatedKey) {
      const clonedParams = _.cloneDeep(params);

      clonedParams["ExclusiveStartKey"] = queryResponse.LastEvaluatedKey;

      return docClient.query(clonedParams)
        .promise()
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

module.exports = {
  get: function(tableName) {
    const dynamoUsersPromise = docClient.scan({
      TableName : tableName,
    })
    .promise();


    return dynamoUsersPromise.then(function(dynamoData) {
      return dynamoData.Items;
    });
  },

  getOne: function(tableName, key) {
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
  batchGet: function(tableName, keyString, keyValues) {
    const keys = keyValues.map((keyValue) => {
      let keyObj = {};

      keyObj[keyString] = keyValue;

      return keyObj;
    });

    const BATCH_LIMIT = 100;
    const batches = keys.reduce((acc, key) => {
      let lastIndex = acc.length === 0? 0 : acc.length - 1;

      if(!acc[lastIndex]) {
        acc[lastIndex] = [];
      }

      if(acc[lastIndex].length >= BATCH_LIMIT) {
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
          if(Object.keys(dynamoData.UnprocessedKeys).length > 0) {
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

  create: function(tableName, newItem) {
    const params = {
      TableName : tableName,
      Item: newItem,
    };

    return docClient.put(params)
      .promise()
      .then(() => {
        return newItem;
      });
  },

  update: function(tableName, key, newItem) {
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

  // Note: Batch operations are NOT atomic and may fail halfway.
  // TODO: Deal with unprocessed items, return created items.
  batchCreate: function(tableName, items) {
    var putRequestArr = items.map(function(item) {
      return {
        PutRequest: {
          Item: item
        }
      };
    });

    return batchWrite(tableName, putRequestArr);
  },

  // Note: Batch operations are NOT atomic and may fail halfway.
  // TODO: Deal with unprocessed items, return created items.
  // batchUpdate: function(tableName, keyString, items) {
  //   let sequence = Promise.resolve();

  //   items.forEach((item) => {
  //     const keyObj = {};
  //     keyObj[keyString] = items[keyString];

  //     sequence = sequence.then(() => {
  //       return new Promise((resolve, reject) => {
  //         setTimeout(() => {
  //           resolve(this.update(tableName, keyObj, item));
  //         }, 20)
  //       });
  //     });
  //   });

  //   return sequence;
  // },


  delete: function(tableName, key) {
    var params = {
      TableName : tableName,
      Key: key,
      ReturnValues: "ALL_OLD"
    };

    return docClient.delete(params)
    .promise()
    .then((dynamoData) => {
      return dynamoData.Attributes;
    });
  },

  batchDelete: function(tableName, keyString, keyValues) {
    var deleteRequestArr = keyValues.map((keyVal) => {
      return {
        DeleteRequest: {
          Key: {[keyString]: keyVal}
        }
      };
    });

    return batchWrite(tableName, deleteRequestArr);
  },

  queryIndex: function(tableName, indexName, queryKey, queryVal) {
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

    return docClient.query(params)
      .promise()
      .then((dynamoData) => {
        return handleQueryResponse(dynamoData, params);
      });
  }
}
