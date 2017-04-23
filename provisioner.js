var AWS = require("aws-sdk");
AWS.config.update({
  region: "ap-southeast-1"
});
var http = require ('https');
var querystring = require ('querystring');
var dynamodb = new AWS.DynamoDB();
var cloudwatch = new AWS.CloudWatch();

var PROVISIONED_UPPER_MARGIN_RATIO = 0.8;
var PROVISIONED_LOWER_MARGIN_RATIO = 0.3;
var PROVISIONED_TO_CONSUMED_IDEAL_RATIO = 1.3;

var MINIMUM_PROVISIONED_READ_CAPACITY = 100;
var MINIMUM_PROVISIONED_WRITE_CAPACITY = 50;

var MINIMUM_DELTA_TO_ACT_UPON = 50;

var MAX_DIAL_DOWNS = 24;

exports.handler = (event, context, callback) =>
{
  console.log("Event received: "+ JSON.stringify(event));
  var snsTopicArn = event.Records[0].Sns.TopicArn;
  var alertMessage = JSON.parse(event.Records[0].Sns.Message);
  var tableName = alertMessage.AlarmName.substring(0, alertMessage.AlarmName.indexOf("-"));
  checkAndUpdateProvisionedCapacity(tableName, snsTopicArn);
}

function checkAndUpdateProvisionedCapacity(tableName, snsTopicArn){
    var savedReads = 0;
    var savedWrites = 0;
    getTableMetric("ConsumedReadCapacityUnits", tableName, function(err, tableName, consumedReadCapacity){
      getTableMetric("ConsumedWriteCapacityUnits", tableName, function(err, tableName, consumedWriteCapacity){
        getTableMetric("ThrottledReadRequests", tableName, function(err, tableName, throttledReadRequests){
          getTableMetric("ThrottledWriteRequests", tableName, function(err, tableName, throttledWriteRequests){
              getTableProvisionedCapacity(tableName, function(err, provisionedReadCapacity, provisionedWriteCapacity, lastDecreaseDateTime, numberOfDecreasesToday){
                  getOptimizedTableCapacity(provisionedReadCapacity, consumedReadCapacity, throttledReadRequests, provisionedWriteCapacity, consumedWriteCapacity, throttledWriteRequests, lastDecreaseDateTime, numberOfDecreasesToday, function(newProvisionedReadCapacity, newProvisionedWriteCapacity){
                      if(provisionedReadCapacity !== newProvisionedReadCapacity || provisionedWriteCapacity !== newProvisionedWriteCapacity)
                      {
                          var message = "";
                          savedReads += (provisionedReadCapacity - newProvisionedReadCapacity);
                          savedWrites += (provisionedWriteCapacity - newProvisionedWriteCapacity);
                          updateTableCapacity(tableName, newProvisionedReadCapacity, newProvisionedWriteCapacity);
                          if(provisionedReadCapacity !== newProvisionedReadCapacity)
                          {
                            var subMessage = tableName +": Consumed Read ("+ consumedReadCapacity.toFixed(0) +") " + provisionedReadCapacity+" => " + newProvisionedReadCapacity.toFixed(0);
                            var bottomReadThreshold = (newProvisionedReadCapacity * PROVISIONED_LOWER_MARGIN_RATIO * 60).toFixed(0);
                            var topReadThreshold = (newProvisionedReadCapacity * PROVISIONED_UPPER_MARGIN_RATIO * 60).toFixed(0);
                            subMessage += ", Alarm Updated to: "+ Math.round(topReadThreshold/60);
                            updateCloudWatchAlarm(tableName, "ConsumedReadCapacityUnits", MINIMUM_PROVISIONED_READ_CAPACITY, bottomReadThreshold, topReadThreshold, snsTopicArn);
                            message += subMessage +"\n";
                          }
                          if(provisionedWriteCapacity !== newProvisionedWriteCapacity)
                          {
                            var subMessage = tableName +": Consumed Write ("+ consumedWriteCapacity.toFixed(0) +") " + provisionedWriteCapacity.toFixed(0)+" => " + newProvisionedWriteCapacity.toFixed(0);
                            var bottomWriteThreshold = (newProvisionedWriteCapacity * PROVISIONED_LOWER_MARGIN_RATIO * 60).toFixed(0);
                            var topWriteThreshold = (newProvisionedWriteCapacity * PROVISIONED_UPPER_MARGIN_RATIO * 60).toFixed(0);
                            subMessage += ", Alarm Updated to: "+ Math.round(topWriteThreshold/60);
                            message += subMessage +"\n";
                            updateCloudWatchAlarm(tableName, "ConsumedWriteCapacityUnits",MINIMUM_PROVISIONED_WRITE_CAPACITY,  bottomWriteThreshold, topWriteThreshold, snsTopicArn);
                          }
                          console.log(message);
                      }
                  });
                });
             });
          });
      });
    });
}

//getTableMetric("ConsumedReadCapacityUnits", "Prod.Clips", function(err, tableName, data){console.log("Data:" + JSON.stringify(data, null, 2))});
function getTableMetric(metricName, tableName, _callback)
{
    var params = {
      EndTime: new Date ,
      MetricName: metricName, /* required */
      Namespace: 'AWS/DynamoDB', /* required */
      Period: 60, /* required */
      StartTime: new Date(new Date().getTime() - 120000),
      Dimensions: [
        {
          Name: 'TableName',
          Value: tableName
        }
      ],
      Statistics: [
        'Sum',
        'Average'
      ],
      Unit: 'Count'
    };
    console.log("Params: "+ JSON.stringify(params));
    cloudwatch.getMetricStatistics(params, function(err, data) {
      if (err){
          _callback(err, tableName, null);
      } // an error occurred
      else
      {
        var capacity = -1;
        console.log(data);
        if(data.Datapoints.length == 1)
        {
            capacity = data.Datapoints[0].Sum;
        }
        else if(data.Datapoints.length == 2)
        {
            var timeStamp = data.Datapoints[0].Timestamp;
            capacity =  data.Datapoints[0].Sum;
            if(timeStamp > data.Datapoints[1].Timestamp)
            {
                capacity = data.Datapoints[1].Sum;
            }
        }
        if(capacity != -1) capacity = capacity / 60;
        _callback(null, tableName, capacity);
        console.log(tableName +" :" + metricName + " : "+ capacity);
      }
    });

}

//getTableProvisionedCapacity("Prod.Clips", function(err, data){console.log("Data:" + JSON.stringify(data, null, 2))});
function getTableProvisionedCapacity(tableName,_callback){

  var params = {
    TableName: tableName /* required */
  };
  dynamodb.describeTable(params, function(err, data) {
    if (err){
      _callback(err,null,null);
    }
    else{
      var provisionedReadCapacity=data.Table.ProvisionedThroughput.ReadCapacityUnits;
      var provisionedWriteCapacity=data.Table.ProvisionedThroughput.WriteCapacityUnits;
      var lastDecreaseDateTime = data.Table.ProvisionedThroughput.LastDecreaseDateTime;
      var numberOfDecreasesToday = data.Table.ProvisionedThroughput.NumberOfDecreasesToday;
      _callback(null,provisionedReadCapacity,provisionedWriteCapacity, lastDecreaseDateTime, numberOfDecreasesToday);
    }
  });
}

//getOptimizedTableCapacity(115, 100, 210, 200, '2016-07-01T13:54:36.881Z', function(newProvisionedReadCapacity, newProvisionedWriteCapacity){console.log(shouldUpdateTableCapacity, newProvisionedReadCapacity, newProvisionedWriteCapacity);})
function getOptimizedTableCapacity(provisionedReadCapacity,consumedReadCapacity, throttledReadRequests, provisionedWriteCapacity, consumedWriteCapacity, throttledWriteRequests, lastDecreaseDateTime, numberOfDecreasesToday, _callback){
    consumedReadCapacity += throttledReadRequests;
    consumedWriteCapacity += throttledWriteRequests;
    var newProvisionedReadCapacity = provisionedReadCapacity;
    var SLOT_LENGTH = 24 / MAX_DIAL_DOWNS;
    var slots_completed = new Date().getHours() / SLOT_LENGTH;

    if(consumedReadCapacity >= 0 && (consumedReadCapacity > provisionedReadCapacity * PROVISIONED_UPPER_MARGIN_RATIO && slots_completed >= numberOfDecreasesToday) || (consumedReadCapacity < provisionedReadCapacity * PROVISIONED_LOWER_MARGIN_RATIO)){
      newProvisionedReadCapacity = Math.round(PROVISIONED_TO_CONSUMED_IDEAL_RATIO * consumedReadCapacity);
      if(newProvisionedReadCapacity < MINIMUM_PROVISIONED_READ_CAPACITY)
      {
          newProvisionedReadCapacity = MINIMUM_PROVISIONED_READ_CAPACITY;
      }
      if(newProvisionedReadCapacity < provisionedReadCapacity && provisionedReadCapacity - newProvisionedReadCapacity < MINIMUM_DELTA_TO_ACT_UPON){
            newProvisionedReadCapacity = provisionedReadCapacity;
      }
    }
    var newProvisionedWriteCapacity = provisionedWriteCapacity;

    if(consumedWriteCapacity >= 0 && (consumedWriteCapacity > provisionedWriteCapacity * PROVISIONED_UPPER_MARGIN_RATIO && slots_completed >= numberOfDecreasesToday) || (consumedWriteCapacity < provisionedWriteCapacity * PROVISIONED_LOWER_MARGIN_RATIO)){
      newProvisionedWriteCapacity = Math.round(PROVISIONED_TO_CONSUMED_IDEAL_RATIO * consumedWriteCapacity);
      if(newProvisionedWriteCapacity < MINIMUM_PROVISIONED_WRITE_CAPACITY)
      {
          newProvisionedWriteCapacity = MINIMUM_PROVISIONED_WRITE_CAPACITY;
      }
      if(newProvisionedWriteCapacity < provisionedWriteCapacity && provisionedWriteCapacity - newProvisionedWriteCapacity < MINIMUM_DELTA_TO_ACT_UPON){
          newProvisionedWriteCapacity = provisionedWriteCapacity;
      }
    }

    console.log("ConsumedRead: " + consumedReadCapacity +", Provisioned: "+ provisionedReadCapacity +", NewProvisioned: "+ newProvisionedReadCapacity);
    _callback(newProvisionedReadCapacity, newProvisionedWriteCapacity);
}

//updateTableCapacity("Capacity_Test", 55, 5);
function updateTableCapacity(tableName, newReadCapacity, newWriteCapacity){
 var params = {
       TableName: tableName, /* required */
       ProvisionedThroughput: {
         ReadCapacityUnits: newReadCapacity,
         WriteCapacityUnits: newWriteCapacity
       }
     };
     dynamodb.updateTable(params, function(err, data) {
       if (err) console.log(err, err.stack); // an error occurred
     });
}

//updateCloudWatchAlarm('Capacity_Test', 'ConsumedWriteCapacityUnits', 100);
function updateCloudWatchAlarm(tableName, metricName, miniumumProvisionedCapacity, bottomThreshold, topThreshold, snsTopicArn)
{
    console.log("Update Cloudwatch Alarm: tableName: "+ tableName +", metricName:" + metricName +", bottomThreshold: "+ bottomThreshold +", topThreshold:"+ topThreshold);
    if(bottomThreshold <= miniumumProvisionedCapacity * PROVISIONED_LOWER_MARGIN_RATIO){
        bottomThreshold = -1; //To avoid hitting alarm for unused tables
    }
     var alarmName = tableName+"-"+metricName.replace("Consumed","")+"Bottom-BasicAlarm";
     var params = {
       AlarmName: alarmName,
       ComparisonOperator: 'LessThanOrEqualToThreshold',
       EvaluationPeriods: 1, /* required */
       MetricName: metricName, /* required */
       Namespace: 'AWS/DynamoDB', /* required */
       Period: 60, /* required */
       Threshold: bottomThreshold, /* required */
       AlarmActions:[
         snsTopicArn
       ],
       Dimensions: [
         {
           Name: 'TableName', /* required */
           Value: tableName /* required */
         },
         /* more items */
       ],
       Statistic: 'Sum',
       Unit: 'Count'
     };
     cloudwatch.putMetricAlarm(params, function(err, data) {
       if (err) console.log(err, err.stack); // an error occurred
     });
     updateAlarmStateToOK(alarmName);

    var alarmName = tableName+"-"+metricName.replace("Consumed","")+"Limit-BasicAlarm";
    var params = {
      AlarmName: alarmName,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
      EvaluationPeriods: 1, /* required */
      MetricName: metricName, /* required */
      Namespace: 'AWS/DynamoDB', /* required */
      Period: 60, /* required */
      Threshold: topThreshold, /* required */
      AlarmActions:[
        snsTopicArn
      ],
      Dimensions: [
        {
          Name: 'TableName', /* required */
          Value: tableName /* required */
        },
        /* more items */
      ],
      Statistic: 'Sum',
      Unit: 'Count'
    };
    cloudwatch.putMetricAlarm(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
    });
    updateAlarmStateToOK(alarmName);
}

function updateAlarmStateToOK(alarmName)
{
    var params = {
      AlarmName: alarmName,
      StateReason: "Set to OK by Dynamo Autoscaling",
      StateValue: "OK"
    };
    console.log("Calling setAlarmState with params: "+ JSON.stringify(params));
    cloudwatch.setAlarmState(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
    });
}