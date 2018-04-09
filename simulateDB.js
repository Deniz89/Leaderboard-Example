var Promise = require('promise');
var redisClient = require('redis').createClient
var redis = redisClient(6379, 'leaderboard.va0tug.ng.0001.use2.cache.amazonaws.com')
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
moment().format();

var url = `mongodb://localhost:27017`

// getRandom integer between high and low
function getRandom(high, low) {
 return Math.floor(Math.random() * (high - low + 1)) + low;
}

// used to prepare obj, generate mock player info
function prepareObj(count) {
  var obj = {
   "userId": parseInt(count, 10),
   "userName": "Player" + count,
   "age" : getRandom(35, 18),
   "score": getRandom(500, 0),
   "dateUpdated": moment().format("YYYY-MM-DD hh:mm:ss")
  }

  return obj;
}

// params:
//  collection : collection obj to be inserted
//  obj: obj to be inserted into the collection
var insert = function(collection, obj) {
 return new Promise((resolve, reject) =>  {
  collection.insertOne(obj, function(err, docs){
   if(err) {
    console.log("ERR can not insert to db: " + obj);
   }

   if(obj.count % 50000 == 0) {
    console.log("success: " + obj.userId);
   }
   return resolve();
  })
 })
}

// used to create player info
// where lo is the low limit
//  hi is the high limit
//  integers between lo and hi used to generate user id s
exports.createDB = function(lo, hi, callback) {
  MongoClient.connect(url, function(err, client){
   if(err) throw err;

   var userCount = hi;

   var db = client.db('leaderboard');
   var collection = db.collection('UserInfo');

   var entryList = [];
   var bulkUpdateOps = [];

   console.log("createDB: loop will start.")

   var i=lo

   var bulkInsertCount = 0;

   for(; i <= userCount; i++) {
    var obj = prepareObj(i)
    bulkUpdateOps.push({"insertOne": {"document": obj} })

     // to0 slow and memory consuming
     // used migrateToRedis instead
//    redis.zadd("leaderboard", obj.score, obj.userId)

    if(bulkUpdateOps.length === 1000){
     collection.bulkWrite(bulkUpdateOps).then((r)=> {
      bulkInsertCount++
      // console.log("bulk done success: " + bulkInsertCount)

      // this means bulk inserts with length 1000 ended eventually
      if(bulkInsertCount == ((hi-lo+1)/1000)) {
       client.close()
       callback()
      }
     })
     bulkUpdateOps = []
    }
   }

   if(bulkUpdateOps > 0) {
    collection.bulkWrite(bulkUpdateOps).then((r) => {
     console.log("remaining executed.")
    })
   }

  })
}

// used to migrate score and user id info to elasticache-redis server
// used only in the beginning
exports.migrateToRedis = function(lo, hi, callback) {

  MongoClient.connect(url, function(err, client){

   var db = client.db('leaderboard')
   var collection = db.collection('UserInfo')

   var redisAddCount=0

   collection.find({"userId": {$lte: hi, $gte: lo} }, {"_id":0,"userId":1,"score":1 }).toArray(function(err,docs){
    for(var i=0;i<docs.length;i++){
     // when used with promise, promise.all etc. performance has decreased
     redis.zadd("leaderboard", docs[i].score, docs[i].userId)
    }
   })
  })
}
