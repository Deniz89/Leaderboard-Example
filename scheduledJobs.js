var Promise = require('promise');
var redisClient = require('redis').createClient
var redis = redisClient(6379, 'leaderboard.va0tug.ng.0001.use2.cache.amazonaws.com')
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
moment().format();

var dbHelper = require('./dbHelper.js')

var url = `mongodb://localhost:27017`


exports.performEndOfDay = () => {
 return new Promise( (resolve, reject) => {

  // delete yesterday s leaderboard
  redis.del("leaderboard_yesterday", (err, response) => {
   if(err) return reject(err)

   // copy leaderboard leaderboard_yesterday
   redis.zunionstore("leaderboard_yesterday", 1, "leaderboard", (err, res) => {
    if(err) return reject(err)

    return resolve()
   })
  })
 })
}

exports.performEndOfWeek = () => {
 return new Promise ( (resolve, reject) => {

  redis.get("totalScore", (err, response) => {

   var totalScore = response

   redis.zrevrangebyscore(['leaderboard', '+inf', '-inf', 'LIMIT', '0' ,'100'], (err, top100list) => {
    dbHelper.getCollection('leaderboard', 'PrizeInfo').then( (collResp) => {

     var client = collResp.client
     var collection = collResp.collection

     var promiseList = []

     for(let i=0;i<top100list.length;i++) {
      if(i==0) {
       promiseList.push( insertWithPromise(collection, top100list[0],  (totalScore * (20/100)) ) )
      }
      else if(i==1) {
       promiseList.push( insertWithPromise(collection, top100list[1],  (totalScore * (15/100)) ) )
      }
      else if(i==2) {
       promiseList.push( insertWithPromise(collection, top100list[2],  (totalScore * (10/100)) ) )
      }
      else {
       promiseList.push( insertWithPromise(collection, top100list[i],  (totalScore * (55/100)) * ( 1/(i+1) ) ) )
      }
     }

     Promise.all(promiseList).then( () => {
      redis.zunionstore("leaderboard", 1, "leaderboard", "WEIGHS", 0, (err, resp) => {
       if(err) return reject(err)

       redis.set("totalScore", 0, (err, response) => {
        if(err) return reject(err)

        return resolve()
       })

      })
     })

    })

   })

  })

 })

}

var insertWithPromise = (collection, userId, prize) => {
 return new Promise( (resolve, reject) => {
  collection.insert({ 'userId': userId, 'prize': prize}, (err, result) => {
   return resolve()
  })
 })
}
