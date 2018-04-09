var Promise = require('promise');
var redisClient = require('redis').createClient
var redis = redisClient(6379, 'leaderboard.va0tug.ng.0001.use2.cache.amazonaws.com')
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
moment().format();

var dbHelper = require('./dbHelper.js')

var url = `mongodb://localhost:27017`

var getRankChange = (userId) => {
 return new Promise( (resolve, reject) => {
  redis.zrevrank("leaderboard_yesterday", userId, (err, resp) => {
   if(err) return reject(err)

   var yesterdayRank = resp

   //   console.log("getRankChange, userId: " + userId + ", prevRank: " + yesterdayRank)

   redis.zrevrank("leaderboard", userId, (err, response) => {
    if(err) return reject(err)

    var currentRank = response

    // console.log("getRankChange, userId: " + userId + ", currRank: " + currentRank)

    if(!yesterdayRank) {
     return resolve( { rankChange:0, rank: currentRank } )
    }

    return resolve( {rankChange: yesterdayRank - currentRank, rank: currentRank})

   })

  })
 })
}

exports.getLeaderboard = (userId) => {
 return new Promise( (resolve, reject) => {
  if(!userId) return reject("userId should exists in query string.")

  this.getTop100().then( (res) => {

   this.getNeighboursByUserId(userId).then( (neighbours) => {

    var rankLookUp = Object.assign(res.rankLookUp, neighbours.rankLookUp)

//     console.log(rankLookUp)

    dbHelper.getCollection('leaderboard', 'UserInfo').then( (collResp) => {

     var client = collResp.client
     var collection = collResp.collection

     var keys = Object.keys(rankLookUp).map(Number)

     collection.find({"userId" : {$in : keys}})
      .sort( {'score': -1} )
      .project( {'_id':0, 'userId': 1, 'userName': 1, 'score': 1, 'age': 1} )
      .toArray( (err, result) => {

       if(err) return reject(err)

       for(let i=0;i<result.length;i++) {
        result[i].rankChange = rankLookUp[result[i].userId].rankChange
//        result[i].rank = rankLookUp[result[i].userId].rank
       }

       resolve(result)
     })

    })

   })

  })

 })
}


// gets the top 100 players with their scores and usernames
exports.getTop100 = () => {
 return new Promise( (resolve, reject) => {
  redis.zrevrangebyscore(['leaderboard', '+inf', '-inf', 'LIMIT', '0' ,'100'], (err, response) => {
   if(err) return reject(err)

   if(!response) return reject("Response is null!!!")

   // in order to get rank changes of the users
   var promiseList = []

  //  console.log("getTop100: ranking: " + JSON.stringify(response) )

   for(let i=0;i<response.length;i++) {
    promiseList.push(getRankChange(response[i]))

    response[i] = parseInt(response[i],10)
   }

   Promise.all(promiseList).then( (rankArr) => {

//    console.log("rankArr: " + JSON.stringify(rankArr))
    var rankLookUp = {}

    for(let i=0; i<rankArr.length;i++) {
     rankLookUp[response[i]] = rankArr[i]
    }

    return resolve({rankLookUp: rankLookUp})

/*
    dbHelper.getCollection('leaderboard', 'UserInfo').then( (collResp) => {

     var client = collResp.client
     var collection = collResp.collection

     collection.find({"userId" : {$in : response}})
      .sort( {'score': -1} )
      .project( {'_id':0, 'userId': 1, 'userName': 1, 'score': 1, 'age': 1} )
      .toArray( (err, result) => {

       if(err) return reject(err)

       for(let i=0;i<result.length;i++) {

        result[i].rankChange = rankLookUp[result[i].userId]
       }

       resolve(result)
     })

    })
*/
   })
  })
 })
}

exports.getNeighboursByUserId = (userId) => {

 return new Promise( (resolve, reject) => {
  redis.zrevrank('leaderboard', parseInt(userId) , (err, response) => {
   if(err) return reject(err)

   // TODO check if this is correct
   var myRank = response

   if(myRank <= 96) { return resolve({})}

   redis.zrevrange('leaderboard', myRank-2, myRank+3, (err, neighbours) => {
    if(err) return reject(err)

    if(!neighbours) return reject('neighbours from zrevrange is null')

    for(let i=0;i<neighbours.length;i++){
     neighbours[i] = parseInt(neighbours[i],10)
    }

    // to merge with top 100 results efficiently
    if(myRank > 96 && myRank < 103) {
     neighbours = neighbours.slice(neighbours.length - ( 3 - (99 - myRank) ) )
    }

    var promiseList = []

    for(let i=0;i<neighbours.length;i++){
     promiseList.push(getRankChange(neighbours[i]))
    }

    Promise.all(promiseList).then( (rankArr) => {

     var rankLookUp = {}

     for(let i=0; i<rankArr.length;i++) {
      rankLookUp[neighbours[i]] = rankArr[i]
     }

     return resolve({rankLookUp: rankLookUp})

    })

   })
  })

 })
}


// OBSOLETE
// gets the username
exports.getNeighboursByUsername = (userName) => {

 return new Promise( (resolve, reject) => {

  dbHelper.getCollection('leaderboard', 'UserInfo').then( (collResp) => {

   var client = collResp.client
   var collection = collResp.collection

   collection.findOne({userName: userName}, (err, resp) => {
    if(err) return reject(err)

    if(!resp) return reject('Response from db is null, userName: ' + userName)

    var myUser = resp

    redis.zrevrank('leaderboard', parseInt(myUser.userId) , (err, response) => {
     if(err) return reject(err)

     // TODO check if this is correct
     var myRank = response

     if(myRank <= 99) { return resolve([])}

     redis.zrevrange('leaderboard', myRank-2, myRank+3, (err, neighbours) => {
      if(err) return reject(err)

      if(!neighbours) return reject('neighbours from zrevrange is null')

      collection.find({userId: {$in: neighbours} })
       .project({ '_id': 0, 'userId': 1, 'userName': 1, 'score': 1, 'age': 1})
       .toArray( (err, result) => {

        if(err) return reject(err)

        for(let i=0; i<result.length; i++) {
         result[i].rank = myRank + 1
        }

        resolve(result)

      })
     })
    })
   })
  })
 })
}

exports.getRankChange = getRankChange
