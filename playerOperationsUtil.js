const express = require('express')
const MongoClient = require('mongodb').MongoClient
const sleep = require('sleep');
const Promise = require('promise');
const axios = require('axios')
const delay = require('delay')
const redisClient = require('redis').createClient
const format = require('util').format

const simulateDB = require('./simulateDB.js');
const rankUtil = require('./rankUtil.js')
const dbHelper = require('./dbHelper.js')

const redisUrl = 'leaderboard.va0tug.ng.0001.use2.cache.amazonaws.com'
const redisPort = 6379
const redis = redisClient(redisPort, redisUrl)
const app = express()
const url = `mongodb://localhost:27017`


// key is of the form { 'userId': [userId] or null,  'userName': [userName] or null }
// if both of them are null, reject with a proper reason
// increment both redis and mongo db score fields
exports.increaseScore = (amount, key) => {
 return new Promise( (resolve, reject) => {

  if(key.userId) {
   redis.zincrby("leaderboard", amount, key.userId, (err, response) => {

    if(err) return reject(err)

    redis.incrby("totalScore", amount, (err, res) => {
     if(err) return reject(err)

     dbHelper.getCollection('leaderboard', 'UserInfo').then( (collResp) => {
      var client = collResp.client
      var collection = collResp.collection

      collection.updateOne( { userId: parseInt(key.userId) }, {$inc : {score: parseInt(amount)}}, (err, res) => {
       if(err) return reject(err)

       client.close()

       return resolve()
      })
     }).catch( (reason) => reject(reason) )

    })

   })
  }

  if(key.userName) {

   dbHelper.getCollection('leaderboard', 'UserInfo').then( (collResp) => {
    var client = collResp.client
    var collection = collResp.collection

    collection.findOne({userName: key.userName}, { '_id': 0, 'userId': 1}, (err, response) => {
     if(err) return reject(err)

     console.log("response: " + JSON.stringify(response))

     if(!response) { return reject("No player found with this username: " + key.userName) }

     var userId = response.userId

     client.close()

     redis.zincrby("leaderboard", amount, userId, (err,resp) => {
      if(err) return reject(err)

      redis.incrby("totalScore", amount, (err, res) => {
       if(err) return reject(err)

       return resolve()
      })

      // unnecessary since total score is being kept in redis
/*
      collection.updateOne( { userId: parseInt(userId) }, {$inc : {score: parseInt(amount) }}, (err, res) => {
       if(err) return reject(err)
      })
*/
     })

    })

   }).catch( (reason) => reject(reason) )

  }

 })

}
