const express = require('express')
const MongoClient = require('mongodb').MongoClient
const sleep = require('sleep');
const Promise = require('promise');
const axios = require('axios')
const delay = require('delay')
const redisClient = require('redis').createClient
const util = require('util')
const format = util.format

const simulateDB = require('./simulateDB.js');
const rankUtil = require('./rankUtil.js')
const playerOpsUtil = require('./playerOperationsUtil.js')
const scheduledJobs = require('./scheduledJobs.js')
const dbHelper = require('./dbHelper.js')

const redisUrl = 'leaderboard.va0tug.ng.0001.use2.cache.amazonaws.com'
const redisPort = 6379
const redis = redisClient(redisPort, redisUrl)
const app = express()
const url = `mongodb://localhost:27017`

// OBSOLETE
app.get('/', (req, res) => {
 MongoClient.connect(url, function(err, client) {
  if(err) throw err;

  var db = client.db('leaderboard');

  var collection = db.collection('UserInfo');

  collection.find({userName:"111"}, (err, response) => {
   console.log(util.inspect(response))
  })
 })
 res.send('Hello World!')
});

// used to create db between lo and hi
// e.g. lo is 1 and hi is 300000
app.get('/createdb', (req, res) => {

 console.log("lo: " + req.query.lo + ", hi: " + req.query.hi)

 simulateDB.createDB(parseInt(req.query.lo, 10), parseInt(req.query.hi, 10), function() {
  console.log("done")
 });

 res.send('OK, check console.');
})

app.get('/createdbInBatches', (req, res) => {
 dbHelper.getCollection('leaderboard', 'UserInfo').then( (collResp) => {
  var client = collResp.client
  var collection = collResp.collection

  collection.find({}).count( (err, count) => {
   if(err) console.log("err: " + err)

   var lo=count+1

   var step=100000

   for(let i=0;i<4;i++) {
    (function(){
     setTimeout( function(){
      axios.get( 'http://127.0.0.1:8080/createdb?lo=' + ( lo + (i*step) ) + '&hi=' + (lo + (i*step) + step - 1) )
     }, 10000 )
    })(i)
   }

   res.send("dummy ans.")
  })
 })
})

// used to migrate initial score info of players to redis
app.get('/migrateToRedis', (req, res) => {

 console.log("init migrateToRedis: lo: " + req.query.lo + ", hi: " + req.query.hi)

 simulateDB.migrateToRedis(parseInt(req.query.lo),parseInt(req.query.hi), function(){
  console.log("done")
 })

 res.send('Check redis and console.')

})

// OBSOLETE, for testing purp.
app.get('/trial2', (req, res) => {
  axios.get( 'http://127.0.0.1:8080/migrateToRedis?lo=' + 1900001 + '&hi=' + 2000000).then(() => {
   console.log('send successfully.')
   res.send("OK.")
  }).catch((err) => {
   console.log('err: ' + err)
  })
})

// OBSOLETE, for testing purp.
app.get('/trial3', (req, res) => {
 redis.zincrby("leaderboard", "15", "9999280", (err, result) => {
  res.send(JSON.stringify(result))
 })
})


// used to call migrateToRedis end point in batches
// should be moved to cron, but used in the beginning only
app.get('/migrateInBatches', (req, res) => {

redis.zcard("leaderboard", (err, result) => {
 var lo=result+1

 var step=100000

 var promiseList = []

 for(let i=0;i<4;i++) {
  (function(){
   setTimeout( function(){ 
    axios.get( 'http://127.0.0.1:8080/migrateToRedis?lo=' + ( lo + (i*step) ) + '&hi=' + (lo + (i*step) + step - 1) )
   }, 10000 )
  })(i)
 }

 res.send("OK.")
 console.log("done executively.")

})

})

app.get('/getRankChange', (req, res) => {

 rankUtil.getRankChange(req.query.userId).then( (resp) => {

  res.send("result: " + JSON.stringify(resp))
 })

})

app.get('/getLeaderboard', (req, res) => {

 rankUtil.getLeaderboard(req.query.userId).then( (response) => {
  res.send(response)
 }).catch( (reason) => {
  res.send(reason)
 })
})

// for testing only
app.get('/getNeighbours', (req, res) => {

 var userId = req.query.userId

 rankUtil.getNeighboursByUserId(userId).then( (resp) => {

  res.send(resp)
})

})

app.get('/getTop100', (req, res)=>{

 rankUtil.getTop100().then((response) => {
  res.send(response)
 }).catch((reason) => {
  res.send(reason)
 })
})

app.get('/increaseScore', (req, res) => {

 var userId = req.query.userId
 var amount = req.query.amount
 var userName = req.query.userName

 playerOpsUtil.increaseScore(amount, {userId: userId, userName:userName}).then( () => {
  res.send("Check from redis and mongo")
 }).catch( (reason) => { res.send( "err " +  reason)})
})


app.get('/performEndOfDay', (req, res) => {

 scheduledJobs.performEndOfDay().then( ()=> {
  res.send("success, check redis")
 }).catch( (reason) => {
  res.send("err: " + err)
 })
})

app.get('/performEndOfWeek', (req, res) => {

 scheduledJobs.performEndOfWeek().then( ()=> {
  res.send("success, check redis")
 }).catch( (reason) => {
  res.send("err: " + err)
 })
})

app.listen(8080, () => console.log('Mock Leaderboard app listening on port 8080!'))
