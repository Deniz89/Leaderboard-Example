const MongoClient = require('mongodb').MongoClient
const url = `mongodb://localhost:27017`

// gets the collection whose is labeled by the name argument
exports.getCollection = (dbName, collectionName) => {
 return new Promise( (resolve, reject)  => {
  MongoClient.connect(url, (err, client) => {
   if(err) return reject(err)

   var db = client.db(dbName)
   var collection = db.collection(collectionName)

   console.log("dbHelper: getCollection")

   return resolve({"client": client, "collection": collection})
  })
 })
}

