# Leaderboard-Example
A leaderboard for a game with 10 million players

* Used Elasticache (Redis) to hold the score info in a sorted set.
* Used MongoDB to hold player data (such as age, userName)

* Scheduled jobs should be added as:
 - 30 23 * * 0 curl -X GET http://127.0.0.1:8080/performEndOfWeek 
 - 30 23 * * * curl -X GET http://127.0.0.1:8080/performEndOfDay
