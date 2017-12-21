'use strict';

const MongoClient = require('mongodb').MongoClient;

const hostName = 'mongodb://127.0.0.1:3000';
const databaseName = 'weather_stations';
const collectionName = 'daily_readings';

//
// Open the connection to the database.
//
function openDatabase () {
    return MongoClient.connect(hostName)
        .then(client => {
            var db = client.db(databaseName);
            var collection = db.collection(collectionName);
            return {
                collection: collection,
                close: () => {
                    return client.close();
                },
            };
        });
};

openDatabase()
    .then(db => {
        return db.collection.find() // Retreive only specified fields.
            .sort({
                Precipitation: 1
            })
            .toArray()
            .then(data => {
                console.log(data);
            })
            .then(() => db.close()); // Close database when done.
    })
    .then(() => {
        console.log("Done.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });