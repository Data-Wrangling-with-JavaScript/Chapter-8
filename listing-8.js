'use strict';

const argv = require('yargs').argv;
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

function processData (collection, skipAmount, limitAmount) {
    return collection.find()
        .skip(skipAmount)
        .limit(limitAmount)
        .toArray()
        .then(data => {
            console.log(">> Your code to process " + data.length + " records here!"); 
        });
};

console.log("Processing records " + argv.skip + " to " + (argv.skip + argv.limit));

openDatabase()
    .then(db => {
        return processData(db.collection, argv.skip, argv.limit) // Process the specified chunk of data.
            .then(() => db.close()); // Close the database connection swhen done.
    })
    .then(() => {
        console.log("Done processing records " + argv.skip + " to " + (argv.skip + argv.limit));
    })
    .catch(err => {
        console.error("An error occurred processing records " + argv.skip + " to " + (argv.skip + argv.limit));
        console.error(err);
    });
