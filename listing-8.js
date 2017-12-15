'use strict';

const argv = require('yargs').argv;
var MongoClient = require('mongodb').MongoClient;

function processData (collection, skipAmount, limitAmount) {
    return collection.find()
        .skip(skipAmount)
        .limit(limitAmount)
        .toArray()
        .then(data => {
            console.log(">> Your code to process " + data.length + " records here!"); 
        });
};

//
// Open the connection to the database.
//
function openDatabase () {
    var MongoClient = require('mongodb').MongoClient;
    return MongoClient.connect('mongodb://localhost')
        .then(client => {
            var db = client.db('weather_stations');
            var collection = db.collection('daily_readings');
            return {
                collection: collection,
                close: () => {
                    return client.close();
                },
            };
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
