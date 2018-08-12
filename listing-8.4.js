"use strict";

const MongoClient = require('mongodb').MongoClient;

const hostName = "mongodb://127.0.0.1:7000";
const databaseName = "weather_stations";
const collectionName = "daily_readings";

//
// Open the connection to the database.
//
function openDatabase () {
    return MongoClient.connect(hostName)
        .then(client => {
            const db = client.db(databaseName);
            const collection = db.collection(collectionName);
            return {
                collection: collection,
                close: () => {
                    return client.close();
                },
            };
        });
};

let numRecords = 0;
let numWindows = 0;

// 
// Read a single data window from the database.
//
function readWindow (collection, windowIndex, windowSize) {
    const skipAmount = windowIndex * windowSize;
    const limitAmount = windowSize;
    return collection.find()
        .skip(skipAmount)
        .limit(windowSize)
        .toArray();
};

// 
// Read the entire database, window by window.
//
function readDatabase (collection, startWindowIndex, windowSize) {
    return readWindow(collection, startWindowIndex, windowSize)
        .then(data => {
            if (data.length > 0) {
                // We got some data back.
                console.log("Window with " + data.length + " elements.");
                
				// TODO: Add your data processsing here.			

                numRecords += data.length;
                ++numWindows;

                // Read the entire database using an asynchronous recursive traversal.
                return readDatabase(collection, startWindowIndex+1, windowSize);
            }
            else {
                // We retreived no data, finished reading.
            }
        })
    
};

openDatabase()
    .then(db => {
		const windowSize = 100;
        return readDatabase(db.collection, 0, windowSize)
            .then(() => {
                return db.close(); // Close database when done.
            });
    })
    .then(() => {
        console.log("Processed " + numRecords + " records in " + numWindows + " windows.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
