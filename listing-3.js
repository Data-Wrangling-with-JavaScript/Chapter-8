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

//
// Read the entire database, document by document using a database cursor.
//
function readDatabase (cursor) {
    return cursor.next()
        .then(record => {
            if (record) {
                // Found another record.
                // Put your code here for processing the record.
                console.log(record);
                ++numRecords;

                // Read the entire database using an asynchronous recursive traversal.
                return readDatabase(cursor);
            }
            else {
                // No more records.
            }
        });
};

openDatabase()
    .then(db => {
        const databaseCursor = db.collection.find();
        return readDatabase(databaseCursor) // NOTE: You could use a query here.
            .then(() => db.close()); // Close database when done.
    })
    .then(() => {
        console.log("Displayed " + numRecords + " records.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });
