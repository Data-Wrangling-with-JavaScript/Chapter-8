"use strict";

const openCsvInputStream = require('./toolkit/open-csv-input-stream');
const openMongodbOutputStream = require('./toolkit/open-mongodb-output-stream');
const MongoClient = require('mongodb').MongoClient;

const hostName = "mongodb://127.0.0.1:6000";
const databaseName = "weather_stations";
const collectionName = "daily_readings";

const inputFilePath = "./data/weather-stations.csv";

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

function streamData (inputFilePath, dbCollection) {
    return new Promise((resolve, reject) => {
        openCsvInputStream(inputFilePath)
            .pipe(openMongodbOutputStream(dbCollection))
            .on("finish", () => {
                resolve();
            })
            .on("error", err => {
                reject(err);
            });
    });
}

openDatabase()
    .then(client => {
        return streamData(inputFilePath, client.collection)
            .then(() => client.close());
    })
    .then(() => {
        console.log("Done");
    })
    .catch(err => {
        console.error("An error occurred.");
        console.error(err);
    });
