"use strict";

const argv = require('yargs').argv;
const MongoClient = require('mongodb').MongoClient;
const spawn = require('child_process').spawn;
const parallel = require('async-await-parallel');

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

//
// Run the slave process.
//
function runSlave (skip, limit, slaveIndex) {
    return new Promise((resolve, reject) => {
        const args = [ "listing-8.8.js", "--skip", skip, "--limit", limit ];

        const childProcess = spawn("node", args);
        childProcess.stdout.on("data", data => {
            console.log("[" + slaveIndex + "]: INF: " + data);
        });

        childProcess.stderr.on("data", data => {
            console.log("[" + slaveIndex + "]: ERR: " + data);
        });

        childProcess.on("close", code => {
            if (code === 0) {
                resolve();
            }
            else {
                reject(code);
            }
        });

        childProcess.on("error", err => {
            reject(err);
        });
    });
};

//
// Run the slave process for a particular batch of records.
//
function processBatch (batchIndex, batchSize) {
    const startIndex = batchIndex * batchSize;
    return () => { // Encapsulate in an anon fn so that execution is deferred until later.
        return runSlave(startIndex, batchSize, batchIndex);
    };
};

//
// Process the entire database in batches of 100 records.
// 2 batches are processed in parallel, but this number can be tweaked based on the number of cores you
// want to throw at the problem.
//
function processDatabase (numRecords) {

    const batchSize = 100; // The number of records to process in each batchs.
    const maxProcesses = 2; // The number of process to run in parallel.
    const numBatches = numRecords / batchSize; // Total number of batches that we need to process.
    const slaveProcesses = [];
    for (let batchIndex = 0; batchIndex < numBatches; ++batchIndex) {
        slaveProcesses.push(processBatch(batchIndex, batchSize));
    }

    return parallel(slaveProcesses, maxProcesses);
};

openDatabase()
    .then(db => {
        return db.collection.find().count() // Determine number of records to process.
            .then(numRecords => processDatabase(numRecords)) // Process the entire database.
            .then(() => db.close()); // Close the database when done.
    })
    .then(() => {
        console.log("Done processing all records.");
    })
    .catch(err => {
        console.error("An error occurred reading the database.");
        console.error(err);
    });

