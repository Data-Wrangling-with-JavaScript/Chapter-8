"use strict";

const papa = require('papaparse');
const fs = require('fs');

//
// Read a text file form the file system.
//
function read (fileName) {
    return new Promise((resolve, reject) => {
        fs.readFile(fileName, "utf8",
            function (err, textFileData) {
                if (err) {
                    reject(err);
                    return;
                }

                resolve(textFileData);
            }
        );
    });
};

//
// Helper function to import a CSV file.
//
function importCsvFile (filePath) {
	return read(filePath)
		.then(textFileData => {
			const result = papa.parse(textFileData, {
				header: true,
				dynamicTyping: true,
			});
			return result.data;
		});
};

function exportToMongoDB (db, collectionName, data) {
    return data.reduce((prevPromise, row) => {
        return prevPromise.then(() => {
            return db[collectionName].insert(row);
        });
    }, Promise.resolve());
};

const mongo = require('promised-mongo');

const db = mongo("localhost:27017/weather_stations", ["daily_readings"]);

importCsvFile("/code/data/weather-stations.csv")
    .then(data => exportToMongoDB(db, "daily_readings", data))
    .then(() => db.daily_readings.createIndex({ Year: 1 }))
    .then(() => db.daily_readings.createIndex({ Year: -1 }))
    .then(() => db.close())
    .catch(err => {
        console.error("An error occurred.");
        console.error(err.stack);
    });
