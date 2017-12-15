'use strict';

const openCsvInputStream = require('./toolkit/open-csv-input-stream');
const openMongodbOutputStream = require('./toolkit/open-mongodb-output-stream');

const inputFilePath = "./data/weather-stations.csv";

openCsvInputStream(inputFilePath)
    .pipe(openMongodbOutputStream({
        host: "mongodb://localhost",  // Output database settings.
        database: "weather_stations", 
        collection: "daily_readings"
    }));