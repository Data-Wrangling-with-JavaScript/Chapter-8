"use strict";

const stream = require('stream');

//
// Open a streaming CSV file for output.
//
function openMongodbOutputStream (dbCollection) {

    const csvOutputStream = new stream.Writable({ objectMode: true }); // Create stream for writing data records, note that 'object mode' is enabled.
    csvOutputStream._write = (chunk, encoding, callback) => { // Handle writes to the stream.
        dbCollection.insertMany(chunk)
            .then(() => {
                callback(); // Successfully added to database.
            }) 
            .catch(err => {
                callback(err); // An error occurred, pass it onto the stream.
            }); 
    };

    return csvOutputStream;
};

module.exports = openMongodbOutputStream;