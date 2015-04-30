
var writeQ = require("./writeQueue.js");
var dal = require("./dalShim.js");
var mongoDAL = require("./dalMongoDB.js");
var MongoClient = require('mongodb').MongoClient;
var mysql = require('mysql');
var step = require('step');


//var writeModes = ["RDBMS", "Transfer", "Drain", "Both", "MongoDB"];
//var writeMode = "RDBMS";


var hId = 100000;

var mongoConn = null;

var mysqlConn = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'hcd'
});

// var mongoDBCollections = {
//     hospitals : "hospitals",
//     physicians : "physicians",
//     patients : "patients",
//     procedures : "procedures",
//     records : "records"
// };

mysqlConn.connect();

MongoClient.connect('mongodb://localhost:27017/hcdTest', function(err, db) {
    if (err)
        console.log('Could not connect to MongoDB');
    else {
        console.log('Connected to MongoDB');
        mongoConn = db;

        step (
            function dropDB () {
                mongoConn.dropDatabase(this);
            },
            function initializeWriteQ(err, result) {
                if (err) callback(err);
                writeQ.initializeQueue(mongoConn, this);
            },
            function startTest () {
                transferTest(this.parallel());
                addHospitals(mysqlConn, mongoConn, this.parallel());
            },
            function finish (err, transfer, add) {
                if (err)
                    console.log("[transferTest.js MongoClient.connect] Transfer error: ", err);
                else
                    console.log("[transferTest.js MongoClient.connect] Transfer complete");
            }
        );
    }
});


function addHospitals(mysqlConn, mongoConn, callback) {
    var hospital;

    step(
        function getWriteMode() {
            return writeQ.getWriteMode(mongoConn, this);
        },
        function performWrites(err, result) {
            var writeMode = result;
            if (writeMode != "Both") {
                hospital = {
                    _id : ++hId,
                    name : "Test Hospital " + hId,
                    city : "Somewhere",
                    state : "XX",
                    beds : 999,
                    traumaCenter : true
                };
                console.log("[transferTest.js addHospital] Adding hospital: ", hospital._id);
                dal.createHospital(writeMode, mysqlConn, mongoConn, hospital, function(err, result) {
                    if (err) return callback(err, null);
                    console.log("[transferTest.js addHospital] hospital: ", hospital._id, " added.");
                    addHospitals(mysqlConn, mongoConn, callback);
                });
            }
        }
    );
}


function transferHospitals(mysqlConn, mongoConn, callback) {
    var query = mysqlConn.query('SELECT * FROM hospitals');

    query.on('error', function(err) {
        // Handle error, an 'end' event will be emitted after this as well
        console.log("Error selecting all hospitals for transfer to MongoDB", err);
        callback(err, null);
    });
    
//    query.on('fields', function(fields) {
    // the field packets for the rows to follow
//    });
    
    query.on('result', function(row) {

        // Pausing the connnection is useful if your processing involves I/O
        console.log("[transferTest.js transferHospital] Inserting hospital ", row._id);


        mysqlConn.query("SELECT physician FROM hosPhysiciansRel WHERE hospital = ?", row._id, function (err, rows) {
            var physicians = [];
//            mysqlConn.pause();

            for (var i = 0; i < rows.length; i++) {
                physicians.push(rows[i].physician);
            }
            row.physicians = physicians;
            mongoDAL.createHospital(mongoConn, row, function (err, result) {
                if (err) return callback(err, null);
//            mysqlConn.resume();
            });

        });
    });

    query.on('end', function() {
        // all rows have been received
        callback(null, true);
    });
}

function transferTest(callback) {

    step (

        function transferWriteMode(err, result) {
            if (err) callback(err);
            console.log("[Transfer] Starting the transfer of hospitals...");
            writeQ.nextWriteMode(mongoConn, this);
        },
        function transferHospitalRecords(err, result) {
            if (err) callback(err);
            console.log("Current write mode: ", result);
            if (err) console.log(err);
            transferHospitals(mysqlConn, mongoConn, this)
        },
        function drainWriteMode(err, result) {
            if (err) callback(err);
            console.log("[Drain] Transfer complete. Draining queue...");
            writeQ.nextWriteMode(mongoConn, this);
        },
        function drainQueue(err, result) {
            if (err) callback(err);
            writeQ.processQueue(mongoConn, this);
        },
        function bothWriteMode(err, result) {
            if (err) callback(err);
            console.log("[Both] Queue drained. Writing to both DBs...");
            writeQ.nextWriteMode(mongoConn, this);
        },
        function done (err, result) {
            callback(err, result);
        }

    );
}
