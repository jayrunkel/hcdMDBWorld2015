
var writeQ = require("./writeQueue.js");
var dal = require("./dalShim.js");
var mongoDAL = require("./dalMongoDB.js");
var mysqlDAL = require("./dalMySQL.js");
var MongoClient = require('mongodb').MongoClient;
var mysql = require('mysql');
var step = require('step');


//var writeModes = ["RDBMS", "Transfer", "Drain", "Both", "MongoDB"];
//var writeMode = "RDBMS";


var hId = 100000;
var pId = 100000;
var transferLogUnit = 250;

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
            function getHospitals () {
                console.log("");
                console.log("================================================================");
                console.log("[RDBMS] Cleaning up from previous run...");
                console.log("================================================================");
                console.log("");
                mysqlConn.query("SELECT _id FROM hospitals WHERE _id >= 100000", this);
            },
            function cleanUpMysqlHos (err, hIds) {
                var group = this.group();
                console.log("Cleaning up hospitals: ", hIds);

                hIds.forEach(function (hId) {
                    mysqlDAL.deleteHospital(mysqlConn, hId._id, group());
                });
            },
            function getPhysicians () {
                mysqlConn.query("SELECT _id FROM physicians WHERE _id >= 100000", this);
            },
            function cleanUpMysqlPhy (err, pIds) {
                var group = this.group();
                console.log("Cleaning up physicians: ", pIds);

                pIds.forEach(function (pId) {
                    mysqlDAL.deletePhysician(mysqlConn, pId._id, group());
                });
            },
            function dropDB (err, result) {
                mongoConn.dropDatabase(this);
            },
            function initializeWriteQ(err, result) {
                if (err) callback(err);
                writeQ.initializeQueue(mongoConn, this);
            },
            function startTest () {
                transferTest(this.parallel());
                addHospitals(mysqlConn, mongoConn, this.parallel());
                addPhysicians(mysqlConn, mongoConn, this.parallel());
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


function addPhysicians(mysqlConn, mongoConn, callback) {
    var physician;

    step(
        function getWriteMode() {
            return writeQ.getWriteMode(mongoConn, this);
        },
        function performWrites(err, result) {
            var writeMode = result;
            if (writeMode == "RDBMS") {
                // don't write anything. Transfer hasn't started
                addPhysicians(mysqlConn, mongoConn, callback);
            }
            else if (writeMode != "Both") {
                physician = {
                    _id : ++pId,
                    first : "Doc_" + pId,
                    last : "Smith_" + pId,
                    addr : {
                        street : "1 Oak Rd", 
                        city : "New York", 
                        state : "NY", 
                        zip : 11111
                    }
                };
                console.log("[transferTest.js addPhysician] Adding physician: ", physician._id);
                dal.createPhysician(writeMode, mysqlConn, mongoConn, physician, function(err, result) {
                    if (err) return callback(err, null);
                    console.log("[transferTest.js addPhysician] physician: ", physician._id, " added.");
                    addPhysicians(mysqlConn, mongoConn, callback);
                });
            }
        }
    );
}

function addHospitals(mysqlConn, mongoConn, callback) {
    var hospital;

    step(
        function getWriteMode() {
            return writeQ.getWriteMode(mongoConn, this);
        },
        function performWrites(err, result) {
            var writeMode = result;
            if (writeMode == "RDBMS") {
                // don't write anything. Transfer hasn't started
                addHospitals(mysqlConn, mongoConn, callback);
            }
            else if (writeMode != "Both") {
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
    var transferCount = 0;

    query.on('error', function(err) {
        // Handle error, an 'end' event will be emitted after this as well
        console.log("Error selecting all hospitals for transfer to MongoDB", err);
        callback(err, null);
    });
    
//    query.on('fields', function(fields) {
    // the field packets for the rows to follow
//    });
    
    query.on('result', function(row) {

        if (++transferCount%transferLogUnit == 0) 
            console.log("[transferTest.js transferHospital] Hospitals Transfered: ", transferCount);


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

function transferPhysicians(mysqlConn, mongoConn, callback) {
    var query = mysqlConn.query('SELECT * FROM physicians');
    var transferCount = 0;

    query.on('error', function(err) {
        // Handle error, an 'end' event will be emitted after this as well
        console.log("Error selecting all physicians for transfer to MongoDB", err);
        callback(err, null);
    });
    
//    query.on('fields', function(fields) {
    // the field packets for the rows to follow
//    });
    
    query.on('result', function(row) {

        if (++transferCount%transferLogUnit == 0) 
            console.log("[transferTest.js transferPhysician] Physicians Transfered: ", transferCount);


        mysqlConn.query("SELECT hospital FROM hosPhysiciansRel WHERE physician = ?", row._id, function (err, rows) {
            var hospitals = [];
//            mysqlConn.pause();

            for (var i = 0; i < rows.length; i++) {
                hospitals.push(rows[i].hospital);
            }
            row.hospitals = hospitals;
            mongoDAL.createPhysician(mongoConn, row, function (err, result) {
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

        function transferWriteMode() {
            console.log("");
            console.log("================================================================");
            console.log("[TRANSFER] Starting the transfer of hospitals...");
            console.log("================================================================");
            console.log("");
            writeQ.nextWriteMode(mongoConn, this);
        },
        function transferHospitalRecords(err, result) {
            if (err)
                console.log("Error changing write mode", err);
            else {
//                console.log("Transfering Hospital Records");
//                console.log("Current write mode: ", result);
                transferHospitals(mysqlConn, mongoConn, this);
            }
        },
        function transferPhysicianRecords(err, result) {
            if (err)
                console.log("Error loading hospitals", err);
            else {
//                console.log("Transfering Physician Records");
//                console.log("Current write mode: ", result);
                transferPhysicians(mysqlConn, mongoConn, this);
            }
        },
        function drainWriteMode(err, result) {
            if (err) callback(err);
            console.log("");
            console.log("================================================================");
            console.log("[DRAIN] Transfer complete. Draining queue...");
            console.log("================================================================");
            console.log("");            
            writeQ.nextWriteMode(mongoConn, this);
        },
        function drainQueue(err, result) {
            if (err) callback(err);
            writeQ.processQueue(mongoConn, this);
        },
        function bothWriteMode(err, result) {
            if (err) callback(err);
            console.log("");
            console.log("================================================================");
            console.log("[BOTH] Queue drained. Writing to both DBs...");
            console.log("================================================================");
            console.log("");
            writeQ.nextWriteMode(mongoConn, this);
        },
        function done (err, result) {
            callback(err, result);
        }

    );
}
