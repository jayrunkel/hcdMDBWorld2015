
var writeQ = require("./writeQueue.js");
var dal = require("./dalShim.js");
var mongoDAL = require("./dalMongoDB.js");
var MongoClient = require('mongodb').MongoClient;
var mysql = require('mysql');


var writeModes = ["RDBMS", "Transfer", "Drain", "Both", "MongoDB"];
var writeMode = "RDBMS";


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
        transferTest();
    }
});

function addHospitals(mysqlConn, mongoConn, callback) {
    var hospital;
    
    while (writeMode != "Both") {
        hospital = {
            _id : ++hId,
            name : "Test Hospital " + hId,
            city : "Somewhere",
            state : "XX",
            beds : 999,
            traumaCenter : true
        };
        console.log("[transferTest.js addHospitals] Adding hospital: ", hospital._id);
        setTimeout(function() {
            dal.createHospital(writeMode, mysqlConn, mongoConn, hospital, function(err, result) {
                if (err) return callback(err, null);
            });
        }, 5000);
    }

    callback(null, true);
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
                physicians.push(rows.physician);
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

function transferTest() {
    mongoConn.dropDatabase();
    
    writeQ.initializeQueue(mongoConn, function (err, result) {
        console.log("[Transfer] Starting the transfer of hospitals...");
        writeMode = "Transfer";
        // start transfer of hospital data
        transferHospitals(mysqlConn, mongoConn, function(err, result) {
            console.log("[Drain] Transfer complete. Draining queue...");
            writeMode = "Drain";

            writeQ.processQueue(mongoConn, function (err, result) {
                console.log("[Both] Queue drained. Writing to both DBs...");
                writeMode = "Both";
                
            });
        });
        
        // add some more hospitals
        addHospitals(mysqlConn, mongoConn, function(err, result) {
            console.log("All hospitals added");
            // if we get a write queue closed error, simply retry. We
            // are switching from drain to both modes
            
        });

        
    });
    
}
