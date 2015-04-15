
var mongoDAL = require("./dalMongoDB.js");

var MongoClient = require('mongodb').MongoClient;

var connection = null;

MongoClient.connect('mongodb://localhost:27017/hcd', function(err, db) {
    if (err)
        console.log('Could not connect to MongoDB');
    else {
        console.log('Connected to MongoDB');
        connection = db;
        runTest();
    }
});

var hospital = {
    _id : 12345678,
    name : "Test Hospital 1",
    city : "Somewhere",
    state : "XX",
    beds : 999,
    traumaCenter : true
};

var physician = {
    _id : 87654321,
    first : "Joe",
    last : "Doctor",
    addr_street : "4 Maple Avenue",
    addr_city : "Grover",
    addr_state : "XX",
    addr_zip : "12345"
};

function runTest() {
    mongoDAL.createHospital(connection, hospital, function(err, success) {
        console.log("Create hospital callback");
        mongoDAL.createPhysician(connection, physician, function(err, success) {
            console.log("Create Physician callback");
            mongoDAL.addPhyHospital(connection, physician._id, hospital._id, function(err, success) {
                console.log("Add Phyisican Hospital Link callback");
                mongoDAL.deletePhysician(connection, physician._id, function(err, success) {
                    console.log("Delete physician callback");
                    mongoDAL.deleteHospital(connection, hospital._id, function(err, success) {
                        console.log("Delete hospital callback");
                    });
                });
            });
        });
    });
}


