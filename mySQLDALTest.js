
var mySQLDAL = require("./dalMySQL.js");
var mysql = require('mysql');

var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'hcd'
});

connection.connect();

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

mySQLDAL.createHospital(connection, hospital, function(err, success) {
    console.log("Create hospital callback");
});

mySQLDAL.createPhysician(connection, physician, function(err, success) {
    console.log("Create Physician callback");
});

mySQLDAL.addPhyHospital(connection, physician._id, hospital._id, function(err, success) {
    console.log("Add Phyisican Hospital Link callback");
//    mySQLDAL.delPhyHospital(connection, physician._id, hospital._id, function(err, success) {
//        console.log("Delete Phyisican Hospital Link callback");
    //    });
    mySQLDAL.deletePhysician(connection, physician._id, function(err, success) {
        console.log("Delete physician callback");
        mySQLDAL.deleteHospital(connection, hospital._id, function(err, success) {
            console.log("Delete hospital callback");
        });
    });
});



