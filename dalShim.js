
// MIGRATION PROCESS
//
// Create a migration queue
// - Data is read from RDBMS, grouped into denormalized objects, and
//   then written to the queue.
// - Objects are then pulled from the queue and inserted into MongoDB
// - Updates that are made during the migration processing are added
//   to the back of the queue
//   + As queue is being loaded need to have special processing that
//     puts updates at the back of the queue
// - All reads occur against the RDBMS until the queue is empty. Once
//   queue is empty, reads and writes go to MongoDB (bypassing queue
//   and RDBMS)
//
// DAL Shim has ? modes:
// 1. RDBMS
//    - Reads and Writes to RDBMS
// 2. Transfer 
//    - Application Reads to RDBMS
//    - ETL Migration writes to MongoDB
//    - Application writes:
//      + to RDBMS
//      + to MongoDB Collection Queue collection - Imdepotent
// 3. Queue drain
//    - Application Reads to RDBMS
//    - Application writes:
//      + to RDBMS
//      + to MongoDB Collection Queue collection - Imdepotent
//    - Queue drained and written to main collection
// 4. MongoDB (When queue length == 0)
//    - Application reads and writes to MongoDB

var mysqlDAL = require("./dalMySQL.js");
var mongoDAL = require("./dalMongoDB.js");
var writeQ = require("./writeQueue.js");

//var writeModes = ["RDBMS", "Transfer", "Drain", "MongoDB"];
//var writeMode = "RDBMS";                 


module.exports = {

    createHospital : function(writeMode, mysqlConn, mongoConn, hospital, callback) {
        console.log("[dalShim.createHospital] Start");
        
        switch (writeMode) {
        case "RDBMS" :
            mysqlDAL.createHospital(mysqlConn, hospital, callback);
            break;
        case "Transfer" :
            mysqlDAL.createHospital(mysqlConn, hospital, callback);
            writeQ.createHospital(mongoConn, hospital, callback);
            break;
        case "Drain" :
            writeQ.createHospital(mongoConn, hospital, function (err, result) {
                if (err) return callback(err, null);
                // it is possible that the write queue was just closed as we were transitioning from
                // drain to both mode
                
                mysqlDAL.createHospital(mysqlConn, hospital, callback);
            });
            break;
        case "Both" :
            mysqlDAL.createHospital(mysqlConn, hospital, callback);
            mongoDAL.createHospital(mongoConn, hospital, callback);
            break;            
        case "MongoDB" :
            mongoDAL.createHospital(mongoConn, hospital, callback);
            break;
        default:
            console.log("Unknown write mode: ", writeMode);
            callback({name: "Unknown write mode", message: "Unknown write mode: " + writeMode}, null);
        }
    },


    createPhysician : function(dbConnection, physician, callback) {
        dbConnection.query('INSERT INTO physicians SET ?', physician, function (err, data) {
            if (err) {
                console.log("Could not insert physician ", err);
                callback(err);
            }
            else {
                console.log("Physician loaded");
                callback(null, true);
            }
        });
    },

    addPhyHospital : function(dbConnection, phyId, hosId, callback) {

        var rel = {};
        rel.hospital = hosId;
        rel.physician = phyId;
        
        dbConnection.query('INSERT INTO hosPhysiciansRel SET ?', rel, function (err, data) {
            if (err) {
                console.log("Could not add physician ", phyId, " to hospital ", hosId, err);
                callback(err);
            }
            else {
                console.log("Physician ", phyId, " added to hospital ", hosId);
                callback(null, true);
            }
        });
    }
};
