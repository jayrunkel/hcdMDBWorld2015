
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
var step = require('step');
//var writeModes = ["RDBMS", "Transfer", "Drain", "MongoDB"];
//var writeMode = "RDBMS";                 


var self = {

    shimFuncBuilder : function(context) {
        switch (context.writeMode) {
        case "RDBMS" :
            context.mysqlFunc(context.mysqlConn, context.args[0], context.callback);
            break;
        case "Transfer" :
            step (
                function executeQueries () {
                    context.mysqlFunc(context.mysqlConn, context.args[0], this.parallel());
                    context.qFunc(context.mongoConn, context.args[0], this.parallel());
                },
                function callback (err, result1, result2) {
                    context.callback(err, [result1, result2]);
                }
            );
            break;
        case "Drain" :
            context.qFunc(context.mongoConn, context.args[0], function (err, result) {
                if (err) return context.callback(err, null);
                // it is possible that the write queue was just closed as we were transitioning from
                // drain to both mode
                
                context.mysqlFunc(context.mysqlConn, context.args[0], context.callback);
            });
            break;
        case "Both" :
            step (
                function executeQueries () {
                    context.mysqlFunc(context.mysqlConn, context.args[0], this.parallel());
                    context.mongoFunc(context.mongoConn, context.args[0], this.parallel());
                },
                function callback (err, result1, result2) {
                    context.callback(err, [result1, result2]);
                }
            );
            break;            
        case "MongoDB" :
            context.mongoFunc(context.mongoConn, context.args[0], context.callback);
            break;
        default:
            console.log("Unknown write mode: ", context.writeMode);
            context.callback({name: "Unknown write mode", message: "Unknown write mode: " + context.writeMode}, null);
        }
    },

    createHospital : function(writeMode, mysqlConn, mongoConn, hospital, callback) {

        var context = {
            writeMode : writeMode, 
            mysqlConn : mysqlConn,
            mongoConn : mongoConn,
            callback : callback,
            mysqlFunc : mysqlDAL.createHospital,
            mongoFunc : mongoDAL.createHospital,
            qFunc : writeQ.createHospital,
            args : [ hospital ]
        };

        self.shimFuncBuilder(context);
        
        // switch (writeMode) {
        // case "RDBMS" :
        //     mysqlDAL.createHospital(mysqlConn, hospital, callback);
        //     break;
        // case "Transfer" :
        //     mysqlDAL.createHospital(mysqlConn, hospital, callback);
        //     writeQ.createHospital(mongoConn, hospital, callback);
        //     break;
        // case "Drain" :
        //     writeQ.createHospital(mongoConn, hospital, function (err, result) {
        //         if (err) return callback(err, null);
        //         // it is possible that the write queue was just closed as we were transitioning from
        //         // drain to both mode
                
        //         mysqlDAL.createHospital(mysqlConn, hospital, callback);
        //     });
        //     break;
        // case "Both" :
        //     mysqlDAL.createHospital(mysqlConn, hospital, callback);
        //     mongoDAL.createHospital(mongoConn, hospital, callback);
        //     break;            
        // case "MongoDB" :
        //     mongoDAL.createHospital(mongoConn, hospital, callback);
        //     break;
        // default:
        //     console.log("Unknown write mode: ", writeMode);
        //     callback({name: "Unknown write mode", message: "Unknown write mode: " + writeMode}, null);
        // }
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

module.exports = self;
