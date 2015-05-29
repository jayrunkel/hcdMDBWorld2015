// Sample write queue object structure
//
// {
//     time : ISODate(),
//     op : createHospital,
//     args : [ {...hospital object...}, ...],           
//     w : false                                         // true when object has been written
// }
//
// Use document to track the id of the last element of the queue written

var assert = require('assert');
var mongoDAL = require('./dalMongoDB.js');
var step = require("step");

var self = {
    writeModes : ["RDBMS", "Transfer", "Drain", "Both", "MongoDB"],
    queueStatus : "open",                       // "open" or "closed"
    queueCol : "writeQueue",
    statusCol : "queueStatus",

    initializeQueue : function(dbConn, callback) {
        var wQCol = dbConn.collection(self.queueCol);
        var statCol = dbConn.collection(self.statusCol);

        // add code to drop queue collection
        step(
            function dropWriteQueue() {
                wQCol.drop(this);
            },
           function createStatusCol(err, result) {
                if (err && err.message != 'ns not found') callback(err);
                dbConn.createCollection(self.statusCol, this);
            },
            function setQueueOpen(err, result) {
                if (err) callback(err);
                self.setQueueStatus(dbConn, "open", this);
            },
            function resetWriteMode(err, result) {
                if (err) callback(err);
                statCol.update({_id : 1}, {$set : {writeMode : 0}}, this);
            },
            function done (err, result) {
                callback(err, result);
            }
        )
    },

    getWriteMode : function (dbConn, callback) {
        var statCol = dbConn.collection(self.statusCol);

        statCol.findOne({_id : 1}, function (err, result) {
            if (err)
                callback(err);
            else
                callback(null, self.writeModes[result.writeMode]);
        });
    },

    nextWriteMode : function (dbConn, callback) {
        var statCol = dbConn.collection(self.statusCol);

        statCol.findOneAndUpdate({_id : 1}, { $inc: { writeMode: 1 } }, {returnOriginal : false}, function (err, result) {
            if (err) {
                console.log("[writeQueue.js - nextWriteMode] error: ", err);
                callback(err);
            }
            else
                callback(null, self.writeModes[result.value.writeMode]);
        });
    },

    setQueueStatus : function(dbConn, status, callback) {
        var statusCol = dbConn.collection(self.statusCol);

        
        statusCol.update({_id : 1}, {$set : {qStatus : status}}, {upsert : true}, function (err, result) {
            if (err) {
                console.log("Queue status could not be set to ", qStatus, err);
                callback(err);
            }
            else {
//                console.log("Queue initialized");
                callback(null, result);
            }
        });   
    },

    insertWriteQueue : function (dbConn, wQObject, callback) {
        if (self.queueStatus == "closed")
            return callback({name: "queueClosed", message: "Cannot add object to closed write queue", wObj: wQObject}, null);
        
        var wQCol = dbConn.collection(self.queueCol);

        wQCol.insert(wQObject, function(err, result) {
            if (err) {
                console.log("Could not insert ", wQObject.op, " operation in write queue ", err);
                callback(err);
            }
            else {
                console.log("[writeQueue.js] Write queue element ", wQObject.op , wQObject.args[0]._id, " loaded");
                callback(null, result);
            }
        });
        
    },

    processWriteObject : function (dbConn, wObject, callback) {

        switch (wObject.op) {
        case "createHospital" :
            mongoDAL.updateHospital(dbConn, wObject.args[0], callback);
            break;
        case "createPhysician" :
            mongoDAL.updatePhysician(dbConn, wObject.args[0], callback);
            break;
        default:
            console.log("Unknown operator ", wObject.op);
            callback({name: "Unknown Operator", wObject: wObject, message: "Unknown operator " + wObject.op}, null);
        }
    },

    createHospital : function (dbConn, hospital, callback) {

        var wObject = {
            time : new Date(),
            op : "createHospital",
            args : [ hospital ],
            w : false
        };
        
        self.insertWriteQueue(dbConn, wObject, callback);
    },

    createPhysician : function (dbConn, physician, callback) {

        var wObject = {
            time : new Date(),
            op : "createPhysician",
            args : [ physician ],
            w : false
        };
        
        self.insertWriteQueue(dbConn, wObject, callback);
    },

    processQueue : function (dbConn, callback) {
//        console.log("Entering processQueue");
        var wQCol = dbConn.collection(self.queueCol);

        // check to see if the queue is open
        
        var qStream;

        qStream = wQCol.find({w : false}).sort({time : -1}).stream();

        qStream.on("data", function (wObject) {
            self.processWriteObject(dbConn, wObject, function (err, result) {
                assert.ifError(err);
                wQCol.update({_id : wObject._id}, {$set : {w : true}}, function (err, result) {
                    assert.ifError(err);
                });
            });
        });


        qStream.on("end", function (err, data) {
//            console.log("Reached the end of the queue");
            wQCol.count({w: false}, function (err, count) {
                assert.ifError(err);
                // check to see if the queue is empty
                console.log("[writeQueue.js processQueue] Queue length is: ", count);
                if (count > 0)
                    // if queue is not empty process the queue
                    self.processQueue(dbConn, callback);
                else  // queue is empty so close the queue
                    self.setQueueStatus(dbConn, "closed", function (err, result) {
                        assert.ifError(err);
                        // check to see if something was inserted into the queue while we were closing it
                        wQCol.count({w: false}, function (err, countAC) {
                            assert.ifError(err);
                            // if queue is empty then we are done
                            if (count > 0)
                                self.processQueue(dbConn, callback);
                            else
                                callback(null, true);
                        });
                    });
            });
        });
    }
};

module.exports = self;
