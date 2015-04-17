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
    writeMode : 0,
    queueStatus : "open",                       // "open" or "closed"
    queueCol : "writeQueue",
    statusCol : "queueStatus",

    initializeQueue : function(dbConn, callback) {
        var wQCol = dbConn.collection(self.queueCol);
        
        // add code to drop queue collection
        Step(
            function dropWriteQueue() {
                wQCol.drop(this);
            },
            function setQueueOpen(err) {
                if (err) callback(err);
                return self.setQueueStatus(dbConn, "open", callback);
                });
            }
        )
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
                console.log("Write queue element loaded");
                callback(null, result);
            }
        });
        
    },

    processWriteObject : function (dbConn, wObject, callback) {

        switch (wObject.op) {
        case "createHospital" : 
            mongoDAL.createHospital(dbConn, wObject.args[0], callback);
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

    processQueue : function (dbConn, callback) {
        console.log("Entering processQueue");
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
            console.log("Reached the end of the queue");
            wQCol.count({w: false}, function (err, count) {
                assert.ifError(err);
                // check to see if the queue is empty
                console.log("Queue length is: ", count);
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
