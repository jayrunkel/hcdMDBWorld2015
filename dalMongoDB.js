
var self = {

    hosColName : "hospitals",
    phyColName : "physicians",
    
    createHospital : function(dbConnection, hospital, callback) {
        var hosCol = dbConnection.collection(self.hosColName);

        hosCol.insert(hospital, function(err, result) {
            if (err) {
                console.log("[dalMongoDB.createHospital] Could not insert hospital ", err);
                callback(err);
            }
            else {
                console.log("[dalMongoDB.createHospital] Hospital loaded");
                callback(null, result);
            }
        });
    },

    // // Creates hospital object with related physicians. Should be only
    // // used during an initial load as it does not create the
    // // corresponding hospital entries on the physician objects.
    // createHospitalObj : function(dbConnection, hospital, physicians, callback) {
    //     self.createHospital(dbConnection, hospital, function(err, result) {
    //         if (err) 
    //             callback(err);
    //         else {
    //             // var hosCol = dbConnection.collection(self.hosColName);
    //             // hosCol.update({_id : hospital._id}, {$set : {physicians : physicians}}, function (err, result) {
    //             //     if (err) {
    //             //         console.log("Could not add related physicians to newly created hospital ", hospital.id, err);
    //             //         callback(err);
    //             //     }
    //             //     else {
    //             //         console.log("Hospital object created");
    //             //         callback(null, result);
    //             //     }
    //             callback(null, result);
    //         }
    //     });
    // },

    deleteHospital : function(dbConnection, hosId, callback) {
        var hosCol = dbConnection.collection(self.hosColName);

        hosCol.findOne({"_id" : hosId}, function (err, hospital) {
            if (err) return callback(err);

            var error = null;
            var physicianIds = hospital.physicians;
            var physLeft = physicianIds.length;

            var deletePhyRefCB = function (physId) {
                return function (err, data) {
                    // an error was previously encountered and the callback was invoked
                    if (error != null) { return; }

                    // an error happen while trying to delete all references to the hospital on the physicians
                    if (err) {
                        error = err;
                        console.log('Error while removing hospital ', hosId, ' reference from physician ', physId, err);
                        return callback(err);
                    }

                    // after last reference to the hospital is removed, delete the hospital
                    if (!--physLeft) {
                        hosCol.remove({"_id" : hosId}, function (err, delHospital) {
                            if (err) {
                                console.log('Error while removing hospital ', hosId, err);
                                return callback(err);
                            }
                            else {
                                console.log("Hospital ", hosId, " deleted.");
                                callback(null, delHospital);
                            }
                        });
                    }
                }
            };
            
            if (physicianIds.length > 0) {
                for (var i = 0; i < physicianIds.length; i++) {
                    var phyId = physicianIds[i];
                    console.log("Removing physician: ", phyId);
                    self.delPhyHospital(dbConnection, phyId, hosId, deletePhyRefCB(phyId));
                }
            }
            else {
                physLeft = 1;
                deletePhyRefCB(-9999)(null, null);
            }
        });
            
    },

    deletePhysician : function(dbConnection, phyId, callback) {
        var phyCol = dbConnection.collection(self.phyColName);

        phyCol.findOne({"_id" : phyId}, function (err, physician) {
            if (err) return callback(err);

            var error = null;
            var hospitalIds = physician.hospitals;
            var hosLeft = hospitalIds.length;

            var deleteHosRefCB = function (hosId) {
                return function (err, data) {
                    // an error was previously encountered and the callback was invoked
                    if (error != null) { return; }

                    // an error happen while trying to delete all references to the physician on the hospital
                    if (err) {
                        error = err;
                        console.log('Error while removing physician ', phyId, ' reference from hospital ', hosId, err);
                        return callback(err);
                    }

                    // after last reference to the hospital is removed, delete the hospital
                    if (!--hosLeft) {
                        phyCol.remove({"_id" : phyId}, function (err, delPhysician) {
                            if (err) {
                                console.log('Error while removing physician ', phyId, err);
                                return callback(err);
                            }
                            else {
                                console.log("Physician ", phyId, " deleted.");
                                callback(null, delPhysician);
                            }
                        });
                    }
                }
            };
            if (hospitalIds.length > 0) {
                for (var i = 0; i < hospitalIds.length; i++) {
                    var hospId = hospitalIds[i];
                    console.log("Removing hospital: ", hospId);
                    self.delPhyHospital(dbConnection, phyId, hospId, deleteHosRefCB(hospId));
                }
            }
            else {
                hosLeft = 1;
                deleteHosRefCB(-9999)(null, null);
            }
        });
            
    },


    createPhysician : function(dbConnection, physician, callback) {
        var phyCol = dbConnection.collection(self.phyColName);

        phyCol.insert(physician, function(err, result) {
            if (err) {
                console.log("Could not insert physician ", err);
                callback(err);
            }
            else {
                console.log("Physician loaded");
                callback(null, result);
            }
        });
    },

    addPhyHospital : function(dbConnection, phyId, hosId, callback) {
        var hosCol = dbConnection.collection(self.hosColName);
        var phyCol = dbConnection.collection(self.phyColName);
        
        hosCol.update({"_id" : hosId}, {$addToSet : {"physicians" : phyId}}, function (err1, result1) {
            if (err1) {
                console.log("Could not add physician ", phyId, " to hospital ", hosId, err1);
                callback(err1);
            }
            else {
                phyCol.update({"_id" : phyId}, {$addToSet : {"hospitals" : hosId}}, function (err2, result2) {
                    if (err2) {
                        console.log("Could not add hospital ", hosId, " to physician ", phyId, err2);
                        callback(err2);
                    }
                    else {
                        console.log("Physician ", phyId, " added to hospital ", hosId);
                        callback(null, [result1, result2]);
                    }
                });
            }
        });
    },

    delPhyHospital : function(dbConnection, phyId, hosId, callback) {
        var hosCol = dbConnection.collection(self.hosColName);
        var phyCol = dbConnection.collection(self.phyColName);
        
        hosCol.update({"_id" : hosId}, {$pull : {"physicians" : phyId}}, function (err1, result1) {
            if (err1) {
                console.log("Could not remove physician ", phyId, " from hospital ", hosId, err1);
                callback(err1);
            }
            else {
                phyCol.update({"_id" : phyId}, {$pull : {"hospitals" : hosId}}, function (err2, result2) {
                    if (err2) {
                        console.log("Could not remove hospital ", hosId, " from physician ", phyId, err2);
                        callback(err2);
                    }
                    else {
                        console.log("Physician ", phyId, " removed from hospital ", hosId);
                        callback(null, [result1, result2]);
                    }
                });
            }
        });
    }
            
};

module.exports = self;
