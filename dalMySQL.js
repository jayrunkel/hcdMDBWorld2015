
var step = require('step');

module.exports = {

    createHospital : function(dbConnection, hospital, callback) {
        dbConnection.query('INSERT INTO hospitals SET ?', hospital, function (err, data) {
            if (err) {
                console.log("Could not insert hospital ", err);
                callback(err);
            }
            else {
//                console.log("Hospital loaded");
                callback(null, true);
            }
        });
    },

    deleteHospital : function(dbConnection, hosId, callback) {

        step (
            function delRows () {
                dbConnection.query('DELETE FROM hosPhysiciansRel WHERE hospital = ?', hosId, this.parallel());
                dbConnection.query('DELETE FROM hospitals WHERE _id = ?', hosId, this.parallel());
            },
            function handleErrors (err, rels, hos) {
                if (err) {
                    console.log("Could not delete hospital ", hosId, err);
                }
                else {
                    console.log("Hospital ", hosId, " deleted");
                }
                callback(err, hosId);
            }
        );
    },

    createPhysician : function(dbConnection, physician, callback) {
        dbConnection.query('INSERT INTO physicians SET ?', physician, function (err, data) {
            if (err) {
                console.log("Could not insert physician ", err);
                callback(err);
            }
            else {
//                console.log("Physician loaded");
                callback(null, true);
            }
        });
    },

    deletePhysician : function(dbConnection, phyId, callback) {

        dbConnection.query('DELETE FROM hosPhysiciansRel WHERE physician = ?', phyId, function (err, data) {
            if (err) {
                console.log("Could not delete hospital physicians relations", err);
                callback(err);
            }
            else {
                console.log('Hospital Physician relationships for physician ', phyId, ' are deleted.');
                dbConnection.query('DELETE FROM physicians WHERE _id = ?', phyId, function (err, data) {
                    if (err) {
                        console.log("Could not delete physician ", phyId, err);
                        callback(err);
                    }
                    else {
                        console.log("Physician ", phyId, " deleted");
                        callback(null, true);
                    }
                });
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
    },

    delPhyHospital : function(dbConnection, phyId, hosId, callback) {
        console.log("delPhyHospital");
        dbConnection.query('DELETE FROM hosPhysiciansRel WHERE hospital = ? AND physician = ?', [hosId, phyId], function (err, data) {
            
            if (err) {
                console.log("Could not remove physician ", phyId, " from hospital ", hosId, err);
                callback(err);
            }
            else {
                console.log("Physician ", phyId, " removed from hospital ", hosId);
                callback(null, true);
            }
        });
        
    }
};
