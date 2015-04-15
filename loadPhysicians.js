var mysql      = require('mysql');
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'hcd'
});


var MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    stream = require('stream');

connection.connect();

function loadTables() {
    MongoClient.connect('mongodb://localhost:27017/hcd', function(err, db) {
        if (err)
            console.log('Could not connect to MongoDB');
        else
            console.log('Connected to MongoDB');
    
        loadHospitals(db, function(error, data) {console.log("Hospital and hospital physician references loaded");});
        loadPhysicians(db, function(error, data) {console.log("Physicians loaded");});
        loadPatients(db,  function(error, data) {console.log("Patients loaded");});
        loadProcedures(db,  function(error, data) {console.log("Procedures loaded");});
        loadRecords(db,  function(error, data) {console.log("Records loaded");});
    });
}

function loadPhysicians(db, callback) {

    var error = null;
    
    var physicians = db.collection('physicians');
    console.log("Reading Physician Stream");
    var phyStream = physicians.find().stream();

    phyStream.on("data", function(phy) {

        delete phy.hospitals;
        for (var addrField in phy.addr) {
            phy["addr_" + addrField] = phy.addr[addrField];
        }
        delete phy.addr;
        
        connection.query('INSERT INTO physicians SET ?', phy, function (err, data) {
            if (err) {
                console.log("Error inserting physician: ", phy._id, " - ", err);
                return callback(err);
            }
        });
    });

    phyStream.on("end", function(err, data) {
        console.log("Hospital Processing Complete.");
        callback(null, "done");
    });

};


function loadHospitals(db, callback) {

    var error = null;
    
    var hospitals = db.collection('hospitals');
    console.log("Reading Hospital Stream");
    var hosStream = hospitals.find().stream();

    hosStream.on("data", function(hos) {
        var physicians = hos.physicians;
        var refsLeft = physicians.length;
        var ref = {};
        ref.hospital = hos._id;
        var error = null;

        var loadHospitalRefs = function (physician) {
            return function(err, data) {
                // an error was previously encountered and the callback was invoked
                if (error !== null) { return; }

                // an error happen while trying to insert the reference, so invoke the callback
                if (err) {
                    error = err;
                    console.log('Error while inserting hospital physician reference[', ref.hospital, ', ', physician , ' ]: ', err);
                    return callback(err);
                }

                // after the last reference was inserted, invoke the callback
                //if (!--refsLeft) {
                //    callback(null, "done");
            }
        };
        
        hos.traumaCenter = hos["trauma center"];
        delete hos["trauma center"];
        delete hos.physicians;

        connection.query('INSERT INTO hospitals SET ?', hos, function (err, data) {
            for (var i = 0; i < physicians.length; i++) {
                ref.physician = physicians[i];
                
                connection.query('INSERT INTO hosPhysiciansRel SET ?', ref, loadHospitalRefs(physicians[i]));  
            };
        });
        
    });

    hosStream.on("end", function(err, data) {
        console.log("Hospital Processing Complete.");
        callback(null, "done");
    });

};

function loadPatients(db, callback) {

    var error = null;
    
    var patients = db.collection('patients');
    console.log("Reading Patient Stream");
    var patStream = patients.find().stream();

    patStream.on("data", function(pat) {
        var procedures = pat.procedures;
        var refsLeft = procedures.length;
        var ref = {};
        ref.patient = pat._id;
        var error = null;

        var loadPatProcedureRefs = function (procedure) {
            return function(err, data) {
                // an error was previously encountered and the callback was invoked
                if (error !== null) { return; }

                // an error happen while trying to insert the reference, so invoke the callback
                if (err) {
                    error = err;
                    console.log('Error while inserting patient procedure reference[', ref.patient, ', ', procedure , ' ]: ', err);
                    return callback(err);
                }

                // after the last reference was inserted, invoke the callback
                //if (!--refsLeft) {
                //    callback(null, "done");
            }
        };
        

        delete pat.physicians;
        delete pat.procedures;
        for (var addrField in pat.addr) {
            pat["addr_" + addrField] = pat.addr[addrField];
        }
        delete pat.addr;

        connection.query('INSERT INTO patients SET ?', pat, function (err, data) {
            for (var i = 0; i < procedures.length; i++) {
                ref.procedure = procedures[i].valueOf();
                
                connection.query('INSERT INTO patProcedureRel SET ?', ref, loadPatProcedureRefs(ref.procedure));  
            };
        });
        
    });

    patStream.on("end", function(err, data) {
        console.log("Patient Processing Complete.");
        callback(null, "done");
    });

};

function loadProcedures(db, callback) {

    var error = null;
    
    var procedures = db.collection('procedures');
    console.log("Reading Procedure Stream");
    var procStream = procedures.find().stream();

    procStream.on("data", function(proc) {
        var records = proc.records;
        var refsLeft = records.length;
        var ref = {};
        ref.procedure = proc._id;
        var error = null;

        var loadProcRecordRefs = function (record) {
            return function(err, data) {
                // an error was previously encountered and the callback was invoked
                if (error !== null) { return; }

                // an error happen while trying to insert the reference, so invoke the callback
                if (err) {
                    error = err;
                    console.log('Error while inserting procedure record reference[', ref.procedure, ', ', record , ' ]: ', err);
                    return callback(err);
                }

                // after the last reference was inserted, invoke the callback
                //if (!--refsLeft) {
                //    callback(null, "done");
            }
        };
        

        delete proc.records;
        proc._id = proc._id.valueOf();
        proc.date = proc.date.toISOString().substring(0,10);
        
        connection.query('INSERT INTO procedures SET ?', proc, function (err, data) {
            for (var i = 0; i < records.length; i++) {
                ref.record = records[i].valueOf();
                
                connection.query('INSERT INTO procRecordRel SET ?', ref, loadProcRecordRefs(ref.record));  
            };
        });
        
    });

    procStream.on("end", function(err, data) {
        console.log("Procedure Processing Complete.");
        callback(null, "done");
    });

};


function loadRecords(db, callback) {

    var records = db.collection('records');
    console.log("Reading Record Stream");
    var recStream = records.find().stream();

    recStream.on("data", function(rec) {

        rec._id = rec._id.valueOf();
        rec.procedure = rec.procedure.valueOf();
        
        connection.query('INSERT INTO records SET ?', rec, function (err, data) {
            if (err) {
                console.log("Error inserting record: ", rec._id, " - ", err);
                return callback(err);
            }
        });
    });

    recStream.on("end", function(err, data) {
        console.log("Record Processing Complete.");
        callback(null, "done");
    });

};


//connection.end();
loadTables();
