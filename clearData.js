

var mysql      = require('mysql');
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'hcd'
});

var tables = ['hosPhysiciansRel', 'hospitals', 'patients', 'patProcedureRel', 'phyPatientRel', 'phyProcedureRel', 'physicians', 'procedures', 'procRecordRel', 'records'];

function truncateTables(tableNames, callback) {
    var tablesLeft = tableNames.length;
    var error = null;
    var contents = [];

    connection.connect();
    
    var truncateTable = function(tableName) {
        return function(err, data) {
            // an error was previously encountered and the callback was invoked
            if (error !== null) { return; }

            // an error happen while trying to read the file, so invoke the callback
            if (err) {
                error = err;
                return callback(err);
            }

            contents.push(tableName);
            
            // after the last file read was executed, invoke the callback
            if (!--tablesLeft) {
                callback(null, contents);
            }
        }
    };

    tableNames.forEach(function(tName) {
        connection.query('TRUNCATE ' + tName, truncateTable(tName));
    });
}

truncateTables(tables, function (err, list) {
    if (err) console.log("Error truncating tables: ", err);
    console.log("Tables truncated: ", list);
    process.exit();
});

//process.exit()

// for (var i = 0; i < tables.length; i++) {

//     connection.query('TRUNCATE ' + tables[i], function (err, results) {
//         if (err)
//             console.log(err);
//         else
//             console.log("Truncated");
//     });
// }
//;

//process.exit();
