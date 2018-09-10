var migrations_table = require('./config')['table'];

function run_query(conn, query, cb) {
  conn.getConnection(function (err, connection) {
    if (err) {
      throw err;
    }

    connection.query(query, function (error, results) {
      connection.release();
      if (error) {
        throw error;
      }
      cb(results);
    });
  });
}

function execute_query(conn, path, final_file_paths, type, cb) {
  if (final_file_paths.length) {
    var file_name = final_file_paths.shift()['file_path'];
    var current_file_path = path + '/' + file_name;

    var timestamp_val = file_name.split('_', 1)[0];
    var migration_name_val = file_name.replace('.js', '');
    var queries = require(current_file_path);
    if (typeof (queries[type]) == 'string') {
      console.info('Migrating: ' + migration_name_val);
      run_query(conn, queries[type], function () {
        updateRecords(conn, type, migrations_table, timestamp_val, migration_name_val, function () {
          execute_query(conn, path, final_file_paths, type, cb);
        });
      });
    } else if (typeof (queries[type]) == 'function') {
      console.info('Migrating: ' + migration_name_val);
      queries[type](conn, function () {
        updateRecords(conn, type, migrations_table, timestamp_val, migration_name_val, function () {
          execute_query(conn, path, final_file_paths, type, cb);
        });
      });
    }
  } else {
    cb();
  }
}

function updateRecords(conn, type, migrations_table, timestamp_val, migration_name_val, cb) {
  var query = '';
  if (type == 'up') {
    query = "INSERT INTO " + migrations_table + " (`timestamp`) VALUES ('" + timestamp_val + "')";
  } else if (type == 'down') {
    query = "DELETE FROM " + migrations_table + " WHERE `timestamp` = '" + timestamp_val + "'"
  }
  run_query(conn, query, function () {
    console.info('Migrated: ' + migration_name_val);
    cb();
  });
}

function createMigrationsTable(conn, cb) {
  run_query(conn, "CREATE TABLE IF NOT EXISTS `" + migrations_table + "` (`timestamp` varchar(254) NOT NULL UNIQUE)", function () {
    console.info('Migration table created successfully.');
    cb();
  });
}

function dropTables(conn, cb) {
  var drop_tables_query = 
    "START TRANSACTION;" +
    "SET FOREIGN_KEY_CHECKS = 0;"+
    "SET GROUP_CONCAT_MAX_LEN=32768;" +
    "SET @tables = NULL;" +
    "SELECT GROUP_CONCAT('`', table_name, '`') INTO @tables " +
    "FROM information_schema.tables "+
    "WHERE table_schema = (SELECT DATABASE());"+
    "SELECT IFNULL(@tables,'dummy') INTO @tables;"+
    "SET @tables = CONCAT('DROP TABLE IF EXISTS ', @tables);" +
    "PREPARE stmt FROM @tables;" +
    "EXECUTE stmt;" +
    "DEALLOCATE PREPARE stmt;" +
    "SET FOREIGN_KEY_CHECKS = 1;" +
    "COMMIT;";
  run_query(conn, drop_tables_query, function (result) {
    console.info('Dropped all tables successfully.');
    cb();
  });
}

module.exports = {
  run_query: run_query,
  execute_query: execute_query,
  updateRecords: updateRecords,
  dropTables: dropTables,
  createMigrationsTable: createMigrationsTable 
};
