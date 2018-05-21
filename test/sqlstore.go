// Copyright 2017-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
)

// Driver names.
const (
	DriverMySQL    = "mysql"
	DriverPostgres = "postgres"
)

// CreateSQLDatabase initializes a SQL Database for NATS Streaming testing.
func CreateSQLDatabase(driver, sourceAdmin, source, dbName string) error {
	db, err := sql.Open(driver, sourceAdmin)
	if err != nil {
		return fmt.Errorf("Error opening connection to SQL datastore %q: %v", sourceAdmin, err)
	}
	defer db.Close()
	if _, err := db.Exec("DROP DATABASE IF EXISTS " + dbName); err != nil {
		return fmt.Errorf("Error dropping database: %v", err)
	}
	switch driver {
	case DriverMySQL:
		if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
			return fmt.Errorf("Error creating database: %v", err)
		}
		if _, err = db.Exec("USE " + dbName); err != nil {
			return fmt.Errorf("Error using database %q: %v", dbName, err)
		}
	case DriverPostgres:
		if _, err := db.Exec("CREATE DATABASE " + dbName); err != nil {
			return fmt.Errorf("Error creating database: %v", err)
		}
		db.Close()
		db, err = sql.Open(driver, source)
		if err != nil {
			return fmt.Errorf("Error connecting to database: %v", err)
		}
		defer db.Close()
	default:
		panic(fmt.Sprintf("Unsupported driver %v", driver))
	}
	sqlCreateDatabase, err := loadCreateDatabaseStmts(driver)
	if err != nil {
		return err
	}
	for _, stmt := range sqlCreateDatabase {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("Error executing statement (%s): %v", stmt, err)
		}
	}
	return nil
}

func loadCreateDatabaseStmts(driver string) ([]string, error) {
	fileName := "../" + driver + ".db.sql"
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("Error opening file: %v", err)
	}
	defer file.Close()
	stmts := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimLeft(line, " ")
		if len(line) == 0 || line[0] == '#' || line[0] == '-' {
			continue
		}
		stmts = append(stmts, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("Error scanning file: %v", err)
	}
	return stmts, nil
}

// CleanupSQLDatastore empties the tables from the NATS Streaming database.
func CleanupSQLDatastore(t TLogger, driver, source string) {
	db, err := sql.Open(driver, source)
	if err != nil {
		StackFatalf(t, "Error cleaning up SQL datastore", err)
	}
	defer db.Close()
	MustExecuteSQL(t, db, "DELETE FROM StoreLock")
	MustExecuteSQL(t, db, "DELETE FROM ServerInfo")
	MustExecuteSQL(t, db, "DELETE FROM Clients")
	MustExecuteSQL(t, db, "DELETE FROM Channels")
	MustExecuteSQL(t, db, "DELETE FROM Messages")
	MustExecuteSQL(t, db, "DELETE FROM Subscriptions")
	MustExecuteSQL(t, db, "DELETE FROM SubsPending")
}

// DeleteSQLDatabase drops the given database.
func DeleteSQLDatabase(driver, sourceAdmin, dbName string) error {
	db, err := sql.Open(driver, sourceAdmin)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("DROP DATABASE " + dbName)
	return err
}

// MustExecuteSQL excutes the given SQL query and is not expecting an error.
// If it does, it calls t.Fatalf().
func MustExecuteSQL(t TLogger, db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		StackFatalf(t, "Error executing query %q: %v", query, err)
	}
	return r
}

// AddSQLFlags adds some SQL options to the given flag set.
func AddSQLFlags(fs *flag.FlagSet, driver, source, sourceAdmin, dbName *string) {
	flag.StringVar(driver, "sql_driver", *driver, "SQL Driver to use")
	flag.StringVar(source, "sql_source", "", "SQL data source")
	flag.StringVar(sourceAdmin, "sql_source_admin", "", "SQL data source to create the database")
	flag.StringVar(dbName, "sql_db_name", *dbName, "SQL database name")
}

// ProcessSQLFlags allows to just specify the driver on the command line and
// use corresponding driver defaults, while still allowing full customization
// of each param.
func ProcessSQLFlags(fs *flag.FlagSet, defaults map[string][]string) error {
	driver := fs.Lookup("sql_driver").Value.String()
	switch driver {
	case DriverMySQL, DriverPostgres:
	default:
		return fmt.Errorf("Unsupported SQL driver %q", driver)
	}
	defaultsSources := defaults[driver]
	source := fs.Lookup("sql_source")
	if source.Value.String() == "" {
		source.Value.Set(defaultsSources[0])
	}
	sourceAdmin := fs.Lookup("sql_source_admin")
	if sourceAdmin.Value.String() == "" {
		sourceAdmin.Value.Set(defaultsSources[1])
	}
	return nil
}
