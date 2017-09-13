// Copyright 2017 Apcera Inc. All rights reserved.

package test

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

// Driver names.
const (
	DriverMySQL    = "mysql"
	DriverPostgres = "postgres"
)

// InitSQLDatabase initializes a SQL Database for NATS Streaming testing.
func InitSQLDatabase(driver, sourceAdmin, source, dbName string) error {
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
		if len(line) == 0 || line[0] == '#' {
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
	MustExecuteSQL(t, db, "DELETE FROM ServerInfo")
	MustExecuteSQL(t, db, "DELETE FROM Clients")
	MustExecuteSQL(t, db, "DELETE FROM Channels")
	MustExecuteSQL(t, db, "DELETE FROM Messages")
	MustExecuteSQL(t, db, "DELETE FROM Subscriptions")
	MustExecuteSQL(t, db, "DELETE FROM SubsPending")
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
