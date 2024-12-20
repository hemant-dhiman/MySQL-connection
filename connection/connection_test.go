package connection

import (
	"fmt"
	"github.com/hemant-dhiman/MySQL-connection/constants"
	"gorm.io/gorm"
	"log"
	"os"
	"testing"
	"time"
)

func initMySql() (*gorm.DB, error) {
	con := GetMySqlConnection()
	getenv := os.Getenv(constants.ENV_PANEL_MYSQL_CONNECTION_STRING)
	fmt.Printf("<%v>", getenv)
	mySqlConfig := DBConfig{
		DataSourceName: getenv,
		MaxOpen:        12,
		MaxIdle:        10,
		Lifetime:       5 * time.Minute,
		IdleTime:       1 * time.Minute,
	}
	err := con.InitDataSourceConnection("mysql", mySqlConfig)
	if err != nil {
		fmt.Printf("Failed during duplicate initialization: %v", err)
	}

	return con.GetDB("mysql")
}

func TestCount(t *testing.T) {

	db, err := initMySql()
	if err != nil {
		fmt.Printf("Error retrieving database connection: %v\n", err)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM izooto.audience").Scan(&count).Error; err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	log.Printf("audience count: %d", count)
}

func TestDBFactory(t *testing.T) {
	// Get the connection factory instance
	dbFactory := GetMySqlConnection()

	testConfig := DBConfig{
		DataSourceName: os.Getenv(constants.ENV_PANEL_MYSQL_CONNECTION_STRING),
		MaxOpen:        5,
		MaxIdle:        2,
		Lifetime:       1 * time.Minute,
		IdleTime:       30 * time.Minute,
	}

	// Test: Initialize the database connection
	t.Run("InitializeDB", func(t *testing.T) {
		err := dbFactory.InitDataSourceConnection("test_db", testConfig)

		if err != nil {
			t.Fatalf("Failed during duplicate initialization: %v", err)
		}

		// Retrieve the connection
		db, err := dbFactory.GetDB("test_db")
		if err != nil {
			t.Fatalf("Failed to retrieve initialized database: %v", err)
		}
		if db == nil {
			t.Fatal("Expected a valid database connection, got nil")
		}

		// Ensure the connection is valid
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatalf("Failed to get underlying SQL DB: %v", err)
		}
		if err := sqlDB.Ping(); err != nil {
			t.Fatalf("Failed to ping the database: %v", err)
		}
	})

	// Test: Duplicate initialization should not create a new connection
	t.Run("DuplicateInitialization", func(t *testing.T) {
		err := dbFactory.InitDataSourceConnection("test_db", testConfig)
		if err != nil {
			t.Fatalf("Failed during duplicate initialization: %v", err)
		}

		dbFactory.PrintAllExistingDb()

		db, err := dbFactory.GetDB("test_db")
		if err != nil {
			t.Fatalf("Failed to retrieve database after duplicate initialization: %v", err)
		}
		if db == nil {
			t.Fatal("Expected a valid database connection after duplicate initialization, got nil")
		}
	})

	// Test: Retrieve non-existent database
	t.Run("GetNonExistentDB", func(t *testing.T) {
		db, err := dbFactory.GetDB("non_existent_db")
		if err == nil {
			t.Fatalf("Expected an error when retrieving a non-existent database, got db: %v", db)
		}
		if db != nil {
			t.Fatalf("Expected nil database, got: %v", db)
		}
	})

	// Test: Close all connections
	t.Run("CloseAllConnections", func(t *testing.T) {
		dbFactory.CloseAllConnections()

		// Verify that the connection is closed
		db, err := dbFactory.GetDB("test_db")
		if err == nil {
			t.Fatal("Expected an error when retrieving a closed database connection, but got nil")
		}

		if db != nil {
			sqlDB, err := db.DB()
			if err == nil && sqlDB != nil {
				if err := sqlDB.Ping(); err == nil {
					t.Fatal("Expected the database connection to be closed, but ping succeeded")
				}
			}
		}
	})
}

func TestReconnect(t *testing.T) {
	dbFactory := GetMySqlConnection()

	testConfig := DBConfig{
		DataSourceName: os.Getenv(constants.ENV_PANEL_MYSQL_CONNECTION_STRING),
		MaxOpen:        5,
		MaxIdle:        2,
		Lifetime:       1 * time.Minute,
		IdleTime:       30 * time.Minute,
	}

	err := dbFactory.InitDataSourceConnection("reconnect_test_db", testConfig)
	if err != nil {
		t.Fatalf("Failed to initialize database connection: %v", err)
	}

	// Retrieve the connection
	db, err := dbFactory.GetDB("reconnect_test_db")
	if err != nil {
		t.Fatalf("Failed to retrieve initialized database: %v", err)
	}

	// Simulate a broken connection
	sqlDB, _ := db.DB()
	_ = sqlDB.Close() // Forcefully close the connection

	// Attempt to get the database, which should trigger a reconnect
	db, err = dbFactory.GetDB("reconnect_test_db")
	if err != nil {
		t.Fatalf("Failed to reconnect to database: %v", err)
	}

	// Verify the new connection is healthy
	sqlDB, _ = db.DB()
	if err := sqlDB.Ping(); err != nil {
		t.Fatalf("Ping failed after reconnect: %v", err)
	}

	// try to get same connection again
	db2, err := dbFactory.GetDB("reconnect_test_db")
	if err != nil {
		t.Fatalf("Failed to reconnect to database: %v", err)
	}
	sqlDB2, _ := db2.DB()
	if err := sqlDB2.Ping(); err != nil {
		t.Fatalf("Ping failed after reconnect: %v", err)
	}

	dbFactory.PrintAllExistingDb()
	dbFactory.CloseAllConnections()
}
