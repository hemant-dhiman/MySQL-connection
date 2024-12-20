/*
Package connection provides a thread-safe implementation for managing MySQL database connections using GORM.
It includes utilities for initializing, managing, reconnecting, and closing database connections in a singleton
pattern to ensure centralized management of resources.

Features:
  - Singleton pattern to manage MySQL connections.
  - Thread-safe methods for managing multiple database connections.
  - Support for reconnecting to unhealthy database connections.
  - Configurable connection pooling parameters (max open, idle connections, lifetime, idle time).
  - Utilities for closing specific or all database connections and retrieving configuration details.

Types:

 1. DBConfig:
    A struct representing the configuration parameters for database connections.
    Fields:
    - DataSourceName: Connection string for the database.
    - MaxOpen: Maximum number of open connections.
    - MaxIdle: Maximum number of idle connections.
    - Lifetime: Maximum lifetime of a connection.
    - IdleTime: Maximum idle time for a connection.

 2. MySqlConnection:
    A struct managing a map of database connections and their configurations.
    Features:
    - Methods for initializing connections (`InitDataSourceConnection`).
    - Methods for reconnecting unhealthy connections.
    - Thread-safe methods for closing specific or all connections.
    - Utilities for retrieving active database connections and their configurations.

Usage Example:

		func main() {
			// Initialize a singleton MySQL connection factory
			dbFactory := connection.GetMySqlConnection()

			// Define the database configuration
			config := connection.DBConfig{
				DataSourceName: "user:password@tcp(localhost:3306)/dbname",
				MaxOpen:        10,
				MaxIdle:        5,
				Lifetime:       time.Hour,
				IdleTime:       30 * time.Minute,
			}

			// Initialize a database connection
			err := dbFactory.InitDataSourceConnection("primary_db", config)
			if err != nil {
				log.Fatalf("Failed to initialize database: %v", err)
			}

			// Retrieve the connection
			db, err := dbFactory.GetDB("primary_db")
			if err != nil {
				log.Fatalf("Failed to get database connection: %v", err)
			}

			// Perform database operations
			var count int
			if err := db.Raw("SELECT COUNT(*) FROM users").Scan(&count).Error; err != nil {
				log.Fatalf("Query failed: %v", err)
			}
			fmt.Printf("Total users: %d\n", count)

			// Close the connection
			err = dbFactory.CloseConnection("primary_db")
			if err != nil {
				log.Fatalf("Failed to close database connection: %v", err)
			}
		}

	 Author: Hemant Dhiman
	 Since: 18/12/24
	 Version: 1.0.1
*/
package connection

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"sync"
	"time"
)

// DBConfig represents the configuration settings for a database connection.
// It includes parameters for connection pooling and resource management.
type DBConfig struct {
	// DataSourceName specifies the connection string for the database.
	// This typically includes the username, password, database name,
	// and host details in the format: "user:password@tcp(host:port)/dbname".
	DataSourceName string

	// MaxOpen defines the maximum number of open connections allowed in the connection pool.
	// A higher value supports higher concurrency but consumes more resources.
	MaxOpen int

	// MaxIdle defines the maximum number of idle connections maintained in the connection pool.
	// Idle connections are ready for immediate use without creating a new connection.
	MaxIdle int

	// Lifetime specifies the maximum amount of time a connection can remain open before being closed.
	// Use this to prevent stale connections or comply with database server limits.
	Lifetime time.Duration

	// IdleTime specifies the maximum duration an idle connection can remain in the pool
	// before being closed. Helps manage resource usage by closing unused connections.
	IdleTime time.Duration
}

// MySqlConnection is a thread-safe singleton structure for managing multiple
// database connections. It provides functionality to initialize, retrieve,
// and close database connections dynamically.
type MySqlConnection struct {
	// connections stores active database connections, keyed by a unique connection name.
	// Each connection is a pointer to a gorm.DB object, representing the GORM abstraction
	// of a database connection.
	connections map[string]*gorm.DB

	// configs stores the configuration details (DBConfig) for each database connection,
	// keyed by the connection name. This is used to manage reconnections or other operations
	// that require access to the original configuration.
	configs map[string]DBConfig

	// mutex ensures thread-safe access to the connections and configs maps,
	// preventing race conditions when multiple goroutines access or modify these resources.
	mutex sync.Mutex
}

var instance *MySqlConnection
var once sync.Once

// GetMySqlConnection Singleton connection
func GetMySqlConnection() *MySqlConnection {
	once.Do(func() {
		instance = &MySqlConnection{
			connections: make(map[string]*gorm.DB),
			configs:     make(map[string]DBConfig),
		}
	})
	return instance
}

// InitDataSourceConnection initializes a database connection
func (f *MySqlConnection) InitDataSourceConnection(name string, config DBConfig) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if _, exists := f.connections[name]; exists {
		log.Printf("Database connection '%s' already exists.", name)
		return nil
	}

	// GORM connection
	db, err := gorm.Open(mysql.Open(config.DataSourceName), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return fmt.Errorf("failed to initialize database connection %q: %w", name, err)
	}

	// connection pool setup
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to retrieve database handle for '%q': %w", name, err)
	}
	sqlDB.SetMaxOpenConns(config.MaxOpen)
	sqlDB.SetMaxIdleConns(config.MaxIdle)
	sqlDB.SetConnMaxLifetime(config.Lifetime)
	sqlDB.SetConnMaxIdleTime(config.IdleTime)

	if err := sqlDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping database '%q': %w", name, err)
	}

	// Store the connection and configuration
	f.connections[name] = db
	f.configs[name] = config
	fmt.Printf("Database connection '%q' initialized successfully.\n", name)
	return nil
}

// GetDB retrieves an existing database connection by its name.
// If the connection is unhealthy or unavailable, it attempts to reconnect using the stored configuration.
//
// Parameters:
// - name: The name of the database connection to retrieve.
//
// Returns:
// - *gorm.DB: A pointer to the GORM database connection object.
// - error: An error if the connection does not exist or if reconnection fails.
//
// Behavior:
// 1. Locks access to ensure thread-safe operations on the `connections` and `configs` maps.
// 2. Checks if the connection exists. If not, returns an error indicating the connection does not exist.
// 3. Performs a health check by calling `Ping()` on the underlying SQL database connection.
//   - If the health check fails, logs an attempt to reconnect.
//   - If no configuration is available for reconnection, returns an error.
//   - If a configuration is available, attempts to reconnect using the `reconnect` method.
//
// 4. If the connection is healthy, returns the connection.
//
// Notes:
// - The mutex ensures that operations on shared resources (`connections` and `configs`) are thread-safe.
// - The reconnection logic prevents stale or unhealthy connections from being used.
// - Health checks enhance the reliability of the database connection pool.
//
// Example Usage:
// db, err := connection.GetMySqlConnection().GetDB("example_db")
//
//	if err != nil {
//	    log.Fatalf("Failed to retrieve database connection: %v", err)
//	}
//
//	if db != nil {
//	    log.Println("Database connection retrieved successfully.")
//	}
func (f *MySqlConnection) GetDB(name string) (*gorm.DB, error) {
	f.mutex.Lock()
	db, exists := f.connections[name]
	config, configExists := f.configs[name]
	f.mutex.Unlock()

	if !exists {
		return nil, fmt.Errorf("database connection '%q' does not exist", name)
	}

	// Health check
	sqlDB, err := db.DB()
	if err != nil || sqlDB.Ping() != nil {
		log.Printf("Database connection '%s' is not healthy. Attempting to reconnect...", name)

		if !configExists {
			return nil, fmt.Errorf("no configuration found to reconnect database '%q'", name)
		}

		// Attempt to reconnect
		return f.reconnect(name, config)
	}

	return db, nil
}

func (f *MySqlConnection) reconnect(name string, config DBConfig) (*gorm.DB, error) {

	// Close the unhealthy connection which needs to be reconnected
	err := f.CloseConnection(name)
	if err != nil {
		return nil, fmt.Errorf("failed to remove connection '%q': %w", name, err)
	}

	// Reinitialize the connection
	err = f.InitDataSourceConnection(name, config)
	if err != nil {
		return nil, fmt.Errorf("failed to reconnect to database '%q': %w", name, err)
	}

	// Return the reinitialized connection
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.connections[name], nil
}

// CloseAllConnections closes all database connections and remove configs
func (f *MySqlConnection) CloseAllConnections() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	for name, db := range f.connections {
		sqlDB, err := db.DB()
		if err != nil {
			fmt.Printf("Error retrieving database handle for %q: %v\n", name, err)
			continue
		}

		if err := sqlDB.Close(); err != nil {
			fmt.Printf("Error closing database connection %q: %v\n", name, err)
		} else {
			fmt.Printf("Database connection %q closed successfully and config remved.\n", name)
		}
	}

	f.connections = make(map[string]*gorm.DB)
	f.configs = make(map[string]DBConfig)
}

// CloseConnection closes a specific database connection and removes its config
func (f *MySqlConnection) CloseConnection(name string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// Check if the connection exists
	db, exists := f.connections[name]
	if !exists {
		return fmt.Errorf("database connection '%q' does not exist", name)
	}

	// Retrieve the SQL DB handle
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("error retrieving database handle for '%q': %v", name, err)
	}

	// Close the connection
	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("error closing database connection '%q': %v", name, err)
	}

	// Remove connection and config
	delete(f.connections, name)
	delete(f.configs, name)

	fmt.Printf("Database connection '%q' closed successfully and config removed.\n", name)
	return nil
}

// PrintAllExistingDb prints the names of all currently active database connections.
//
// Behavior:
// 1. Locks access to the `connections` map to ensure thread-safe iteration over the active connections.
// 2. Iterates through the `connections` map to retrieve the names of all active connections.
// 3. For each connection:
//   - Attempts to retrieve the underlying SQL database handle.
//   - If retrieval fails, logs an error and skips to the next connection.
//
// 4. Appends the names of valid connections to a list.
// 5. Outputs the list of active connection names to the console.
//
// Notes:
// - This method is primarily for debugging and monitoring purposes, providing a snapshot of all existing database connections.
// - The mutex ensures safe concurrent access to the `connections` map during iteration.
//
// Example Output:
// If there are two active connections ("db1" and "db2"), the console output will be:
// Database connections: [db1 db2]
//
// Example Usage:
// connection.GetMySqlConnection().PrintAllExistingDb()
//
// Limitations:
// - The method only checks the presence of connections in the `connections` map. It does not verify the health of each connection.
func (f *MySqlConnection) PrintAllExistingDb() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	var connectionNames []string
	for name, db := range f.connections {
		_, err := db.DB()
		if err != nil {
			fmt.Printf("Error retrieving database handle for %q: %v\n", name, err)
			continue
		}
		connectionNames = append(connectionNames, name)
	}

	fmt.Printf("Database connections: %v\n", connectionNames)
}

// GetDbConfig retrieves the configuration for a specific database connection.
//
// Parameters:
// - conName: The name of the database connection for which the configuration is requested.
//
// Returns:
// - DBConfig: The configuration details (e.g., connection string, pool settings) of the specified database connection.
// - If the connection name does not exist, it returns an empty `DBConfig` structure.
//
// Behavior:
// 1. Acquires a mutex lock to ensure thread-safe access to the `configs` map.
// 2. Retrieves the configuration for the specified `conName` from the `configs` map.
// 3. If the configuration does not exist (an empty `DBConfig` is found):
//   - Logs a message to the console indicating that the connection name does not exist.
//   - Returns an empty `DBConfig`.
//
// 4. If the configuration exists, it is returned to the caller.
//
// Example Usage:
// dbConfig := connection.GetMySqlConnection().GetDbConfig("my_database")
//
//	if dbConfig == (DBConfig{}) {
//	    fmt.Println("Configuration for the database does not exist.")
//	} else {
//
//	    fmt.Printf("Database Config: %+v\n", dbConfig)
//	}
//
// Notes:
// - The method is read-only and does not modify the state of the connection pool.
// - Useful for debugging or monitoring to retrieve configuration details of active connections.
//
// Limitations:
// - Returns an empty `DBConfig` when the connection does not exist, which may require additional checks by the caller.
func (f *MySqlConnection) GetDbConfig(conName string) DBConfig {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	config := f.configs[conName]
	if config == (DBConfig{}) {
		fmt.Printf("database connection '%s' does not exist", conName)
		return DBConfig{}
	}
	return config
}
