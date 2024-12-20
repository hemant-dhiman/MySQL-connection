package main

import (
	"github.com/hemant-dhiman/MySQL-connection/connection"
	"github.com/hemant-dhiman/MySQL-connection/constants"
	"log"
	"os"
	"time"
)

func main() {
	con := connection.GetMySqlConnection()
	getenv := os.Getenv(constants.ENV_PANEL_MYSQL_CONNECTION_STRING)
	log.Printf("<%v>", getenv)
	mySqlConfig := connection.DBConfig{
		DataSourceName: getenv,
		MaxOpen:        12,
		MaxIdle:        10,
		Lifetime:       5 * time.Minute,
		IdleTime:       1 * time.Minute,
	}
	err := con.InitDataSourceConnection("mysql", mySqlConfig)
	if err != nil {
		log.Printf("Failed during duplicate initialization: %v", err)
	}

	db, err := con.GetDB("mysql")

	if err != nil {
		log.Printf("Error retrieving database connection: %v\n", err)
	}
	var count int64
	if err := db.Raw("SELECT COUNT(*) FROM izooto.audience").Scan(&count).Error; err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	log.Printf("audience count: %d", count)
}
