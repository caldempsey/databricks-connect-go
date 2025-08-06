package main

import (
	"context"
	"fmt"
	"log"
	"os"

	databricks "github.com/caldempsey/databricks-connect-go"
)

func main() {
	// Use standard library logger
	logger := databricks.NewStdLogger(log.New(os.Stderr, "[databricks] ", log.LstdFlags))

	// Configure connection
	config := databricks.Config{
		WorkspaceURL: os.Getenv("DATABRICKS_WORKSPACE_URL"),
		ClientID:     os.Getenv("DATABRICKS_CLIENT_ID"),
		ClientSecret: os.Getenv("DATABRICKS_CLIENT_SECRET"),
		ClusterID:    os.Getenv("DATABRICKS_CLUSTER_ID"),
		Logger:       logger,
	}

	// Create session manager
	manager, err := databricks.NewSessionManager(config)
	if err != nil {
		log.Fatalf("Failed to create session manager: %v", err)
	}
	defer manager.Close()

	// Get Spark session
	spark, err := manager.GetSession()
	if err != nil {
		log.Fatalf("Failed to get Spark session: %v", err)
	}

	// Example: Execute a simple query
	ctx := context.Background()
	df, err := spark.Sql(ctx, "SELECT 1 as col")
	if err != nil {
		log.Fatalf("Failed to execute SQL: %v", err)
	}

	// Collect results
	rows, err := df.Collect(ctx)
	if err != nil {
		log.Fatalf("Failed to collect results: %v", err)
	}

	// Print results
	for _, row := range rows {
		fmt.Printf("Row values: %v\n", row.Values())
	}

	// Check session health
	if manager.IsHealthy() {
		fmt.Println("Session manager is healthy")
	}

	// Check token expiry
	fmt.Printf("Token expires at: %v\n", manager.TokenExpiryTime())
}
