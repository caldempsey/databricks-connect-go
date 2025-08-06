package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	databricks "github.com/caldempsey/databricks-connect-go"
)

func main() {
	// Create slog logger with JSON output
	slogger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Adapt slog to our Logger interface
	logger := databricks.NewSlogAdapter(slogger)

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

	// Example: Execute a query
	ctx := context.Background()
	df, err := spark.Sql(ctx, "SHOW TABLES")
	if err != nil {
		log.Fatalf("Failed to execute SQL: %v", err)
	}

	// Collect results
	rows, err := df.Collect(ctx)
	if err != nil {
		log.Fatalf("Failed to collect results: %v", err)
	}

	fmt.Printf("Found %d tables\n", len(rows))
}
