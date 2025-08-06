package main

import (
	"context"
	"fmt"
	"log"
	"os"

	databricks "github.com/caldempsey/databricks-connect-go"
	"github.com/rs/zerolog"
)

// ZerologAdapter adapts zerolog to the databricks.Logger interface
type ZerologAdapter struct {
	logger zerolog.Logger
}

func NewZerologAdapter(logger zerolog.Logger) databricks.Logger {
	return &ZerologAdapter{logger: logger}
}

func (z *ZerologAdapter) Debug(msg string, keysAndValues ...any) {
	event := z.logger.Debug()
	z.addFields(event, keysAndValues...).Msg(msg)
}

func (z *ZerologAdapter) Info(msg string, keysAndValues ...any) {
	event := z.logger.Info()
	z.addFields(event, keysAndValues...).Msg(msg)
}

func (z *ZerologAdapter) Warn(msg string, keysAndValues ...any) {
	event := z.logger.Warn()
	z.addFields(event, keysAndValues...).Msg(msg)
}

func (z *ZerologAdapter) Error(msg string, keysAndValues ...any) {
	event := z.logger.Error()
	z.addFields(event, keysAndValues...).Msg(msg)
}

func (z *ZerologAdapter) addFields(event *zerolog.Event, keysAndValues ...any) *zerolog.Event {
	for i := 0; i < len(keysAndValues); i += 2 {
		if i+1 < len(keysAndValues) {
			key := fmt.Sprint(keysAndValues[i])
			event = event.Interface(key, keysAndValues[i+1])
		}
	}
	return event
}

func main() {
	// Create zerolog logger
	zlog := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Adapt zerolog to our Logger interface
	logger := NewZerologAdapter(zlog)

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

	// Use the session
	ctx := context.Background()
	df, err := spark.Sql(ctx, "SELECT current_timestamp() as now")
	if err != nil {
		log.Fatalf("Failed to execute SQL: %v", err)
	}

	rows, err := df.Collect(ctx)
	if err != nil {
		log.Fatalf("Failed to collect results: %v", err)
	}

	for _, row := range rows {
		fmt.Printf("Current timestamp: %v\n", row.Values())
	}
}
