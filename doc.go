// Package databricks provides a Go client for managing Apache Spark sessions
// with Databricks Connect, featuring automatic OAuth token refresh and
// session lifecycle management.
//
// This library simplifies the process of connecting to Databricks clusters
// using OAuth 2.0 authentication and maintains session validity through
// automatic token renewal.
//
// Basic usage:
//
//	config := databricks.Config{
//	    WorkspaceURL: "https://your-workspace.databricks.com",
//	    ClientID:     "your-client-id",
//	    ClientSecret: "your-client-secret",
//	    ClusterID:    "your-cluster-id",
//	}
//
//	manager, err := databricks.NewSessionManager(config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer manager.Close()
//
//	spark, err := manager.GetSession()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Use spark session for your operations
//	df, err := spark.Sql(ctx, "SELECT * FROM table")
package databricks
