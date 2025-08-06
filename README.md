# databricks-connect-go

A Go library for managing Apache Spark sessions with Databricks Connect, featuring automatic OAuth token refresh and session lifecycle management.

## Features

- 🔐 **OAuth 2.0 Authentication** - Secure authentication using client credentials flow
- 🔄 **Automatic Token Refresh** - Seamlessly refreshes OAuth tokens before expiry
- 💻 **Automatic Start-Stop Clusters** - Automatically starts compute on your Databricks Workspace for any offline cluster.
- 🧵 **Thread-Safe** - Concurrent-safe session management
- 📝 **Flexible Logging** - Bring your own logger with a simple interface
- ⚡ **Efficient Session Reuse** - Minimizes session creation overhead
- 🛡️ **Graceful and hardened** - Battle tested library and graceful cleanup

## Quick Start

```go
package main

import (
    "context"
    "log"
    
    databricks "github.com/caldempsey/databricks-connect-go"
)

func main() {
    config := databricks.Config{
        WorkspaceURL: "https://your-workspace.databricks.com",
        ClientID:     "your-client-id",
        ClientSecret: "your-client-secret",
        ClusterID:    "your-cluster-id",
    }

    manager, err := databricks.NewSessionManager(config)
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close()

    // Get Spark session - automatically refreshed as needed
    spark, err := manager.GetSession()
    if err != nil {
        log.Fatal(err)
    }

    // Use the Spark session
    df, err := spark.Sql(context.Background(), "SELECT * FROM table")
    // ... work with your DataFrame
}
```

## Configuration

The `Config` struct supports the following options:

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `WorkspaceURL` | string | Yes | Databricks workspace URL | - |
| `ClientID` | string | Yes | OAuth client ID | - |
| `ClientSecret` | string | Yes | OAuth client secret | - |
| `ClusterID` | string | Yes | Databricks cluster ID | - |
| `Logger` | Logger | No | Logger instance | No-op logger |
| `HTTPTimeout` | time.Duration | No | HTTP client timeout | 30 seconds |
| `TokenRefreshBuffer` | time.Duration | No | Buffer time before token expiry to refresh | 5 minutes |

## Logging

The library uses a simple `Logger` interface that you can implement with any logging library:

```go
type Logger interface {
    Debug(msg string, keysAndValues ...any)
    Info(msg string, keysAndValues ...any)
    Warn(msg string, keysAndValues ...any)
    Error(msg string, keysAndValues ...any)
}
```

### Using Standard Library Logger

```go
logger := databricks.NewStdLogger(log.Default())
config.Logger = logger
```

### Using slog (Go 1.21+)

```go
slogger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
config.Logger = databricks.NewSlogAdapter(slogger)
```

### Using Custom Loggers

See the `examples/with_zerolog` directory for an example of adapting other logging libraries like zerolog.

### No-Op Logger

If no logger is provided, a no-op logger is used by default (no output).

## Authentication Setup

This library uses OAuth 2.0 machine-to-machine (M2M) authentication. To set up:

1. Create a service principal in your Databricks workspace
2. Generate OAuth credentials (client ID and secret)
3. Grant necessary permissions to the service principal
4. Use the credentials in your configuration

See [Databricks OAuth documentation](https://docs.databricks.com/dev-tools/auth/oauth-m2m.html) for detailed setup instructions.

## Advanced Usage

### Custom Logger

```go
// Using standard library logger
logger := databricks.NewStdLogger(
    log.New(os.Stderr, "[spark] ", log.LstdFlags|log.Lshortfile),
)

config := databricks.Config{
    // ... other config
    Logger: logger,
}

// Or using slog with structured logging
slogger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

config := databricks.Config{
    // ... other config
    Logger: databricks.NewSlogAdapter(slogger),
}
```

### Custom Timeouts

```go
config := databricks.Config{
    // ... other config
    HTTPTimeout:        1 * time.Minute,
    TokenRefreshBuffer: 10 * time.Minute,
}
```

### Health Checks

```go
if manager.IsHealthy() {
    // Session manager is ready to provide sessions
}

expiryTime := manager.TokenExpiryTime()
fmt.Printf("Token expires at: %v\n", expiryTime)
```

## Error Handling

The library provides typed errors for better error handling:

```go
spark, err := manager.GetSession()
if err != nil {
    switch err {
    case databricks.ErrSessionClosed:
        // Session manager was closed
    default:
        var tokenErr databricks.ErrTokenRefresh
        if errors.As(err, &tokenErr) {
            // Token refresh failed
        }
        
        var sessionErr databricks.ErrSessionCreation
        if errors.As(err, &sessionErr) {
            // Session creation failed
        }
    }
}
```

## Thread Safety

The `SessionManager` is fully thread-safe. Multiple goroutines can safely call `GetSession()` concurrently. The manager ensures only one token refresh happens at a time, even under concurrent access.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
