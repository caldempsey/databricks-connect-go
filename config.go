package databricks

import (
	"fmt"
	"strings"
	"time"
)

// Logger is the interface for logging. Implement this interface to use your preferred logging library.
type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// Config holds the configuration for connecting to Databricks.
type Config struct {
	// WorkspaceURL is the URL of your Databricks workspace
	// Example: https://your-workspace.databricks.com
	WorkspaceURL string

	// ClientID is the OAuth client ID for authentication
	ClientID string

	// ClientSecret is the OAuth client secret for authentication
	ClientSecret string

	// ClusterID is the ID of the Databricks cluster to connect to
	ClusterID string

	// Logger is an optional logger. If not provided, a no-op logger will be used
	Logger Logger

	// HTTPTimeout is the timeout for HTTP requests. Defaults to 30 seconds if not set
	HTTPTimeout time.Duration

	// TokenRefreshBuffer is how long before token expiry to refresh. Defaults to 5 minutes
	TokenRefreshBuffer time.Duration
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.WorkspaceURL == "" {
		return fmt.Errorf("WorkspaceURL is required")
	}
	if c.ClientID == "" {
		return fmt.Errorf("ClientID is required")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("ClientSecret is required")
	}
	if c.ClusterID == "" {
		return fmt.Errorf("ClusterID is required")
	}

	// Ensure URL doesn't have trailing slash
	c.WorkspaceURL = strings.TrimRight(c.WorkspaceURL, "/")

	// Set defaults
	if c.HTTPTimeout == 0 {
		c.HTTPTimeout = 30 * time.Second
	}
	if c.TokenRefreshBuffer == 0 {
		c.TokenRefreshBuffer = 5 * time.Minute
	}
	if c.Logger == nil {
		c.Logger = &noOpLogger{}
	}

	return nil
}

// sparkConnectURL builds the Spark Connect URL with the provided token
func (c *Config) sparkConnectURL(token string) string {
	host := strings.TrimPrefix(strings.TrimPrefix(c.WorkspaceURL, "https://"), "http://")
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	return fmt.Sprintf("sc://%s:443/;token=%s;x-databricks-cluster-id=%s",
		host, token, c.ClusterID)
}

// noOpLogger is a logger that does nothing
type noOpLogger struct{}

func (n *noOpLogger) Debug(msg string, keysAndValues ...any) {}
func (n *noOpLogger) Info(msg string, keysAndValues ...any)  {}
func (n *noOpLogger) Warn(msg string, keysAndValues ...any)  {}
func (n *noOpLogger) Error(msg string, keysAndValues ...any) {}
