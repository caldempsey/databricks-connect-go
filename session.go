package databricks

import (
	"context"
	"fmt"
	"sync"
	"time"

	scsql "github.com/apache/spark-connect-go/v40/spark/sql"
)

// SessionManager manages Spark sessions with automatic OAuth token refresh
type SessionManager struct {
	config Config
	logger Logger
	oauth  *oauthClient

	mu          sync.RWMutex
	spark       scsql.SparkSession
	tokenExpiry time.Time
	closed      bool
}

// NewSessionManager creates a new session manager with the provided configuration
func NewSessionManager(config Config) (*SessionManager, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	manager := &SessionManager{
		config: config,
		logger: config.Logger,
		oauth:  newOAuthClient(config),
	}

	// Initialize first session
	if _, err := manager.GetSession(); err != nil {
		return nil, fmt.Errorf("failed to initialize session: %w", err)
	}

	manager.logger.Info("Session manager initialized successfully")
	return manager, nil
}

// GetSession returns a valid Spark session, creating or refreshing as needed.
// The session is automatically refreshed when the OAuth token is about to expire.
func (m *SessionManager) GetSession() (scsql.SparkSession, error) {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return nil, ErrSessionClosed
	}

	// Check if current session is still valid
	if m.spark != nil && time.Now().Before(m.tokenExpiry.Add(-m.config.TokenRefreshBuffer)) {
		spark := m.spark
		m.mu.RUnlock()
		return spark, nil
	}
	m.mu.RUnlock()

	// Need to refresh session
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if m.closed {
		return nil, ErrSessionClosed
	}

	// Check again in case another goroutine already refreshed
	if m.spark != nil && time.Now().Before(m.tokenExpiry.Add(-m.config.TokenRefreshBuffer)) {
		return m.spark, nil
	}

	m.logger.Info("Refreshing Spark session with new OAuth token")

	// Get new token
	token, expiresIn, err := m.oauth.getAccessToken()
	if err != nil {
		return nil, ErrTokenRefresh{Underlying: err}
	}

	// Update expiry time
	m.tokenExpiry = time.Now().Add(time.Duration(expiresIn) * time.Second)

	// Build new session
	sparkConnectURL := m.config.sparkConnectURL(token)
	newSpark, err := (&scsql.SparkSessionBuilder{}).
		Remote(sparkConnectURL).
		Build(context.Background())
	if err != nil {
		return nil, ErrSessionCreation{Underlying: err}
	}

	// Store old session for cleanup
	oldSpark := m.spark

	// Update to new session
	m.spark = newSpark

	// Clean up old session asynchronously
	if oldSpark != nil {
		go func() {
			m.logger.Debug("Stopping old Spark session")
			if err := oldSpark.Stop(); err != nil {
				m.logger.Warn("Failed to stop old Spark session", "error", err)
			}
		}()
	}

	m.logger.Info("Successfully refreshed Spark session",
		"token_expiry", m.tokenExpiry,
		"expires_in_seconds", expiresIn)

	return m.spark, nil
}

// Close closes the session manager and stops the underlying Spark session
func (m *SessionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true

	if m.spark != nil {
		m.logger.Info("Closing session manager and stopping Spark session")
		if err := m.spark.Stop(); err != nil {
			return fmt.Errorf("failed to stop Spark session: %w", err)
		}
		m.spark = nil
	}

	m.logger.Info("Session manager closed successfully")
	return nil
}

// IsHealthy checks if the session manager is healthy and can provide sessions
func (m *SessionManager) IsHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return !m.closed && m.spark != nil
}

// TokenExpiryTime returns the current token expiry time
func (m *SessionManager) TokenExpiryTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.tokenExpiry
}
