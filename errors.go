package databricks

import "fmt"

// ErrSessionClosed is returned when operations are attempted on a closed session manager
var ErrSessionClosed = fmt.Errorf("session manager is closed")

// ErrTokenRefresh is returned when token refresh fails
type ErrTokenRefresh struct {
	Underlying error
}

func (e ErrTokenRefresh) Error() string {
	return fmt.Sprintf("failed to refresh OAuth token: %v", e.Underlying)
}

func (e ErrTokenRefresh) Unwrap() error {
	return e.Underlying
}

// ErrSessionCreation is returned when session creation fails
type ErrSessionCreation struct {
	Underlying error
}

func (e ErrSessionCreation) Error() string {
	return fmt.Sprintf("failed to create Spark session: %v", e.Underlying)
}

func (e ErrSessionCreation) Unwrap() error {
	return e.Underlying
}
