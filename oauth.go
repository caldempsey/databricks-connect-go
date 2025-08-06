package databricks

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// tokenResponse represents the OAuth token response from Databricks
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
}

// oauthClient handles OAuth token operations
type oauthClient struct {
	workspaceURL string
	clientID     string
	clientSecret string
	httpClient   *http.Client
}

// newOAuthClient creates a new OAuth client
func newOAuthClient(config Config) *oauthClient {
	return &oauthClient{
		workspaceURL: config.WorkspaceURL,
		clientID:     config.ClientID,
		clientSecret: config.ClientSecret,
		httpClient:   &http.Client{Timeout: config.HTTPTimeout},
	}
}

// getAccessToken retrieves an access token from Databricks
func (c *oauthClient) getAccessToken() (token string, expiresIn int, err error) {
	endpoint := c.workspaceURL + "/oidc/v1/token"

	form := url.Values{
		"grant_type": {"client_credentials"},
		"scope":      {"all-apis"},
	}

	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, fmt.Errorf("create request: %w", err)
	}

	req.SetBasicAuth(c.clientID, c.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return "", 0, fmt.Errorf("token request failed with status %s: %s",
			resp.Status, strings.TrimSpace(string(body)))
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", 0, fmt.Errorf("decode response: %w", err)
	}

	if tr.AccessToken == "" {
		return "", 0, fmt.Errorf("empty access_token in response")
	}

	return tr.AccessToken, tr.ExpiresIn, nil
}
