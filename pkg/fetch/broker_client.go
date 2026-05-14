package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

type brokerClient struct {
	baseURL    string
	httpClient *http.Client
}

type brokerDelegateRequest struct {
	RequestedDestinations []string `json:"requested_destinations"`
}

type brokerDelegateResponse struct {
	Nonce string `json:"nonce"`
}

type brokerTokenRequest struct {
	Nonce       string `json:"nonce"`
	Destination string `json:"destination"`
}

type brokerTokenResponse struct {
	Token string `json:"token"`
}

func newBrokerClient(baseURL string) *brokerClient {
	return &brokerClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 15 * 1e9}, // 15s
	}
}

func (c *brokerClient) fetchCredential(ctx context.Context, clientJWT, destination string) (string, error) {
	nonce, err := c.delegate(ctx, clientJWT, destination)
	if err != nil {
		return "", fmt.Errorf("delegate: %w", err)
	}
	token, err := c.token(ctx, nonce, destination)
	if err != nil {
		return "", fmt.Errorf("token: %w", err)
	}
	return token, nil
}

func (c *brokerClient) delegate(ctx context.Context, clientJWT, destination string) (string, error) {
	body, _ := json.Marshal(brokerDelegateRequest{RequestedDestinations: []string{destination}})
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/delegate", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+clientJWT)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("POST /delegate: HTTP %d: %s", resp.StatusCode, snippet)
	}

	var dr brokerDelegateResponse
	if err := json.NewDecoder(resp.Body).Decode(&dr); err != nil {
		return "", fmt.Errorf("decode /delegate response: %w", err)
	}
	return dr.Nonce, nil
}

func (c *brokerClient) token(ctx context.Context, nonce, destination string) (string, error) {
	body, _ := json.Marshal(brokerTokenRequest{Nonce: nonce, Destination: destination})
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return "", fmt.Errorf("POST /token for %q: HTTP %d: %s", destination, resp.StatusCode, snippet)
	}

	var tr brokerTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		return "", fmt.Errorf("decode /token response: %w", err)
	}
	return tr.Token, nil
}

func extractBearerToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	auths := md.Get("authorization")
	if len(auths) == 0 {
		return ""
	}
	return strings.TrimPrefix(auths[0], "Bearer ")
}
