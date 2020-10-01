package main

import (
	"context"
	"encoding/base64"
)

type creds struct {
	user string
	pass string
}

func (u creds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"Authorization": "Basic " + basicAuth(u.user, u.pass),
	}, nil
}

func (u creds) RequireTransportSecurity() bool {
	return false
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

