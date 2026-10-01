package httpapi

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

var ErrUnauthenticated = errors.New("unauthenticated")

type Principal struct {
	ProviderID string
	Internal   bool
}

type Authenticator interface {
	Authenticate(context.Context, string) (Principal, error)
}

type OIDCAuthenticator struct {
	verifier         *oidc.IDTokenVerifier
	internalClientID string
}

func NewOIDCAuthenticator(ctx context.Context, issuer, audience, internalClientID string) (*OIDCAuthenticator, error) {
	issuer = strings.TrimRight(strings.TrimSpace(issuer), "/")
	if issuer == "" || strings.TrimSpace(audience) == "" || strings.TrimSpace(internalClientID) == "" {
		return nil, errors.New("OIDC issuer, audience, and internal client ID are required")
	}
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("load OIDC discovery document: %w", err)
	}
	return &OIDCAuthenticator{
		verifier:         provider.Verifier(&oidc.Config{ClientID: audience}),
		internalClientID: internalClientID,
	}, nil
}

func (authenticator *OIDCAuthenticator) Authenticate(ctx context.Context, rawToken string) (Principal, error) {
	token, err := authenticator.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: verify OIDC token: %v", ErrUnauthenticated, err)
	}
	var claims struct {
		AuthorizedParty string `json:"azp"`
		ProviderID      string `json:"provider_id"`
	}
	if err := token.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: decode OIDC claims: %v", ErrUnauthenticated, err)
	}
	if token.Subject == "" || claims.AuthorizedParty == "" {
		return Principal{}, fmt.Errorf("%w: required OIDC subject or authorized party is missing", ErrUnauthenticated)
	}
	if claims.AuthorizedParty == authenticator.internalClientID && claims.ProviderID == "" {
		return Principal{Internal: true}, nil
	}
	if claims.ProviderID != "" && claims.AuthorizedParty != authenticator.internalClientID {
		return Principal{ProviderID: claims.ProviderID}, nil
	}
	return Principal{}, fmt.Errorf("%w: unsupported OIDC client identity azp=%q provider_id=%q", ErrUnauthenticated, claims.AuthorizedParty, claims.ProviderID)
}
