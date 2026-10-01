package httpapi

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOIDCAuthenticatorValidatesJWTClaimsAndSignature(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	issuer := ""
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/realm/.well-known/openid-configuration":
			writeJSON(writer, http.StatusOK, map[string]string{"issuer": issuer, "jwks_uri": issuer + "/keys"})
		case "/realm/keys":
			publicKey := privateKey.PublicKey
			modulus := base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes())
			writeJSON(writer, http.StatusOK, map[string]any{"keys": []map[string]string{{
				"kty": "RSA", "kid": "test-key", "use": "sig", "alg": "RS256", "n": modulus, "e": "AQAB",
			}}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	issuer = server.URL + "/realm"

	authenticator, err := NewOIDCAuthenticator(context.Background(), issuer, "wagering-api", "internal-service")
	if err != nil {
		t.Fatalf("create OIDC authenticator: %v", err)
	}
	validClaims := map[string]any{
		"iss": issuer, "sub": "service-account", "aud": "wagering-api", "azp": "provider-alpha",
		"provider_id": "provider-alpha", "iat": time.Now().Add(-time.Minute).Unix(), "exp": time.Now().Add(time.Minute).Unix(),
	}
	validToken := signTestToken(t, privateKey, validClaims)
	principal, err := authenticator.Authenticate(context.Background(), validToken)
	if err != nil || principal.ProviderID != "provider-alpha" || principal.Internal {
		t.Fatalf("valid provider claims = %#v, error %q", principal, err)
	}
	parts := strings.Split(validToken, ".")
	parts[2] = base64.RawURLEncoding.EncodeToString([]byte("tampered-signature"))
	if _, err := authenticator.Authenticate(context.Background(), strings.Join(parts, ".")); err == nil {
		t.Fatal("JWT with invalid signature was accepted")
	}

	internalClaims := cloneClaims(validClaims)
	delete(internalClaims, "provider_id")
	internalClaims["azp"] = "internal-service"
	principal, err = authenticator.Authenticate(context.Background(), signTestToken(t, privateKey, internalClaims))
	if err != nil || !principal.Internal || principal.ProviderID != "" {
		t.Fatalf("valid internal claims = %#v, error %v", principal, err)
	}

	for _, test := range []struct {
		name   string
		change func(map[string]any)
	}{
		{name: "expired", change: func(claims map[string]any) { claims["exp"] = time.Now().Add(-time.Minute).Unix() }},
		{name: "wrong issuer", change: func(claims map[string]any) { claims["iss"] = issuer + "/other" }},
		{name: "wrong audience", change: func(claims map[string]any) { claims["aud"] = "another-api" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			claims := cloneClaims(validClaims)
			test.change(claims)
			if _, err := authenticator.Authenticate(context.Background(), signTestToken(t, privateKey, claims)); err == nil {
				t.Fatal("invalid JWT was accepted")
			}
		})
	}
	if _, err := authenticator.Authenticate(context.Background(), "not-a-jwt"); err == nil {
		t.Fatal("malformed bearer token was accepted")
	}
}

func signTestToken(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "RS256", "kid": "test-key", "typ": "JWT"})
	if err != nil {
		t.Fatalf("encode test JWT header: %v", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("encode test JWT claims: %v", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(encoded))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("sign test JWT: %v", err)
	}
	return encoded + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func cloneClaims(claims map[string]any) map[string]any {
	cloned := make(map[string]any, len(claims))
	for key, value := range claims {
		cloned[key] = value
	}
	return cloned
}
