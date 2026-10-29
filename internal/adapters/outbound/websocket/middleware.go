package websocket

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/zap"
)

type contextKey string

const (
	ProjectIDKey contextKey = "project_id"
	UserIDKey    contextKey = "user_id"
)

// AuthClaims represents custom JWT claims containing project and user identifiers.
type AuthClaims struct {
	ProjectID string `json:"project_id"`
	UserID    string `json:"user_id"`
	jwt.RegisteredClaims
}

// AuthMiddleware validates JWT authentication for WebSocket handshake requests.
// It extracts the token from URL queries ("?token="), Sec-WebSocket-Protocol, or Authorization header.
func AuthMiddleware(secretKey string, logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString := extractToken(r)
			if tokenString == "" {
				logger.Warn("WebSocket handshake rejected: missing authentication token",
					zap.String("remote_addr", r.RemoteAddr),
				)
				http.Error(w, "Unauthorized: missing token", http.StatusUnauthorized)
				return
			}

			projectID, userID, err := validateToken(tokenString, secretKey)
			if err != nil {
				logger.Warn("WebSocket handshake rejected: invalid token",
					zap.String("remote_addr", r.RemoteAddr),
					zap.Error(err),
				)
				http.Error(w, fmt.Sprintf("Unauthorized: %v", err), http.StatusUnauthorized)
				return
			}

			// Store extracted identities securely in the request context
			ctx := context.WithValue(r.Context(), ProjectIDKey, projectID)
			ctx = context.WithValue(ctx, UserIDKey, userID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractToken attempts to find the JWT string from query parameters, websocket protocols, or headers.
func extractToken(r *http.Request) string {
	// 1. Try URL query parameter (?token=...)
	if token := r.URL.Query().Get("token"); token != "" {
		return token
	}

	// 2. Try Authorization Header (Bearer ...)
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return parts[1]
		}
	}

	// 3. Try Sec-WebSocket-Protocol (e.g., "jwt, <token>" or "<token>")
	if protoHeader := r.Header.Get("Sec-WebSocket-Protocol"); protoHeader != "" {
		protocols := strings.Split(protoHeader, ",")
		for _, p := range protocols {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" && !strings.EqualFold(trimmed, "jwt") {
				return trimmed
			}
		}
	}

	return ""
}

// validateToken parses and verifies the signature and claims of the JWT string.
func validateToken(tokenString, secretKey string) (projectID, userID string, err error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Ensure token signing method is HMAC
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secretKey), nil
	})

	if err != nil {
		return "", "", err
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return "", "", errors.New("invalid token claims")
	}

	pID, okP := claims["project_id"].(string)
	uID, okU := claims["user_id"].(string)
	if !okP || !okU || pID == "" || uID == "" {
		return "", "", errors.New("token claims must contain valid 'project_id' and 'user_id'")
	}

	return pID, uID, nil
}

// GetAuthInfo retrieves the authenticated ProjectID and UserID from the context.
func GetAuthInfo(ctx context.Context) (projectID, userID string, ok bool) {
	pID, okP := ctx.Value(ProjectIDKey).(string)
	uID, okU := ctx.Value(UserIDKey).(string)
	if !okP || !okU {
		return "", "", false
	}
	return pID, uID, true
}

