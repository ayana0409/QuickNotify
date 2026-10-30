package websocket

import (
	"net/http"
	"strings"
)

// IsOriginAllowed checks if a given origin matches any of the configured allowed origins or wildcards.
func IsOriginAllowed(origin string, allowedOrigins []string) bool {
	if len(allowedOrigins) == 0 {
		return false
	}

	trimmedOrigin := strings.TrimSpace(origin)
	if trimmedOrigin == "" {
		// Non-browser clients (native mobile apps, server-side services) usually do not send Origin header
		return true
	}

	for _, pattern := range allowedOrigins {
		pattern = strings.TrimSpace(pattern)
		if pattern == "*" {
			return true
		}

		if strings.EqualFold(pattern, trimmedOrigin) {
			return true
		}

		// Support wildcard pattern matching (e.g., "http://localhost:*", "https://*.example.com")
		if strings.Contains(pattern, "*") {
			if matchWildcard(pattern, trimmedOrigin) {
				return true
			}
		}
	}

	return false
}

// matchWildcard compares an origin with a single wildcard pattern (prefix*suffix).
func matchWildcard(pattern, target string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) != 2 {
		return false
	}

	pLower := strings.ToLower(pattern)
	tLower := strings.ToLower(target)
	prefix := strings.ToLower(parts[0])
	suffix := strings.ToLower(parts[1])

	if len(tLower) < len(pLower)-1 {
		return false
	}

	return strings.HasPrefix(tLower, prefix) && strings.HasSuffix(tLower, suffix)
}

// CORSMiddleware enforces origin validation and injects standard CORS headers for HTTP routes.
func CORSMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			if origin != "" && IsOriginAllowed(origin, allowedOrigins) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Sec-WebSocket-Protocol, Sec-WebSocket-Extensions, Sec-WebSocket-Key, Sec-WebSocket-Version")
			}

			// Handle preflight OPTIONS request
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

