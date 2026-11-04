package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// AuthClaims matches the claims expected by internal/adapters/outbound/websocket/middleware.go
type AuthClaims struct {
	ProjectID string `json:"project_id"`
	UserID    string `json:"user_id"`
	jwt.RegisteredClaims
}

func main() {
	projectID := flag.String("project", "quickbite", "Project ID (tenant identifier)")
	userID := flag.String("user", "user_101", "User ID (target recipient identifier)")
	secretKey := flag.String("secret", "quicknotify_dev_secret_key_2026", "HMAC secret key used by QuickNotify")
	expiryDuration := flag.Duration("exp", 24*time.Hour, "Token validity duration (e.g. 1h, 24h, 720h)")
	host := flag.String("host", "localhost", "Server host for generated URLs")
	port := flag.Int("port", 8080, "Direct QuickNotify service HTTP/WS port")
	traefikPort := flag.Int("traefik-port", 80, "Traefik ingress port")
	quiet := flag.Bool("quiet", false, "Output only the raw JWT token string")
	flag.Parse()

	if *projectID == "" || *userID == "" || *secretKey == "" {
		fmt.Fprintln(os.Stderr, "Error: -project, -user, and -secret must not be empty.")
		os.Exit(1)
	}

	now := time.Now()
	claims := AuthClaims{
		ProjectID: *projectID,
		UserID:    *userID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "quicknotify-mock-auth",
			Subject:   *userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(*expiryDuration)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(*secretKey))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating JWT token: %v\n", err)
		os.Exit(1)
	}

	if *quiet {
		fmt.Println(tokenString)
		return
	}

	fmt.Println("================================================================================")
	fmt.Println("                     QUICKNOTIFY MOCK JWT TOKEN GENERATOR                      ")
	fmt.Println("================================================================================")
	fmt.Printf("Project ID: %s\n", *projectID)
	fmt.Printf("User ID:    %s\n", *userID)
	fmt.Printf("Expires In: %v (at %s)\n", *expiryDuration, now.Add(*expiryDuration).Format(time.RFC3339))
	fmt.Printf("Secret Key: %s\n", *secretKey)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("TOKEN:")
	fmt.Println(tokenString)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CONNECTION URLS & TEST EXAMPLES:")
	fmt.Println()
	fmt.Println("1. Direct WebSocket URL:")
	fmt.Printf("   ws://%s:%d/ws?token=%s\n\n", *host, *port, tokenString)

	if *traefikPort == 80 {
		fmt.Println("2. Traefik Ingress WebSocket URL (Port 80):")
		fmt.Printf("   ws://%s/ws?token=%s\n\n", *host, tokenString)
	} else {
		fmt.Printf("2. Traefik Ingress WebSocket URL (Port %d):\n", *traefikPort)
		fmt.Printf("   ws://%s:%d/ws?token=%s\n\n", *host, *traefikPort, tokenString)
	}

	fmt.Println("3. wscat CLI connection command:")
	fmt.Printf("   wscat -c \"ws://%s:%d/ws?token=%s\"\n\n", *host, *port, tokenString)

	fmt.Println("4. Subprotocol Header connection:")
	fmt.Printf("   Header: 'Sec-WebSocket-Protocol: %s'\n", tokenString)
	fmt.Println("================================================================================")
}

