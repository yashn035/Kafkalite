package main

import (
	"flag"
	"fmt"
	"os"

	"kafkalite/internal/auth"
)

func main() {
	username := flag.String("username", "admin", "Username for the token")
	role := flag.String("role", "admin", "Role for the token (admin, producer, consumer)")
	secret := flag.String("secret", "", "Optional custom JWT secret")
	flag.Parse()

	if *secret != "" {
		os.Setenv("JWT_SECRET", *secret)
	}

	token, err := auth.GenerateJWT(*username, *role)
	if err != nil {
		fmt.Printf("Error generating token: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(token)
}
