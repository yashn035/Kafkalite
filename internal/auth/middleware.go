package auth

import (
	"errors"
	"net"

	"kafkalite/internal/protocol"
)

// AuthenticateConnection reads the first frame hoping for a ReqAuthenticate.
// Returns role, token string, error
func AuthenticateConnection(conn net.Conn) (string, string, error) {
	req, err := protocol.ReadRequest(conn)
	if err != nil {
		return "", "", err
	}
	if req.Type != protocol.ReqAuthenticate {
		return "", "", errors.New("authentication required")
	}

	// Verify hardcoded credentials
	var role string
	if req.Username == "admin" && req.Password == "admin" {
		role = RoleAdmin
	} else if req.Username == "producer" && req.Password == "producer" {
		role = RoleProducer
	} else if req.Username == "consumer" && req.Password == "consumer" {
		role = RoleConsumer
	} else {
		return "", "", errors.New("invalid credentials")
	}

	token, err := GenerateJWT(req.Username, role)
	if err != nil {
		return "", "", err
	}

	return role, token, nil
}
