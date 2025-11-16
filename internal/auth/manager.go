package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type ctxKey string

const (
	claimsContextKey ctxKey = "sylosClaims"
)

var (
	ErrInvalidToken  = errors.New("invalid token")
	ErrMissingSecret = errors.New("jwt secret is required")
)

type Claims struct {
	Subject string   `json:"sub"`
	Roles   []string `json:"roles,omitempty"`
	jwt.RegisteredClaims
}

type Manager struct {
	secret         []byte
	ttl            time.Duration
	allowAnonymous bool
}

type Config struct {
	Secret         string
	TTL            time.Duration
	AllowAnonymous bool
}

func NewManager(cfg Config) (*Manager, error) {
	if cfg.Secret == "" {
		return nil, ErrMissingSecret
	}

	if cfg.TTL <= 0 {
		cfg.TTL = 15 * time.Minute
	}

	return &Manager{
		secret:         []byte(cfg.Secret),
		ttl:            cfg.TTL,
		allowAnonymous: cfg.AllowAnonymous,
	}, nil
}

func (m *Manager) GenerateToken(subject string, roles []string) (string, error) {
	now := time.Now().UTC()
	claims := Claims{
		Subject: subject,
		Roles:   roles,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   subject,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.secret)
}

func (m *Manager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}
		return m.secret, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}

func (m *Manager) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			if m.allowAnonymous {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "missing authorization header", http.StatusUnauthorized)
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			if m.allowAnonymous {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "invalid authorization header", http.StatusUnauthorized)
			return
		}

		claims, err := m.ValidateToken(parts[1])
		if err != nil {
			if m.allowAnonymous {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), claimsContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey).(*Claims)
	return claims, ok
}
