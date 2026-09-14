package users

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
	"github.com/oklog/ulid/v2"
	"golang.org/x/crypto/bcrypt"
)

var (
	ErrNotFound      = errors.New("user not found")
	ErrExists        = errors.New("username already exists")
	ErrInvalidCreds  = errors.New("invalid credentials")
	ErrDisabled      = errors.New("account disabled")
	ErrLastAdmin     = errors.New("cannot remove or demote the last admin")
	ErrSetupComplete = errors.New("setup already completed")
)

type Role string

const (
	RoleAdmin Role = "admin"
	RoleUser  Role = "user"
)

type User struct {
	ID           string     `json:"id"`
	Username     string     `json:"username"`
	Role         Role       `json:"role"`
	CreatedAt    time.Time  `json:"createdAt"`
	Disabled     bool       `json:"disabled"`
	LastLoginAt  *time.Time `json:"lastLoginAt,omitempty"`
	LastLogoutAt *time.Time `json:"lastLogoutAt,omitempty"`
}

type Store struct {
	db         *apidb.DB
	bcryptCost int
}

// Open opens a user store backed by the API Badger database.
func Open(db *apidb.DB, bcryptCost int) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("api database is required")
	}
	if bcryptCost <= 0 {
		bcryptCost = bcrypt.DefaultCost
	}
	return &Store{db: db, bcryptCost: bcryptCost}, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Count() (int, error) {
	return s.db.CountUsers()
}

func (s *Store) Create(username, password string, role Role) (User, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return User{}, fmt.Errorf("username and password are required")
	}
	if role != RoleAdmin && role != RoleUser {
		return User{}, fmt.Errorf("invalid role")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return User{}, err
	}

	user := User{
		ID:        ulid.Make().String(),
		Username:  username,
		Role:      role,
		CreatedAt: time.Now().UTC(),
		Disabled:  false,
	}

	rec := userToRecord(user, string(hash))
	if err := s.db.CreateUser(rec); err != nil {
		if isUniqueViolation(err) {
			return User{}, ErrExists
		}
		return User{}, err
	}
	return user, nil
}

func (s *Store) CreateInitialAdmin(username, password string) (User, error) {
	count, err := s.Count()
	if err != nil {
		return User{}, err
	}
	if count > 0 {
		return User{}, ErrSetupComplete
	}
	return s.Create(username, password, RoleAdmin)
}

func (s *Store) Authenticate(username, password string) (User, error) {
	user, hash, err := s.findByUsername(username)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return User{}, ErrInvalidCreds
		}
		return User{}, err
	}
	if user.Disabled {
		return User{}, ErrDisabled
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
		return User{}, ErrInvalidCreds
	}
	return user, nil
}

func (s *Store) GetByID(id string) (User, error) {
	rec, err := s.db.GetUser(id)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	return recordToUser(rec), nil
}

func (s *Store) List() ([]User, error) {
	recs, err := s.db.ListUsers()
	if err != nil {
		return nil, err
	}
	out := make([]User, 0, len(recs))
	for _, rec := range recs {
		out = append(out, recordToUser(rec))
	}
	return out, nil
}

func (s *Store) Update(id string, password *string, role *Role, disabled *bool) (User, error) {
	rec, err := s.db.GetUser(id)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	user := recordToUser(rec)

	if role != nil && *role != user.Role {
		if user.Role == RoleAdmin && *role != RoleAdmin {
			if err := s.ensureNotLastAdmin(id); err != nil {
				return User{}, err
			}
		}
		user.Role = *role
		rec.Role = string(*role)
	}
	if disabled != nil {
		if user.Role == RoleAdmin && *disabled {
			if err := s.ensureNotLastAdmin(id); err != nil {
				return User{}, err
			}
		}
		user.Disabled = *disabled
		rec.Disabled = *disabled
	}

	if password != nil && *password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(*password), s.bcryptCost)
		if err != nil {
			return User{}, err
		}
		rec.PasswordHash = string(hash)
	}

	if err := s.db.UpdateUser(rec); err != nil {
		if isUniqueViolation(err) {
			return User{}, ErrExists
		}
		return User{}, err
	}
	return user, nil
}

func (s *Store) Delete(id string) error {
	rec, err := s.db.GetUser(id)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	if Role(rec.Role) == RoleAdmin {
		if err := s.ensureNotLastAdmin(id); err != nil {
			return err
		}
	}
	if err := s.db.DeleteUser(id); err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// DeleteMany deletes users by id, skipping actorID and applying the same last-admin rules as Delete.
func (s *Store) DeleteMany(ids []string, actorID string) (deleted []string, err error) {
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || id == actorID {
			continue
		}
		if delErr := s.Delete(id); delErr != nil {
			if err == nil {
				err = delErr
			}
			continue
		}
		deleted = append(deleted, id)
	}
	if len(deleted) == 0 && err != nil {
		return nil, err
	}
	return deleted, err
}

func (s *Store) ensureNotLastAdmin(id string) error {
	count, err := s.db.CountActiveAdminsExcept(id)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrLastAdmin
	}
	return nil
}

func (s *Store) findByUsername(username string) (User, string, error) {
	rec, err := s.db.GetUserByUsername(username)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return User{}, "", ErrNotFound
		}
		return User{}, "", err
	}
	return recordToUser(rec), rec.PasswordHash, nil
}

func (s *Store) findByID(id string) (User, string, error) {
	rec, err := s.db.GetUser(id)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return User{}, "", ErrNotFound
		}
		return User{}, "", err
	}
	return recordToUser(rec), rec.PasswordHash, nil
}

func recordToUser(rec apidb.UserRecord) User {
	return User{
		ID:           rec.ID,
		Username:     rec.Username,
		Role:         Role(rec.Role),
		CreatedAt:    rec.CreatedAt,
		Disabled:     rec.Disabled,
		LastLoginAt:  rec.LastLoginAt,
		LastLogoutAt: rec.LastLogoutAt,
	}
}

func userToRecord(user User, passwordHash string) apidb.UserRecord {
	return apidb.UserRecord{
		ID:           user.ID,
		Username:     user.Username,
		PasswordHash: passwordHash,
		Role:         string(user.Role),
		CreatedAt:    user.CreatedAt,
		Disabled:     user.Disabled,
	}
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") ||
		strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "constraint") ||
		strings.Contains(msg, "already exists")
}
