package users

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
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
	db         *sql.DB
	bcryptCost int
	ownsDB     bool
}

// OpenConn opens a user store on an existing connection or a new plaintext file at path.
func OpenConn(conn *sql.DB, path string, bcryptCost int) (*Store, error) {
	if bcryptCost <= 0 {
		bcryptCost = bcrypt.DefaultCost
	}
	var db *sql.DB
	var err error
	if conn != nil {
		db = conn
	} else {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		db, err = sql.Open("duckdb", path)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
	}

	store := &Store{db: db, bcryptCost: bcryptCost, ownsDB: conn == nil}
	if err := store.migrate(); err != nil {
		if store.ownsDB {
			_ = db.Close()
		}
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s.db == nil || !s.ownsDB {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS users (
  id VARCHAR PRIMARY KEY,
  username VARCHAR NOT NULL,
  password_hash VARCHAR NOT NULL,
  role VARCHAR NOT NULL,
  created_at VARCHAR NOT NULL,
  disabled BOOLEAN NOT NULL DEFAULT false
);`); err != nil {
		return err
	}

	_, err := s.db.Exec(`
CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower
ON users (lower(username));`)
	if err != nil {
		return err
	}

	if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN preferences VARCHAR`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return err
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN recovery_code_hash VARCHAR`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return err
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN recovery_reissue_on_login BOOLEAN`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return err
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN recovery_ack_pending BOOLEAN`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return err
		}
	}
	_, _ = s.db.Exec(`UPDATE users SET recovery_reissue_on_login = false WHERE recovery_reissue_on_login IS NULL`)
	_, _ = s.db.Exec(`UPDATE users SET recovery_ack_pending = false WHERE recovery_ack_pending IS NULL`)

	if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN last_login_at VARCHAR`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return err
		}
	}
	if _, err := s.db.Exec(`ALTER TABLE users ADD COLUMN last_logout_at VARCHAR`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return err
		}
	}

	if _, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS user_audit_events (
  id VARCHAR PRIMARY KEY,
  occurred_at VARCHAR NOT NULL,
  actor_user_id VARCHAR,
  target_user_id VARCHAR,
  action VARCHAR NOT NULL,
  metadata VARCHAR
);`); err != nil {
		return err
	}
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS user_audit_events_occurred_at ON user_audit_events (occurred_at)`)
	return err
}

func (s *Store) Count() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count)
	return count, err
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

	_, err = s.db.Exec(
		`INSERT INTO users (id, username, password_hash, role, created_at, disabled) VALUES (?, ?, ?, ?, ?, false)`,
		user.ID, user.Username, string(hash), string(user.Role), user.CreatedAt.Format(time.RFC3339),
	)
	if err != nil {
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
	row := s.db.QueryRow(
		`SELECT id, username, role, created_at, disabled, last_login_at, last_logout_at FROM users WHERE id = ?`,
		id,
	)
	return scanUser(row)
}

func (s *Store) List() ([]User, error) {
	rows, err := s.db.Query(
		`SELECT id, username, role, created_at, disabled, last_login_at, last_logout_at FROM users ORDER BY lower(username)`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []User
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, user)
	}
	return out, rows.Err()
}

func (s *Store) Update(id string, password *string, role *Role, disabled *bool) (User, error) {
	user, _, err := s.findByID(id)
	if err != nil {
		return User{}, err
	}

	if role != nil && *role != user.Role {
		if user.Role == RoleAdmin && *role != RoleAdmin {
			if err := s.ensureNotLastAdmin(id); err != nil {
				return User{}, err
			}
		}
		user.Role = *role
	}
	if disabled != nil {
		if user.Role == RoleAdmin && *disabled {
			if err := s.ensureNotLastAdmin(id); err != nil {
				return User{}, err
			}
		}
		user.Disabled = *disabled
	}

	if password != nil && *password != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(*password), s.bcryptCost)
		if err != nil {
			return User{}, err
		}
		if _, err := s.db.Exec(`UPDATE users SET password_hash = ? WHERE id = ?`, string(hash), id); err != nil {
			return User{}, err
		}
	}

	if _, err := s.db.Exec(`UPDATE users SET role = ?, disabled = ? WHERE id = ?`, string(user.Role), user.Disabled, id); err != nil {
		return User{}, err
	}
	return user, nil
}

func (s *Store) Delete(id string) error {
	user, _, err := s.findByID(id)
	if err != nil {
		return err
	}
	if user.Role == RoleAdmin {
		if err := s.ensureNotLastAdmin(id); err != nil {
			return err
		}
	}
	res, err := s.db.Exec(`DELETE FROM users WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
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
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM users WHERE role = ? AND disabled = false AND id != ?`,
		RoleAdmin, id,
	).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrLastAdmin
	}
	return nil
}

func (s *Store) findByUsername(username string) (User, string, error) {
	row := s.db.QueryRow(
		`SELECT id, username, role, created_at, disabled, last_login_at, last_logout_at, password_hash FROM users WHERE lower(username) = lower(?)`,
		strings.TrimSpace(username),
	)
	var hash string
	user, err := scanUserWithHash(row, &hash)
	return user, hash, err
}

func (s *Store) findByID(id string) (User, string, error) {
	row := s.db.QueryRow(
		`SELECT id, username, role, created_at, disabled, last_login_at, last_logout_at, password_hash FROM users WHERE id = ?`,
		id,
	)
	var hash string
	user, err := scanUserWithHash(row, &hash)
	return user, hash, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanUser(row rowScanner) (User, error) {
	var (
		user         User
		role         string
		createdAt    string
		disabled     bool
		lastLogin    sql.NullString
		lastLogout   sql.NullString
	)
	if err := row.Scan(&user.ID, &user.Username, &role, &createdAt, &disabled, &lastLogin, &lastLogout); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	user.Role = Role(role)
	user.Disabled = disabled
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		user.CreatedAt = t
	}
	user.LastLoginAt = parseOptionalTime(lastLogin)
	user.LastLogoutAt = parseOptionalTime(lastLogout)
	return user, nil
}

func scanUserWithHash(row rowScanner, hash *string) (User, error) {
	var (
		user         User
		role         string
		createdAt    string
		disabled     bool
		lastLogin    sql.NullString
		lastLogout   sql.NullString
	)
	if err := row.Scan(&user.ID, &user.Username, &role, &createdAt, &disabled, &lastLogin, &lastLogout, hash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	user.Role = Role(role)
	user.Disabled = disabled
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		user.CreatedAt = t
	}
	user.LastLoginAt = parseOptionalTime(lastLogin)
	user.LastLogoutAt = parseOptionalTime(lastLogout)
	return user, nil
}

func parseOptionalTime(value sql.NullString) *time.Time {
	if !value.Valid {
		return nil
	}
	raw := strings.TrimSpace(value.String)
	if raw == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil
	}
	return &t
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") ||
		strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "constraint")
}
