package users

import (
	"database/sql"
	"errors"
)

func (s *Store) GetPreferencesJSON(id string) (string, error) {
	var prefs sql.NullString
	err := s.db.QueryRow(`SELECT preferences FROM users WHERE id = ?`, id).Scan(&prefs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	if !prefs.Valid || prefs.String == "" {
		return "", nil
	}
	return prefs.String, nil
}

func (s *Store) SetPreferencesJSON(id string, prefsJSON string) error {
	res, err := s.db.Exec(`UPDATE users SET preferences = ? WHERE id = ?`, prefsJSON, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
