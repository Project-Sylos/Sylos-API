package users

import (
	"errors"

	"codeberg.org/Sylos/Sylos-API/internal/corebridge/apidb"
)

func (s *Store) GetPreferencesJSON(id string) (string, error) {
	prefs, err := s.db.GetUserPreferences(id)
	if err != nil {
		if errors.Is(err, apidb.ErrNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	return prefs, nil
}

func (s *Store) SetPreferencesJSON(id string, prefsJSON string) error {
	err := s.db.SetUserPreferences(id, prefsJSON)
	if errors.Is(err, apidb.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
