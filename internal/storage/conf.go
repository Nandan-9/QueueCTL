package storage

import (
	"database/sql"
)

func ConfigSet(db *sql.DB, key, value string) error {
	_, err := db.Exec(`INSERT INTO config(key,value) VALUES(?,?)
                      ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		key, value)
	return err
}

func ConfigGet(db *sql.DB, key string) (string, error) {
	var val string
	err := db.QueryRow(`SELECT value FROM config WHERE key=?`, key).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return val, err
}

func ConfigList(db *sql.DB) (map[string]string, error) {
	rows, err := db.Query(`SELECT key,value FROM config`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := make(map[string]string)
	for rows.Next() {
		var k, v string
		rows.Scan(&k, &v)
		m[k] = v
	}
	return m, nil
}
