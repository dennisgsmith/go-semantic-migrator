package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const databaseDriver = "pgx"

//go:embed schemas/*
var schemasFS embed.FS

//go:embed repeat/*
var repeatFS embed.FS

// version.Version supports Scanner and Valuer interfaces
type SchemaMigrationHistory struct {
	InstalledRank   int
	Version         *Version
	Filename        string
	Checksum        []byte
	InstalledBy     string
	InstalledOn     time.Time
	ExecutionTimeMs int
}

type Version struct {
	*version.Version
}

func (v *Version) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	versionStr, ok := src.(string)
	if !ok {
		return errors.New("invalid type conversion")
	}
	semver, err := version.NewVersion(versionStr)
	if err != nil {
		return err
	}
	if semver != nil {
		v = &Version{semver}
	}

	return nil
}

func main() {
	start := time.Now()
	ctx := context.Background()

	if err := migrate(ctx); err != nil {
		log.Fatalln(err.Error)
	}

	end := time.Now()
	log.Print("successfully completed database migrations in %s", end.Sub(start))
}

func migrate(ctx context.Context) error {
	// make connection to database
	conn, err := sql.Open(databaseDriver, os.Getenv("DATABASE_URL"))
	if err != nil {
		return fmt.Errorf("unable to connect to database: %v\n", err.Error())
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	if err := conn.PingContext(ctx); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migration_history (
			installed_rank integer PRIMARY KEY,
			version text,
			filename text NOT NULL,
			checksum bytea NOT NULL,
			installed_by text NOT NULL,
			installed_on timestamptz NOT NULL DEFAULT now(),
			execution_time_ms integer NOT NULL
		)
	`); err != nil {
		return err
	}

	// check current schema_migration_history version
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Fatalf(err.Error())
		}
	}()

	rows, err := tx.QueryContext(ctx, "SELECT installed_rank, version, filename, checksum, installed_by, installed_on, execution_time_ms FROM schema_migration_history")
	if err != nil {
		return err
	}
	defer rows.Close()

	var history []SchemaMigrationHistory
	for rows.Next() {
		var h SchemaMigrationHistory
		if err := rows.Scan(&h.InstalledRank, &h.Version, &h.Filename, &h.Checksum, &h.InstalledBy, &h.InstalledOn, &h.ExecutionTimeMs); err != nil {
			log.Fatal(err.Error())
		}
		history = append(history, h)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	bySemver := func(a, b SchemaMigrationHistory) int {
		o := a.Version.Compare(b.Version.Version)
		if o == 0 {
			log.Fatalf("duplicate versions for %s", a.Version.String())
		}
		return o
	}
	slices.SortFunc(history, bySemver)

	sh := make(map[string]SchemaMigrationHistory)
	for _, s := range history {
		sh[s.Filename] = s
	}

	schemas, err := schemasFS.ReadDir(".")
	if err != nil {
		return err
	}
	if len(schemas) == 0 {
		return errors.New("schemas directory must not be empty")
	}

	h := sha256.New()
	migrationsNew := make([]SchemaMigrationHistory, 0)
	var mostRecent *SchemaMigrationHistory
	if len(history) > 0 {
		mostRecent = &history[len(history)-1]
	}

	for _, f := range schemas {
		n := f.Name()
		// check hash of all files up to version in FS against hash of most recent version in schema_migration_history
		// if they don't match, exit and tell the user they can't edit existing migrations
		content, err := schemasFS.ReadFile(n)
		if err != nil {
			return err
		}
		if _, err := h.Write(content); err != nil {
			return err
		}

		if !strings.HasPrefix(n, "V") {
			return errors.New("schemas directory must only contain migrations prefixed with 'V' (versioned)")
		}

		v, err := ExtractVersion(n)
		if err != nil {
			return err
		}
		if v == nil {
			return fmt.Errorf("file %s could not be parsed: version is nil", n)
		}

		m, exists := sh[n]
		if !exists {
			if mostRecent != nil && v.LessThan(mostRecent.Version.Version) {
				return fmt.Errorf(
					"migration %s applied out of order: most recent applied version is %s",
					n, mostRecent.Version,
				)
			}

			mNew := SchemaMigrationHistory{
				Version:  &Version{v},
				Filename: n,
			}
			migrationsNew = append(migrationsNew, mNew)
			continue
		}

		checksum := h.Sum(nil)
		log.Printf("checksum: %x", checksum)

		if !bytes.Equal(checksum, m.Checksum) {
			return fmt.Errorf("checksums for %s did not match", n)
		}
	}

	installedRank := len(history) + 1

	for _, m := range migrationsNew {
		content, err := schemasFS.ReadFile(m.Filename)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			return err
		}
		log.Print(string(content))
		// if current version is not the same as newer versions, apply newer migrations
		// update schema_migration_history table with new versions and migration hash sum
		if _, err := h.Write(content); err != nil {
			log.Fatal(err.Error())
		}

		m.InstalledRank = installedRank
		m.Checksum = h.Sum(nil)
		log.Printf("new checksum: %x", m.Checksum)

		if _, err := tx.ExecContext(
			ctx, `
				INSERT INTO schema_migration_history
				(installed_rank, version, filename, checksum)
				VALUES ($1, $2, $3, $4)
			`,
			m.InstalledRank, m.Version, m.Filename, m.Checksum,
		); err != nil {
			return err
		}

		installedRank++
	}

	repeat, err := repeatFS.ReadDir(".")
	if err != nil {
		return err
	}
	sort.Slice(repeat, func(i, j int) bool {
		return repeat[i].Name() < repeat[j].Name()
	})
	for _, f := range repeat {
		content, err := schemasFS.ReadFile(f.Name())
		if err != nil {
			log.Fatal(err.Error())
		}
		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			log.Fatal(err.Error())
		}
		log.Print(string(content))
	}

	return tx.Commit()
}

func ExtractVersion(s string) (*version.Version, error) {
	n, err := ExtractPrefix(s, "V", "__")
	if err != nil {
		return nil, err
	}

	v, err := version.NewVersion(n)
	if err != nil {
		return nil, fmt.Errorf("file %s could not be parsed: error %s; skipping...", s, err.Error())
	}
	return v, nil
}

func ExtractPrefix(s, beginDelim, endDelim string) (string, error) {
	start := strings.Index(s, beginDelim)
	if start == -1 {
		return "", fmt.Errorf("starting sequence '%s' not found", beginDelim)
	}
	start++ // Move index to the character after beginDelim

	end := strings.Index(s, endDelim)
	if end == -1 || end <= start {
		return "", fmt.Errorf("ending sequence '%s' not found or in wrong position", endDelim)
	}

	return s[start:end], nil
}
