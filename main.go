package migrate

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/go-version"
)

const databaseDriver = "pgx"

//go:embed schema/*.sql
var schemaMigrations embed.FS

//go:embed repeat/*.sql
var repeatMigrations embed.FS

//go:embed seed/*.sql
var seedMigrations embed.FS

type migrationService struct {
	db *sql.DB
}

func NewMigrationService() *migrationService {
	ctx := context.Background()
	db, err := sql.Open(databaseDriver, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("unable to connect to database: %v\n", err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal(err.Error())
		}
	}()

	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err.Error())
	}

	schemas := os.Getenv("DATABASE_SCHEMAS")
	schemaList := strings.Split(schemas, ",")

	if len(schemaList) < 1 {
		log.Fatal("invalid DATABASE_SCHEMAS")
	}

	if len(schemaList) == 1 && schemaList[0] == "" {
		schemaList[0] = "public"
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Fatal(err.Error())
		}
	}()

	for _, schema := range schemaList {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema)); err != nil {
			log.Fatalf("could not create schema %s; %s", schema, err.Error())
		}
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s".schema_migration_history (
			installed_rank integer PRIMARY KEY,
			version text UNIQUE NOT NULL,
			filename text NOT NULL,
			checksum bytea NOT NULL,
			installed_by text NOT NULL,
			installed_on timestamptz NOT NULL DEFAULT now(),
			execution_time_ms integer NOT NULL
		)
	`, schemaList[0])); err != nil {
		log.Fatalf("could not create table schema_migration_history; %s", err.Error())
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err.Error())
	}

	return &migrationService{db}
}

type SchemaMigrationHistory struct {
	InstalledRank   int
	Version         Version
	Filename        string
	Checksum        []byte
	InstalledBy     string
	InstalledOn     time.Time
	ExecutionTimeMs int
}

type Version struct {
	*version.Version
}

type Options struct {
	Init      bool
	SeedLocal bool
}

func (s migrationService) Run(ctx context.Context) {
	start := time.Now()

	if err := s.migrate(ctx, Options{Init: false, SeedLocal: true}); err != nil {
		log.Fatalf("%s\nsuccessfully rolled back migrations!", err.Error())
	}

	end := time.Now()
	log.Printf("successfully completed database migrations in %s", end.Sub(start))
}

func (s migrationService) Diff(ctx context.Context) {
	// TODO
}

func (s migrationService) migrate(ctx context.Context, args Options) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Fatalf(err.Error())
		}
	}()

	history, err := s.querySchemaHistory(ctx)
	if err != nil {
		return err
	}

	sh := make(map[string]SchemaMigrationHistory)
	for _, s := range history {
		sh[s.Filename] = s
	}

	schemas, err := schemaMigrations.ReadDir("schema")
	if err != nil {
		return err
	}
	if len(schemas) == 0 {
		return errors.New("schemas directory must not be empty")
	}

	if args.SeedLocal {
		seed, err := seedMigrations.ReadDir("seed")
		if err != nil {
			return err
		}
		schemas = append(schemas, seed...)
	}

	hasher := sha1.New()
	migrationsNew := make([]SchemaMigrationHistory, 0)
	var mostRecent *SchemaMigrationHistory
	if len(history) > 0 {
		mostRecent = &history[len(history)-1]
	}

	slices.SortFunc(schemas, sortFileBySemver)

	log.Printf("validating %d migrations...", len(history))
	for _, f := range schemas {
		n := f.Name()
		nn := strings.Split(n, "__")
		if len(nn) != 2 {
			return errors.New("all schemas must contain exactly one double underscore (example: V0.0.1__init.sql)")
		}

		v, err := extractVersion(n)
		if err != nil {
			return err
		}
		if err != nil {
			return fmt.Errorf("invalid version prefix: '%s'", nn[0])
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
				Version:  Version{v},
				Filename: n,
			}
			migrationsNew = append(migrationsNew, mNew)
			continue
		}

		// check hash of all files up to version in FS against hash of most recent version in schema_migration_history
		// if they don't match, exit and tell the user they can't edit existing migrations
		content, err := schemaMigrations.ReadFile("schema/" + n)
		if err != nil {
			return err
		}
		if _, err := hasher.Write(content); err != nil {
			return err
		}
		checksum := hasher.Sum(nil)

		if !bytes.Equal(checksum, m.Checksum) {
			return fmt.Errorf("checksums for %s did not match:\n\twant: %x\n\thave: %x", n, m.Checksum, checksum)
		}
		log.Printf("migration %s validated", n)
	}

	installedRank := len(history) + 1

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO schema_migration_history
		(installed_rank, version, filename, checksum, execution_time_ms, installed_by)
		VALUES ($1, $2, $3, $4, $5, CURRENT_USER)
	`)
	if err != nil {
		return err
	}

	for _, m := range migrationsNew {
		content, err := schemaMigrations.ReadFile("schema/" + m.Filename)
		if err != nil {
			return err
		}
		startExec := time.Now()
		if !args.Init {
			if _, err := tx.ExecContext(ctx, string(content)); err != nil {
				return err
			}
		}
		endExec := time.Now()

		if _, err := hasher.Write(content); err != nil {
			return err
		}

		m.InstalledRank = installedRank
		m.Checksum = hasher.Sum(nil)

		if _, err := stmt.ExecContext(
			ctx,
			m.InstalledRank, m.Version, m.Filename, m.Checksum, endExec.Sub(startExec).Milliseconds(),
		); err != nil {
			return err
		}
		installedRank++
		log.Printf("migration %s applied", m.Filename)
	}

	repeat, err := repeatMigrations.ReadDir("repeat")
	if err != nil {
		return err
	}

	sort.Slice(repeat, func(i, j int) bool {
		return repeat[i].Name() < repeat[j].Name()
	})

	for _, f := range repeat {
		n := f.Name()
		content, err := repeatMigrations.ReadFile("repeat/" + n)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			return err
		}
		log.Printf("repeat migration %s applied", n)
	}

	return tx.Commit()
}

func (s migrationService) querySchemaHistory(ctx context.Context) ([]SchemaMigrationHistory, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT installed_rank, version, filename, checksum, installed_by, installed_on, execution_time_ms
		FROM schema_migration_history
		ORDER BY filename ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []SchemaMigrationHistory
	for rows.Next() {
		var h SchemaMigrationHistory
		if err := rows.Scan(&h.InstalledRank, &h.Version, &h.Filename, &h.Checksum, &h.InstalledBy, &h.InstalledOn, &h.ExecutionTimeMs); err != nil {
			return nil, err
		}
		history = append(history, h)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return history, nil
}

func sortFileBySemver(a, b fs.DirEntry) int {
	va, err := extractVersion(a.Name())
	if err != nil {
		log.Fatal("invalid version for %s", a.Name())
	}
	vb, err := extractVersion(b.Name())
	if err != nil {
		log.Fatal("invalid version for %s", b.Name())
	}

	o := va.Compare(vb)
	if o == 0 {
		log.Fatalf("duplicate versions for %s", vb.String())
	}
	return o
}

func extractVersion(s string) (*version.Version, error) {
	n, err := extractPrefix(s, "V", "__")
	if err != nil {
		return nil, err
	}

	v, err := version.NewVersion(n)
	if err != nil {
		return nil, fmt.Errorf("file %s could not be parsed: error %s; skipping...", s, err.Error())
	}
	return v, nil
}

func extractPrefix(s, beginDelim, endDelim string) (string, error) {
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
