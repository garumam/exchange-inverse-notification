package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	db *sql.DB
}

func NewDatabase() (*Database, error) {
	// Usar caminho no diretório data para compatibilidade com Docker
	dbPath := "./bybit_accounts.db"
	if dataDir := getDataDir(); dataDir != "" {
		dbPath = filepath.Join(dataDir, "bybit_accounts.db")
	}
	
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	database := &Database{db: db}
	if err := database.initSchema(); err != nil {
		return nil, err
	}

	return database, nil
}

func (d *Database) Close() error {
	return d.db.Close()
}

func (d *Database) initSchema() error {
	// Tabela de contas Bybit
	createAccountsTable := `
	CREATE TABLE IF NOT EXISTS bybit_accounts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL DEFAULT '',
		api_key TEXT NOT NULL DEFAULT '',
		api_secret TEXT NOT NULL DEFAULT '',
		webhook_url TEXT NOT NULL DEFAULT '',
		webhook_url_google_sheets TEXT NOT NULL DEFAULT '',
		sheet_url_google_sheets TEXT NOT NULL DEFAULT '',
		mark_everyone_order INTEGER DEFAULT 0,
		mark_everyone_wallet INTEGER DEFAULT 0,
		active INTEGER DEFAULT 1,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	// Tabela de conexões ativas
	createConnectionsTable := `
	CREATE TABLE IF NOT EXISTS active_connections (
		account_id INTEGER PRIMARY KEY,
		connected INTEGER DEFAULT 1,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (account_id) REFERENCES bybit_accounts(id) ON DELETE CASCADE
	);`

	// Tabela de ordens
	createOrdersTable := `
	CREATE TABLE IF NOT EXISTS orders (
		order_id TEXT PRIMARY KEY,
		account_id INTEGER NOT NULL,
		order_data TEXT NOT NULL,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (account_id) REFERENCES bybit_accounts(id) ON DELETE CASCADE
	);`

	// Tabela de última mensagem por tipo (wallet/position), uma linha por account_id + message_type + symbol
	createLastMessageSnapshotsTable := `
	CREATE TABLE IF NOT EXISTS last_message_snapshots (
		account_id INTEGER NOT NULL,
		message_type TEXT NOT NULL,
		symbol TEXT NOT NULL,
		message TEXT NOT NULL,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (account_id, message_type, symbol),
		FOREIGN KEY (account_id) REFERENCES bybit_accounts(id) ON DELETE CASCADE
	);`

	if _, err := d.db.Exec(createAccountsTable); err != nil {
		return err
	}

	if _, err := d.db.Exec(createConnectionsTable); err != nil {
		return err
	}

	if _, err := d.db.Exec(createOrdersTable); err != nil {
		return err
	}

	if _, err := d.db.Exec(createLastMessageSnapshotsTable); err != nil {
		return err
	}

	// Adicionar novas colunas se não existirem
	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_order", "INTEGER DEFAULT 0"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_wallet", "INTEGER DEFAULT 0"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "webhook_url_google_sheets", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "sheet_url_google_sheets", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "webhook_url_executions", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_execution", "INTEGER DEFAULT 0"); err != nil {
		return err
	}
	if err := d.addColumnIfNotExists("bybit_accounts", "sheet_url_google_sheets_executions", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "platform", "TEXT NOT NULL DEFAULT 'bybit'"); err != nil {
		return err
	}
	if err := d.addColumnIfNotExists("bybit_accounts", "metadata", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}

	return nil
}

func (d *Database) GetDB() *sql.DB {
	return d.db
}

// SaveLastMessageSnapshot grava ou atualiza a última mensagem (wallet ou position) por account_id, tipo e símbolo.
func (d *Database) SaveLastMessageSnapshot(accountID int64, messageType, symbol, messageJSON string) error {
	_, err := d.db.Exec(
		`INSERT OR REPLACE INTO last_message_snapshots (account_id, message_type, symbol, message, updated_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		accountID, messageType, symbol, messageJSON,
	)
	return err
}

// WalletSnapshotRow representa uma linha de snapshot de wallet retornada do banco.
type WalletSnapshotRow struct {
	Symbol    string
	Message   string
	UpdatedAt string
}

// GetWalletSnapshotsUpdatedSince retorna snapshots de wallet atualizados desde since (para a conta).
func (d *Database) GetWalletSnapshotsUpdatedSince(accountID int64, since time.Time) ([]WalletSnapshotRow, error) {
	sinceStr := since.UTC().Format("2006-01-02 15:04:05")
	rows, err := d.db.Query(
		`SELECT symbol, message, updated_at FROM last_message_snapshots WHERE account_id = ? AND message_type = 'wallet' AND updated_at >= ? ORDER BY updated_at DESC`,
		accountID, sinceStr,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []WalletSnapshotRow
	for rows.Next() {
		var r WalletSnapshotRow
		if err := rows.Scan(&r.Symbol, &r.Message, &r.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// PositionSnapshotRow representa uma linha de snapshot de position.
type PositionSnapshotRow struct {
	Symbol  string
	Message string
}

// GetPositionSnapshots retorna os snapshots de position da conta (uma linha por símbolo, a mais recente).
func (d *Database) GetPositionSnapshots(accountID int64) ([]PositionSnapshotRow, error) {
	rows, err := d.db.Query(
		`SELECT symbol, message FROM last_message_snapshots WHERE account_id = ? AND message_type = 'position'`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []PositionSnapshotRow
	for rows.Next() {
		var r PositionSnapshotRow
		if err := rows.Scan(&r.Symbol, &r.Message); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// addColumnIfNotExists verifica se uma coluna existe na tabela e a adiciona se não existir
func (d *Database) addColumnIfNotExists(tableName, columnName, columnDefinition string) error {
	// Verificar se a coluna já existe usando PRAGMA table_info
	rows, err := d.db.Query("PRAGMA table_info(" + tableName + ")")
	if err != nil {
		return err
	}
	defer rows.Close()

	columnExists := false
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue interface{}
		var pk int

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}

		if name == columnName {
			columnExists = true
			break
		}
	}

	if !columnExists {
		alterTableSQL := "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnDefinition
		if _, err := d.db.Exec(alterTableSQL); err != nil {
			return err
		}
	}

	return nil
}

func getDataDir() string {
	// Verificar se existe variável de ambiente
	if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
		return dataDir
	}
	
	// Verificar se existe diretório ./data
	if _, err := os.Stat("./data"); err == nil {
		return "./data"
	}
	
	// Criar diretório data se não existir
	if err := os.MkdirAll("./data", 0755); err == nil {
		return "./data"
	}
	
	return ""
}

