package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	HTTPSendTypeDiscord      = "discord"
	HTTPSendTypeGoogleSheets = "google_sheets"
)

// HTTPSendQueueItem representa uma linha da fila de envios HTTP.
type HTTPSendQueueItem struct {
	ID        int64
	AccountID int64
	Type      string
	URL       string
	Payload   string
	LastError string
	Attempts  int
	CreatedAt string
	DeletedAt sql.NullString
}

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
		one_way_mode INTEGER DEFAULT 1,
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

	// Fila de envios HTTP (Discord / Google Sheets). INTEGER PRIMARY KEY sem AUTOINCREMENT.
	createHTTPSendQueueTable := `
	CREATE TABLE IF NOT EXISTS http_send_queue (
		id INTEGER PRIMARY KEY,
		account_id INTEGER NOT NULL DEFAULT 0,
		type TEXT NOT NULL,
		url TEXT NOT NULL,
		payload TEXT NOT NULL,
		last_error TEXT NOT NULL DEFAULT '',
		attempts INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		deleted_at DATETIME NULL
	);`

	createHTTPSendQueueIndex := `
	CREATE INDEX IF NOT EXISTS idx_http_send_queue_type_deleted_id
	ON http_send_queue (type, deleted_at, id);`

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

	if _, err := d.db.Exec(createHTTPSendQueueTable); err != nil {
		return err
	}

	if _, err := d.db.Exec(createHTTPSendQueueIndex); err != nil {
		return err
	}

	// Adicionar novas colunas se não existirem
	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_order", "INTEGER DEFAULT 0"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_wallet", "INTEGER DEFAULT 0"); err != nil {
		return err
	}
	if err := d.addColumnIfNotExists("bybit_accounts", "one_way_mode", "INTEGER DEFAULT 1"); err != nil {
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
	if err := d.addColumnIfNotExists("bybit_accounts", "notification_delay_seconds", "INTEGER DEFAULT 0"); err != nil {
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
	return d.GetPositionSnapshotsByTypes(accountID, []string{"position"})
}

// GetPositionSnapshotsByTypes retorna snapshots de position filtrando por tipos de mensagem.
func (d *Database) GetPositionSnapshotsByTypes(accountID int64, messageTypes []string) ([]PositionSnapshotRow, error) {
	if len(messageTypes) == 0 {
		return []PositionSnapshotRow{}, nil
	}

	placeholders := make([]string, len(messageTypes))
	args := make([]interface{}, 0, len(messageTypes)+1)
	args = append(args, accountID)
	for i, messageType := range messageTypes {
		placeholders[i] = "?"
		args = append(args, messageType)
	}

	query := `SELECT symbol, message FROM last_message_snapshots WHERE account_id = ? AND message_type IN (` + strings.Join(placeholders, ",") + `)`
	rows, err := d.db.Query(
		query,
		args...,
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

// LastMessageSnapshotRow representa uma linha completa de snapshot para administração.
type LastMessageSnapshotRow struct {
	MessageType string
	Symbol      string
	Message     string
	UpdatedAt   string
}

// ListLastMessageSnapshots retorna todos os snapshots de uma conta.
func (d *Database) ListLastMessageSnapshots(accountID int64) ([]LastMessageSnapshotRow, error) {
	rows, err := d.db.Query(
		`SELECT message_type, symbol, message, updated_at FROM last_message_snapshots WHERE account_id = ? ORDER BY message_type, symbol`,
		accountID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []LastMessageSnapshotRow
	for rows.Next() {
		var r LastMessageSnapshotRow
		if err := rows.Scan(&r.MessageType, &r.Symbol, &r.Message, &r.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// DeleteLastMessageSnapshot remove um snapshot específico da conta.
func (d *Database) DeleteLastMessageSnapshot(accountID int64, messageType, symbol string) error {
	_, err := d.db.Exec(
		`DELETE FROM last_message_snapshots WHERE account_id = ? AND message_type = ? AND symbol = ?`,
		accountID, messageType, symbol,
	)
	return err
}

func validateHTTPSendQueueURL(sendType, url string) error {
	if url == "" {
		return fmt.Errorf("URL vazia")
	}
	switch sendType {
	case HTTPSendTypeDiscord:
		if !validateDiscordWebhookURL(url) {
			return fmt.Errorf("URL Discord inválida: %s", url)
		}
	case HTTPSendTypeGoogleSheets:
		if !validateGoogleSheetsWebhookURL(url) {
			return fmt.Errorf("URL Google Sheets inválida: %s", url)
		}
	default:
		if !isValidHTTPURL(url) {
			return fmt.Errorf("URL inválida: %s", url)
		}
	}
	return nil
}

// EnqueueHTTPSend valida a URL e insere um item na fila. Retorna erro sem salvar se a URL for inválida.
func (d *Database) EnqueueHTTPSend(accountID int64, sendType, url, payload string) (int64, error) {
	if err := validateHTTPSendQueueURL(sendType, url); err != nil {
		logHTTPQueue(accountID, "[http_send_queue] enqueue rejeitado (type=%s): %v", sendType, err)
		return 0, err
	}

	res, err := d.db.Exec(
		`INSERT INTO http_send_queue (account_id, type, url, payload, last_error, attempts, created_at, deleted_at)
		 VALUES (?, ?, ?, ?, '', 0, CURRENT_TIMESTAMP, NULL)`,
		accountID, sendType, url, payload,
	)
	if err != nil {
		logHTTPQueue(accountID, "[http_send_queue] erro ao inserir na fila (type=%s): %v", sendType, err)
		return 0, err
	}
	return res.LastInsertId()
}

// ClaimNextHTTPSend retorna o próximo item pendente do type (FIFO), ou nil se a fila estiver vazia.
func (d *Database) ClaimNextHTTPSend(sendType string) (*HTTPSendQueueItem, error) {
	row := d.db.QueryRow(
		`SELECT id, account_id, type, url, payload, last_error, attempts, created_at, deleted_at
		 FROM http_send_queue
		 WHERE type = ? AND deleted_at IS NULL
		 ORDER BY id ASC
		 LIMIT 1`,
		sendType,
	)
	var item HTTPSendQueueItem
	err := row.Scan(
		&item.ID, &item.AccountID, &item.Type, &item.URL, &item.Payload,
		&item.LastError, &item.Attempts, &item.CreatedAt, &item.DeletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// MarkHTTPSendSuccess remove permanentemente o item após envio bem-sucedido.
func (d *Database) MarkHTTPSendSuccess(id int64) error {
	_, err := d.db.Exec(`DELETE FROM http_send_queue WHERE id = ?`, id)
	return err
}

// MarkHTTPSendFailure incrementa attempts, grava last_error e soft-deleta se attempts > 20.
func (d *Database) MarkHTTPSendFailure(id int64, errText string) error {
	_, err := d.db.Exec(
		`UPDATE http_send_queue
		 SET attempts = attempts + 1,
		     last_error = ?,
		     deleted_at = CASE WHEN attempts + 1 > 20 THEN CURRENT_TIMESTAMP ELSE deleted_at END
		 WHERE id = ?`,
		errText, id,
	)
	return err
}

// ListHTTPSendQueue lista itens por type. Se includeDeleted for false, omite soft-deleted.
func (d *Database) ListHTTPSendQueue(sendType string, includeDeleted bool) ([]HTTPSendQueueItem, error) {
	query := `SELECT id, account_id, type, url, payload, last_error, attempts, created_at, deleted_at
	          FROM http_send_queue WHERE type = ?`
	if !includeDeleted {
		query += ` AND deleted_at IS NULL`
	}
	query += ` ORDER BY id ASC`

	rows, err := d.db.Query(query, sendType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []HTTPSendQueueItem
	for rows.Next() {
		var item HTTPSendQueueItem
		if err := rows.Scan(
			&item.ID, &item.AccountID, &item.Type, &item.URL, &item.Payload,
			&item.LastError, &item.Attempts, &item.CreatedAt, &item.DeletedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// GetHTTPSendQueueItem retorna um item pelo id.
func (d *Database) GetHTTPSendQueueItem(id int64) (*HTTPSendQueueItem, error) {
	row := d.db.QueryRow(
		`SELECT id, account_id, type, url, payload, last_error, attempts, created_at, deleted_at
		 FROM http_send_queue WHERE id = ?`,
		id,
	)
	var item HTTPSendQueueItem
	err := row.Scan(
		&item.ID, &item.AccountID, &item.Type, &item.URL, &item.Payload,
		&item.LastError, &item.Attempts, &item.CreatedAt, &item.DeletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// UpdateHTTPSendURL valida e atualiza a URL de um item da fila.
func (d *Database) UpdateHTTPSendURL(id int64, url string) error {
	item, err := d.GetHTTPSendQueueItem(id)
	if err != nil {
		return err
	}
	if item == nil {
		return fmt.Errorf("item id=%d não encontrado", id)
	}
	if err := validateHTTPSendQueueURL(item.Type, url); err != nil {
		return err
	}
	_, err = d.db.Exec(`UPDATE http_send_queue SET url = ? WHERE id = ?`, url, id)
	return err
}

// HardDeleteHTTPSend remove permanentemente um item da fila.
func (d *Database) HardDeleteHTTPSend(id int64) error {
	_, err := d.db.Exec(`DELETE FROM http_send_queue WHERE id = ?`, id)
	return err
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

