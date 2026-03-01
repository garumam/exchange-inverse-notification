package main

import (
	"database/sql"
	"errors"
	"strings"
)

type BybitAccount struct {
	ID                            int64
	Name                          string
	APIKey                        string
	APISecret                     string
	WebhookURL                    string
	Active                        bool
	MarkEveryoneOrder             bool
	MarkEveryoneWallet            bool
	WebhookURLGoogleSheets        string
	SheetURLGoogleSheets          string
	WebhookURLExecutions          string
	MarkEveryoneExecution         bool
	SheetURLGoogleSheetsExecutions string
	Platform                      string // "bybit" ou "okx"
	Metadata                      string // JSON; OKX: {"passphrase":"..."}
}

type AccountManager struct {
	db *Database
}

func NewAccountManager(db *Database) *AccountManager {
	return &AccountManager{db: db}
}

func (am *AccountManager) AddAccount(account *BybitAccount) error {
	platform := strings.TrimSpace(account.Platform)
	if platform == "" {
		platform = "bybit"
	}
	metadata := account.Metadata
	if metadata == "" && platform == "okx" {
		metadata = "{}"
	}

	query := `INSERT INTO bybit_accounts (name, api_key, api_secret, webhook_url, active, mark_everyone_order, mark_everyone_wallet, webhook_url_google_sheets, sheet_url_google_sheets, webhook_url_executions, mark_everyone_execution, sheet_url_google_sheets_executions, platform, metadata) 
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	
	markEveryoneOrder := 0
	if account.MarkEveryoneOrder {
		markEveryoneOrder = 1
	}
	markEveryoneWallet := 0
	if account.MarkEveryoneWallet {
		markEveryoneWallet = 1
	}
	markEveryoneExecution := 0
	if account.MarkEveryoneExecution {
		markEveryoneExecution = 1
	}
	active := 0
	if account.Active {
		active = 1
	}
	
	_, err := am.db.GetDB().Exec(query, account.Name, account.APIKey, account.APISecret,
		account.WebhookURL, active, markEveryoneOrder, markEveryoneWallet,
		account.WebhookURLGoogleSheets, account.SheetURLGoogleSheets,
		account.WebhookURLExecutions, markEveryoneExecution, account.SheetURLGoogleSheetsExecutions,
		platform, metadata)
	return err
}

func (am *AccountManager) RemoveAccount(id int64) error {
	// Remove também a conexão ativa se existir
	_, err := am.db.GetDB().Exec("DELETE FROM active_connections WHERE account_id = ?", id)
	if err != nil {
		return err
	}

	_, err = am.db.GetDB().Exec("DELETE FROM bybit_accounts WHERE id = ?", id)
	return err
}

func (am *AccountManager) ListAccounts() ([]*BybitAccount, error) {
	query := `SELECT id, name, api_key, api_secret, webhook_url, active, mark_everyone_order, mark_everyone_wallet, webhook_url_google_sheets, sheet_url_google_sheets, webhook_url_executions, mark_everyone_execution, sheet_url_google_sheets_executions, platform, metadata 
	          FROM bybit_accounts ORDER BY id`
	
	rows, err := am.db.GetDB().Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*BybitAccount
	for rows.Next() {
		acc := &BybitAccount{}
		var active, markEveryoneOrder, markEveryoneWallet, markEveryoneExecution int
		err := rows.Scan(&acc.ID, &acc.Name, &acc.APIKey, &acc.APISecret,
			&acc.WebhookURL, &active, &markEveryoneOrder, &markEveryoneWallet,
			&acc.WebhookURLGoogleSheets, &acc.SheetURLGoogleSheets,
			&acc.WebhookURLExecutions, &markEveryoneExecution, &acc.SheetURLGoogleSheetsExecutions,
			&acc.Platform, &acc.Metadata)
		if err != nil {
			return nil, err
		}
		acc.Active = active == 1
		acc.MarkEveryoneOrder = markEveryoneOrder == 1
		acc.MarkEveryoneWallet = markEveryoneWallet == 1
		acc.MarkEveryoneExecution = markEveryoneExecution == 1
		if acc.Platform == "" {
			acc.Platform = "bybit"
		}
		accounts = append(accounts, acc)
	}

	return accounts, rows.Err()
}

func (am *AccountManager) GetAccount(id int64) (*BybitAccount, error) {
	query := `SELECT id, name, api_key, api_secret, webhook_url, active, mark_everyone_order, mark_everyone_wallet, webhook_url_google_sheets, sheet_url_google_sheets, webhook_url_executions, mark_everyone_execution, sheet_url_google_sheets_executions, platform, metadata 
	          FROM bybit_accounts WHERE id = ?`
	
	acc := &BybitAccount{}
	var active, markEveryoneOrder, markEveryoneWallet, markEveryoneExecution int
	err := am.db.GetDB().QueryRow(query, id).Scan(
		&acc.ID, &acc.Name, &acc.APIKey, &acc.APISecret,
		&acc.WebhookURL, &active, &markEveryoneOrder, &markEveryoneWallet,
		&acc.WebhookURLGoogleSheets, &acc.SheetURLGoogleSheets,
		&acc.WebhookURLExecutions, &markEveryoneExecution, &acc.SheetURLGoogleSheetsExecutions,
		&acc.Platform, &acc.Metadata)
	
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("conta não encontrada")
		}
		return nil, err
	}
	
	acc.Active = active == 1
	acc.MarkEveryoneOrder = markEveryoneOrder == 1
	acc.MarkEveryoneWallet = markEveryoneWallet == 1
	acc.MarkEveryoneExecution = markEveryoneExecution == 1
	if acc.Platform == "" {
		acc.Platform = "bybit"
	}
	return acc, nil
}

func (am *AccountManager) SetConnectionActive(accountID int64, active bool) error {
	if active {
		query := `INSERT OR REPLACE INTO active_connections (account_id, connected, updated_at) 
		          VALUES (?, 1, CURRENT_TIMESTAMP)`
		_, err := am.db.GetDB().Exec(query, accountID)
		return err
	} else {
		_, err := am.db.GetDB().Exec("DELETE FROM active_connections WHERE account_id = ?", accountID)
		return err
	}
}

func (am *AccountManager) GetActiveConnections() ([]int64, error) {
	query := `SELECT account_id FROM active_connections WHERE connected = 1`
	rows, err := am.db.GetDB().Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accountIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		accountIDs = append(accountIDs, id)
	}

	return accountIDs, rows.Err()
}

// UpdateAccount atualiza a conta. platform não é alterado. apiKey e apiSecret são persistidos; metadata pode ser passado para atualizar (ex.: passphrase OKX); use "" para manter o atual.
func (am *AccountManager) UpdateAccount(id int64, name, apiKey, apiSecret, webhookURL string, markEveryoneOrder, markEveryoneWallet bool, webhookURLGoogleSheets, sheetURLGoogleSheets, webhookURLExecutions, sheetURLGoogleSheetsExecutions string, markEveryoneExecution bool, metadata string) error {
	markEveryoneOrderInt := 0
	if markEveryoneOrder {
		markEveryoneOrderInt = 1
	}
	markEveryoneWalletInt := 0
	if markEveryoneWallet {
		markEveryoneWalletInt = 1
	}
	markEveryoneExecutionInt := 0
	if markEveryoneExecution {
		markEveryoneExecutionInt = 1
	}

	// Se metadata foi passado (não é o sentinel "keep"), atualizar; senão fazer UPDATE sem metadata
	if metadata != "" {
		query := `UPDATE bybit_accounts SET name = ?, api_key = ?, api_secret = ?, webhook_url = ?, mark_everyone_order = ?, mark_everyone_wallet = ?, webhook_url_google_sheets = ?, sheet_url_google_sheets = ?, webhook_url_executions = ?, mark_everyone_execution = ?, sheet_url_google_sheets_executions = ?, metadata = ? WHERE id = ?`
		_, err := am.db.GetDB().Exec(query, name, apiKey, apiSecret, webhookURL, markEveryoneOrderInt, markEveryoneWalletInt, webhookURLGoogleSheets, sheetURLGoogleSheets, webhookURLExecutions, markEveryoneExecutionInt, sheetURLGoogleSheetsExecutions, metadata, id)
		return err
	}
	query := `UPDATE bybit_accounts SET name = ?, api_key = ?, api_secret = ?, webhook_url = ?, mark_everyone_order = ?, mark_everyone_wallet = ?, webhook_url_google_sheets = ?, sheet_url_google_sheets = ?, webhook_url_executions = ?, mark_everyone_execution = ?, sheet_url_google_sheets_executions = ? WHERE id = ?`
	_, err := am.db.GetDB().Exec(query, name, apiKey, apiSecret, webhookURL, markEveryoneOrderInt, markEveryoneWalletInt, webhookURLGoogleSheets, sheetURLGoogleSheets, webhookURLExecutions, markEveryoneExecutionInt, sheetURLGoogleSheetsExecutions, id)
	return err
}

// Métodos para gerenciar ordens
func (am *AccountManager) SaveOrder(orderID string, accountID int64, orderDataJSON string) error {
	query := `INSERT OR REPLACE INTO orders (order_id, account_id, order_data) VALUES (?, ?, ?)`
	_, err := am.db.GetDB().Exec(query, orderID, accountID, orderDataJSON)
	return err
}

func (am *AccountManager) GetOrder(orderID string) (string, error) {
	var orderData string
	query := `SELECT order_data FROM orders WHERE order_id = ?`
	err := am.db.GetDB().QueryRow(query, orderID).Scan(&orderData)
	if err != nil {
		return "", err
	}
	return orderData, nil
}

func (am *AccountManager) DeleteOrder(orderID string) error {
	query := `DELETE FROM orders WHERE order_id = ?`
	_, err := am.db.GetDB().Exec(query, orderID)
	return err
}

