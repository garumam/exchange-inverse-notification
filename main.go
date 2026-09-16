package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

const projectVersion = "v0.0.8"

func main() {
	db, err := NewDatabase()
	if err != nil {
		fmt.Printf("Erro ao conectar ao banco de dados: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	httpQueue := NewHTTPQueue(db)
	httpQueue.Start()

	manager := NewAccountManager(db)
	wsManager := NewWebSocketManager(db, manager)

	// Restaurar conexões ativas ao iniciar
	if err := wsManager.RestoreConnections(); err != nil {
		fmt.Printf("Erro ao restaurar conexões: %v\n", err)
	}

	scanner := bufio.NewScanner(os.Stdin)

	for {
		clearScreen()
		showMenu(wsManager)
		fmt.Print("Escolha uma opção: ")
		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			handleAddAccountMenu(manager, scanner)
		case "2":
			handleListAccounts(manager, wsManager, scanner)
		case "3":
			handleRemoveAccount(manager, wsManager, scanner)
		case "4":
			handleEditAccount(manager, wsManager, scanner)
		case "5":
			handleStartWebSocket(wsManager, scanner)
		case "6":
			handleStopWebSocket(wsManager, scanner)
		case "7":
			handleViewMonitoredAccounts(wsManager, scanner)
		case "8":
			handleViewLogs(wsManager.accountManager, scanner)
		case "9":
			handleManageSnapshots(wsManager.accountManager, db, scanner)
		case "10":
			handleManageHTTPSendQueue(db, scanner)
		case "11":
			fmt.Println("Saindo...")
			return
		default:
		}
	}
}

func clearScreen() {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	} else {
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func getMonitoredAccountsCount(wsManager *WebSocketManager) int {
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		return 0
	}

	count := 0
	for _, acc := range accounts {
		if wsManager.IsConnectionActive(acc.ID) {
			count++
		}
	}
	return count
}

func showMenu(wsManager *WebSocketManager) {
	monitoredCount := getMonitoredAccountsCount(wsManager)
	
	fmt.Printf("\n=== Gerenciador de Contas Bybit (%s) ===\n", projectVersion)
	fmt.Printf("📊 Contas sendo monitoradas: %d\n", monitoredCount)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("1. Cadastrar conta")
	fmt.Println("2. Listar contas cadastradas")
	fmt.Println("3. Remover conta cadastrada")
	fmt.Println("4. Editar conta")
	fmt.Println("5. Monitorar conta")
	fmt.Println("6. Parar monitoramento da conta")
	fmt.Println("7. Ver contas monitoradas")
	fmt.Println("8. Visualizar logs")
	fmt.Println("9. Gerenciar snapshots do banco")
	fmt.Println("10. Gerenciar fila de envios")
	fmt.Println("11. Desligar")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("ℹ️  Se a janela for fechada, o monitoramento será pausado")
	fmt.Println("   automaticamente.")
	fmt.Println()
}

// AuthField descreve um campo de autenticação por plataforma.
type AuthField struct {
	Label    string
	Key      string
	Required bool
}

// GetAuthOptionsBybit retorna as opções de autenticação para Bybit: API Key e API Secret.
func GetAuthOptionsBybit() []AuthField {
	return []AuthField{
		{Label: "API Key", Key: "api_key", Required: true},
		{Label: "API Secret", Key: "api_secret", Required: true},
	}
}

// GetAuthOptionsOKX retorna as opções de autenticação para OKX: API Key, API Secret e Passphrase.
func GetAuthOptionsOKX() []AuthField {
	return []AuthField{
		{Label: "API Key", Key: "api_key", Required: true},
		{Label: "API Secret", Key: "api_secret", Required: true},
		{Label: "Passphrase", Key: "passphrase", Required: true},
	}
}

// handleAddAccountMenu exibe escolha de plataforma e chama o fluxo correto de cadastro.
func handleAddAccountMenu(manager *AccountManager, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Cadastrar Conta ===")
	fmt.Println("Escolha a plataforma:")
	fmt.Println("1. Bybit")
	fmt.Println("2. OKX")
	fmt.Println("0. Voltar ao menu principal")
	fmt.Print("\nOpção: ")
	scanner.Scan()
	opt := strings.TrimSpace(scanner.Text())
	switch opt {
	case "0":
		return
	case "1":
		handleAddAccountBybit(manager, scanner)
		return
	case "2":
		handleAddAccountOKX(manager, scanner)
		return
	default:
		fmt.Println("Opção inválida.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleAddAccountBybit(manager *AccountManager, scanner *bufio.Scanner) {
	handleAddAccountCommon(manager, scanner, "bybit")
}

func handleAddAccountOKX(manager *AccountManager, scanner *bufio.Scanner) {
	handleAddAccountCommon(manager, scanner, "okx")
}

// escapeJSONString escapa aspas e barras invertidas para uso dentro de uma string JSON.
func escapeJSONString(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func handleAddAccountCommon(manager *AccountManager, scanner *bufio.Scanner, platform string) {
	clearScreen()
	title := "=== Cadastrar Conta Bybit ==="
	if platform == "okx" {
		title = "=== Cadastrar Conta OKX ==="
	}
	fmt.Println(title)
	fmt.Println("(Digite 'cancelar' ou '0' em qualquer momento para voltar ao menu principal)\n")

	fmt.Print("Nome da conta: ")
	scanner.Scan()
	nome := strings.TrimSpace(scanner.Text())
	if nome == "cancelar" || nome == "0" {
		return
	}

	var opts []AuthField
	if platform == "okx" {
		opts = GetAuthOptionsOKX()
	} else {
		opts = GetAuthOptionsBybit()
	}
	var apiKey, apiSecret, metadata string
	for _, f := range opts {
		fmt.Printf("%s: ", f.Label)
		scanner.Scan()
		val := strings.TrimSpace(scanner.Text())
		if val == "cancelar" || val == "0" {
			return
		}
		switch f.Key {
		case "api_key":
			apiKey = val
		case "api_secret":
			apiSecret = val
		case "passphrase":
			metadata = `{"passphrase":"` + escapeJSONString(val) + `"}`
		}
		if f.Required && val == "" {
			fmt.Printf("Erro: %s é obrigatório!\n", f.Label)
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
			return
		}
	}

	fmt.Print("Webhook Discord (opcional, deixe em branco para notificar no terminal): ")
	scanner.Scan()
	webhookURL := strings.TrimSpace(scanner.Text())
	if webhookURL == "cancelar" || webhookURL == "0" {
		return
	}
	if webhookURL != "" && !validateDiscordWebhookURL(webhookURL) {
		fmt.Println("URL inválida, campo ignorado.")
		webhookURL = ""
	}

	fmt.Print("Marcar @everyone em notificações de ordens? (sim/s ou não/n, padrão: não): ")
	scanner.Scan()
	markEveryoneOrderInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	markEveryoneOrder := markEveryoneOrderInput == "sim" || markEveryoneOrderInput == "s"
	if markEveryoneOrderInput == "cancelar" || markEveryoneOrderInput == "0" {
		return
	}

	fmt.Print("Marcar @everyone em notificações de carteira? (sim/s ou não/n, padrão: não): ")
	scanner.Scan()
	markEveryoneWalletInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	markEveryoneWallet := markEveryoneWalletInput == "sim" || markEveryoneWalletInput == "s"
	if markEveryoneWalletInput == "cancelar" || markEveryoneWalletInput == "0" {
		return
	}

	fmt.Print("Webhook Discord para execuções (opcional, deixe em branco para não usar): ")
	scanner.Scan()
	webhookURLExecutions := strings.TrimSpace(scanner.Text())
	if webhookURLExecutions == "cancelar" || webhookURLExecutions == "0" {
		return
	}
	if webhookURLExecutions != "" && !validateDiscordWebhookURL(webhookURLExecutions) {
		fmt.Println("URL inválida, campo ignorado.")
		webhookURLExecutions = ""
	}

	fmt.Print("Marcar @everyone em notificações de execuções? (sim/s ou não/n, padrão: não): ")
	scanner.Scan()
	markEveryoneExecutionInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	markEveryoneExecution := markEveryoneExecutionInput == "sim" || markEveryoneExecutionInput == "s"
	if markEveryoneExecutionInput == "cancelar" || markEveryoneExecutionInput == "0" {
		return
	}

	fmt.Print("Webhook URL Google Planilhas feito com google scripts (opcional, deixe em branco para não usar): ")
	scanner.Scan()
	webhookURLGoogleSheets := strings.TrimSpace(scanner.Text())
	if webhookURLGoogleSheets == "cancelar" || webhookURLGoogleSheets == "0" {
		return
	}
	if webhookURLGoogleSheets != "" && !validateGoogleSheetsWebhookURL(webhookURLGoogleSheets) {
		fmt.Println("URL inválida, campo ignorado.")
		webhookURLGoogleSheets = ""
	}

	var sheetURLGoogleSheets string
	if webhookURLGoogleSheets != "" {
		fmt.Print("URL da planilha do Google para receber os dados (obrigatório se webhook do google planilhas foi preenchido): ")
		scanner.Scan()
		sheetURLGoogleSheets = strings.TrimSpace(scanner.Text())
		if sheetURLGoogleSheets == "cancelar" || sheetURLGoogleSheets == "0" {
			return
		}

		if sheetURLGoogleSheets == "" {
			fmt.Println("Erro: URL da planilha do Google é obrigatória quando webhook URL é preenchida!")
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
			return
		}

		if !validateGoogleSheetsURL(sheetURLGoogleSheets) {
			fmt.Println("URL inválida, campo ignorado.")
			sheetURLGoogleSheets = ""
			webhookURLGoogleSheets = ""
		}
	}

	var sheetURLGoogleSheetsExecutions string
	if webhookURLGoogleSheets != "" {
		fmt.Print("URL da planilha do Google para execuções (opcional, mesma planilha ou outra): ")
		scanner.Scan()
		sheetURLGoogleSheetsExecutions = strings.TrimSpace(scanner.Text())
		if sheetURLGoogleSheetsExecutions == "cancelar" || sheetURLGoogleSheetsExecutions == "0" {
			return
		}
		if sheetURLGoogleSheetsExecutions != "" && !validateGoogleSheetsURL(sheetURLGoogleSheetsExecutions) {
			fmt.Println("URL inválida, campo ignorado.")
			sheetURLGoogleSheetsExecutions = ""
		}
	}

	fmt.Print("Delay de notificação para ordens, stops e execuções (0=desligado, 3-20 segundos, Enter=desligado): ")
	scanner.Scan()
	delayInput := strings.TrimSpace(scanner.Text())
	if delayInput == "cancelar" {
		return
	}
	notificationDelaySeconds := 0
	if delayInput != "" && delayInput != "0" {
		var d int
		if _, err := fmt.Sscanf(delayInput, "%d", &d); err == nil {
			if d == 0 {
				notificationDelaySeconds = 0
			} else if d >= 3 && d <= 20 {
				notificationDelaySeconds = d
			} else {
				fmt.Println("Erro: delay deve ser 0 ou um valor entre 3 e 20 segundos.")
				fmt.Println("\nPressione Enter para voltar ao menu principal...")
				scanner.Scan()
				return
			}
		} else {
			fmt.Println("Erro: valor inválido. Use 0 ou um número entre 3 e 20.")
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
			return
		}
	}

	if nome == "" || apiKey == "" || apiSecret == "" {
		fmt.Println("Erro: Nome, API Key e API Secret são obrigatórios!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := &BybitAccount{
		Name:                            nome,
		APIKey:                          apiKey,
		APISecret:                       apiSecret,
		WebhookURL:                      webhookURL,
		Active:                          true,
		MarkEveryoneOrder:               markEveryoneOrder,
		MarkEveryoneWallet:              markEveryoneWallet,
		WebhookURLGoogleSheets:          webhookURLGoogleSheets,
		SheetURLGoogleSheets:            sheetURLGoogleSheets,
		WebhookURLExecutions:            webhookURLExecutions,
		MarkEveryoneExecution:           markEveryoneExecution,
		SheetURLGoogleSheetsExecutions:  sheetURLGoogleSheetsExecutions,
		Platform:                        platform,
		Metadata:                        metadata,
		NotificationDelaySeconds:        notificationDelaySeconds,
	}

	if err := manager.AddAccount(account); err != nil {
		fmt.Printf("Erro ao cadastrar conta: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	} else {
		fmt.Println("Conta cadastrada com sucesso!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleListAccounts(manager *AccountManager, wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
	} else {
		for i, acc := range accounts {
			monitoringStatus := "Desligado"
			if wsManager.IsConnectionActive(acc.ID) {
				monitoringStatus = "Ligado"
			}
			platformLabel := acc.Platform
			if platformLabel == "" {
				platformLabel = "bybit"
			}
			fmt.Printf("\n%d. Nome: %s\n", i+1, acc.Name)
			fmt.Printf("   Plataforma: %s\n", platformLabel)
			fmt.Printf("   API Key: %s\n", maskAPIKey(acc.APIKey))
			if acc.WebhookURL != "" {
				fmt.Printf("   Webhook Discord: Configurado\n")
			} else {
				fmt.Printf("   Webhook Discord: Não configurado (notificações no terminal)\n")
			}
			if acc.WebhookURLGoogleSheets != "" && acc.SheetURLGoogleSheets != "" {
				fmt.Printf("   Webhook Google Planilhas: Configurado\n")
			} else {
				fmt.Printf("   Webhook Google Planilhas: Não configurado\n")
			}
			fmt.Printf("   Status: %s\n", getStatusText(acc.Active))
			fmt.Printf("   Monitoramento: %s\n", monitoringStatus)
			fmt.Printf("   Marcar @everyone em ordens: %s\n", getBooleanText(acc.MarkEveryoneOrder))
			fmt.Printf("   Marcar @everyone no balance da carteira: %s\n", getBooleanText(acc.MarkEveryoneWallet))
			if acc.WebhookURLExecutions != "" {
				fmt.Printf("   Webhook Discord execuções: Configurado\n")
			} else {
				fmt.Printf("   Webhook Discord execuções: Não configurado\n")
			}
			fmt.Printf("   Marcar @everyone em execuções: %s\n", getBooleanText(acc.MarkEveryoneExecution))
			if acc.SheetURLGoogleSheetsExecutions != "" {
				fmt.Printf("   Planilha Google execuções: Configurada\n")
			} else {
				fmt.Printf("   Planilha Google execuções: Não configurada\n")
			}
		}
		fmt.Printf("\nTotal: %d conta(s) cadastrada(s)\n", len(accounts))
	}
	
	fmt.Println("\nPressione Enter para voltar ao menu principal...")
	scanner.Scan()
}

func handleRemoveAccount(manager *AccountManager, wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para remover (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(scanner.Text(), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if index == 0 {
		return
	}

	if index < 1 || index > len(accounts) {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := accounts[index-1]
	
	// Verificar se a conta está sendo monitorada
	if wsManager.IsConnectionActive(account.ID) {
		clearScreen()
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("  ERRO: Não é possível remover a conta enquanto está")
		fmt.Println("        sendo monitorada!")
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Printf("\nA conta '%s' está sendo monitorada no momento.\n", account.Name)
		fmt.Println("\nPor favor:")
		fmt.Println("1. Volte ao menu principal")
		fmt.Println("2. Escolha a opção '5. Parar monitoramento da conta'")
		fmt.Println("3. Pare o monitoramento desta conta")
		fmt.Println("4. Depois tente remover novamente")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	// Pedir confirmação antes de remover
	clearScreen()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Printf("  ATENÇÃO: Você está prestes a remover a conta '%s'\n", account.Name)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("\nEsta ação não pode ser desfeita!")
	fmt.Print("\nDeseja realmente remover esta conta? (sim/s ou não/n): ")
	scanner.Scan()
	confirmation := strings.ToLower(strings.TrimSpace(scanner.Text()))

	if confirmation != "sim" && confirmation != "s" {
		fmt.Println("\nRemoção cancelada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	// Remover a conta
	if err := manager.RemoveAccount(account.ID); err != nil {
		fmt.Printf("\nErro ao remover conta: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	} else {
		fmt.Printf("\nConta '%s' removida com sucesso!\n", account.Name)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleEditAccount(manager *AccountManager, wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para editar (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(scanner.Text(), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if index == 0 {
		return
	}

	if index < 1 || index > len(accounts) {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := accounts[index-1]
	
	clearScreen()
	fmt.Println("=== Editar Conta ===")
	fmt.Printf("Conta: %s\n", account.Name)
	fmt.Println("(Digite 'cancelar' ou '0' em qualquer momento para voltar ao menu principal)\n")
	
	// Mostrar valores atuais
	fmt.Printf("Nome atual: %s\n", account.Name)
	fmt.Print("Novo nome (pressione Enter para manter o atual): ")
	scanner.Scan()
	newName := strings.TrimSpace(scanner.Text())
	if newName == "cancelar" || newName == "0" {
		return
	}
	if newName == "" {
		newName = account.Name
	}

	// Credenciais logo após o nome: usar opções da plataforma
	var editOpts []AuthField
	if account.Platform == "okx" {
		editOpts = GetAuthOptionsOKX()
	} else {
		editOpts = GetAuthOptionsBybit()
	}
	newApiKey := account.APIKey
	newApiSecret := account.APISecret
	var newMetadata string
	for _, f := range editOpts {
		fmt.Printf("\n%s atual: (configurado)\n", f.Label)
		fmt.Printf("Novo %s (pressione Enter para manter o atual): ", f.Label)
		scanner.Scan()
		val := strings.TrimSpace(scanner.Text())
		if val == "cancelar" || val == "0" {
			return
		}
		switch f.Key {
		case "api_key":
			if val != "" {
				newApiKey = val
			}
		case "api_secret":
			if val != "" {
				newApiSecret = val
			}
		case "passphrase":
			if val != "" {
				newMetadata = `{"passphrase":"` + escapeJSONString(val) + `"}`
			}
		}
	}

	currentWebhook := account.WebhookURL
	if currentWebhook == "" {
		currentWebhook = "(não configurado)"
	}
	fmt.Printf("\nWebhook Discord atual: %s\n", currentWebhook)
	fmt.Print("Novo Webhook Discord (pressione Enter para manter o atual, ou digite 'remover' para remover): ")
	scanner.Scan()
	newWebhook := strings.TrimSpace(scanner.Text())
	if newWebhook == "cancelar" || newWebhook == "0" {
		return
	}
	if newWebhook == "" {
		newWebhook = account.WebhookURL
	} else if newWebhook == "remover" {
		newWebhook = ""
	} else if !validateDiscordWebhookURL(newWebhook) {
		fmt.Println("URL inválida, campo ignorado.")
		newWebhook = ""
	}

	currentMarkEveryoneOrder := "Não"
	if account.MarkEveryoneOrder {
		currentMarkEveryoneOrder = "Sim"
	}
	fmt.Printf("\nMarcar @everyone em notificações de ordens atual: %s\n", currentMarkEveryoneOrder)
	fmt.Print("Novo valor (pressione Enter para manter o atual, sim/s para ativar, não/n para desativar): ")
	scanner.Scan()
	markEveryoneOrderInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if markEveryoneOrderInput == "cancelar" || markEveryoneOrderInput == "0" {
		return
	}
	newMarkEveryoneOrder := account.MarkEveryoneOrder
	if markEveryoneOrderInput != "" {
		newMarkEveryoneOrder = markEveryoneOrderInput == "sim" || markEveryoneOrderInput == "s"
	}

	currentMarkEveryoneWallet := "Não"
	if account.MarkEveryoneWallet {
		currentMarkEveryoneWallet = "Sim"
	}
	fmt.Printf("\nMarcar @everyone em notificações de carteira atual: %s\n", currentMarkEveryoneWallet)
	fmt.Print("Novo valor (pressione Enter para manter o atual, sim/s para ativar, não/n para desativar): ")
	scanner.Scan()
	markEveryoneWalletInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if markEveryoneWalletInput == "cancelar" || markEveryoneWalletInput == "0" {
		return
	}
	newMarkEveryoneWallet := account.MarkEveryoneWallet
	if markEveryoneWalletInput != "" {
		newMarkEveryoneWallet = markEveryoneWalletInput == "sim" || markEveryoneWalletInput == "s"
	}

	currentWebhookURLGoogleSheets := account.WebhookURLGoogleSheets
	if currentWebhookURLGoogleSheets == "" {
		currentWebhookURLGoogleSheets = "(não configurado)"
	}
	fmt.Printf("\nWebhook URL Google Planilhas atual: %s\n", currentWebhookURLGoogleSheets)
	fmt.Print("Novo Webhook URL Google Planilhas (pressione Enter para manter o atual, ou digite 'remover' para remover): ")
	scanner.Scan()
	newWebhookURLGoogleSheets := strings.TrimSpace(scanner.Text())
	if newWebhookURLGoogleSheets == "cancelar" || newWebhookURLGoogleSheets == "0" {
		return
	}
	if newWebhookURLGoogleSheets == "" {
		newWebhookURLGoogleSheets = account.WebhookURLGoogleSheets
	} else if newWebhookURLGoogleSheets == "remover" {
		newWebhookURLGoogleSheets = ""
	} else if !validateGoogleSheetsWebhookURL(newWebhookURLGoogleSheets) {
		fmt.Println("URL inválida, campo ignorado.")
		newWebhookURLGoogleSheets = ""
	}

	currentSheetURLGoogleSheets := account.SheetURLGoogleSheets
	if currentSheetURLGoogleSheets == "" {
		currentSheetURLGoogleSheets = "(não configurado)"
	}
	fmt.Printf("\nURL da planilha do Google atual: %s\n", currentSheetURLGoogleSheets)
	fmt.Print("Nova URL da planilha do Google (pressione Enter para manter o atual, ou digite 'remover' para remover): ")
	scanner.Scan()
	newSheetURLGoogleSheets := strings.TrimSpace(scanner.Text())
	if newSheetURLGoogleSheets == "cancelar" || newSheetURLGoogleSheets == "0" {
		return
	}
	if newSheetURLGoogleSheets == "" {
		newSheetURLGoogleSheets = account.SheetURLGoogleSheets
	} else if newSheetURLGoogleSheets == "remover" {
		newSheetURLGoogleSheets = ""
	} else if !validateGoogleSheetsURL(newSheetURLGoogleSheets) {
		fmt.Println("URL inválida, campo ignorado.")
		newSheetURLGoogleSheets = ""
	}

	// Validar que se webhook URL foi preenchida, sheet URL também deve estar preenchida
	if newWebhookURLGoogleSheets != "" && newSheetURLGoogleSheets == "" {
		fmt.Println("Erro: URL da planilha do Google é obrigatória quando webhook URL do google planilhas é preenchida!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	// Webhook Discord para execuções
	currentWebhookExecutions := account.WebhookURLExecutions
	if currentWebhookExecutions == "" {
		currentWebhookExecutions = "(não configurado)"
	}
	fmt.Printf("\nWebhook Discord para execuções atual: %s\n", currentWebhookExecutions)
	fmt.Print("Novo Webhook Discord para execuções (pressione Enter para manter, ou 'remover' para remover): ")
	scanner.Scan()
	newWebhookURLExecutions := strings.TrimSpace(scanner.Text())
	if newWebhookURLExecutions == "cancelar" || newWebhookURLExecutions == "0" {
		return
	}
	if newWebhookURLExecutions == "" {
		newWebhookURLExecutions = account.WebhookURLExecutions
	} else if newWebhookURLExecutions == "remover" {
		newWebhookURLExecutions = ""
	} else if !validateDiscordWebhookURL(newWebhookURLExecutions) {
		fmt.Println("URL inválida, campo ignorado.")
		newWebhookURLExecutions = ""
	}

	// Marcar @everyone em execuções
	currentMarkEveryoneExecution := "Não"
	if account.MarkEveryoneExecution {
		currentMarkEveryoneExecution = "Sim"
	}
	fmt.Printf("\nMarcar @everyone em notificações de execuções atual: %s\n", currentMarkEveryoneExecution)
	fmt.Print("Novo valor (pressione Enter para manter, sim/s ou não/n): ")
	scanner.Scan()
	markEveryoneExecutionInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	if markEveryoneExecutionInput == "cancelar" || markEveryoneExecutionInput == "0" {
		return
	}
	newMarkEveryoneExecution := account.MarkEveryoneExecution
	if markEveryoneExecutionInput != "" {
		newMarkEveryoneExecution = markEveryoneExecutionInput == "sim" || markEveryoneExecutionInput == "s"
	}

	// URL da planilha Google para execuções
	currentSheetURLExecutions := account.SheetURLGoogleSheetsExecutions
	if currentSheetURLExecutions == "" {
		currentSheetURLExecutions = "(não configurado)"
	}
	fmt.Printf("\nURL da planilha Google para execuções atual: %s\n", currentSheetURLExecutions)
	fmt.Print("Nova URL da planilha para execuções (pressione Enter para manter, ou 'remover'): ")
	scanner.Scan()
	newSheetURLGoogleSheetsExecutions := strings.TrimSpace(scanner.Text())
	if newSheetURLGoogleSheetsExecutions == "cancelar" || newSheetURLGoogleSheetsExecutions == "0" {
		return
	}
	if newSheetURLGoogleSheetsExecutions == "" {
		newSheetURLGoogleSheetsExecutions = account.SheetURLGoogleSheetsExecutions
	} else if newSheetURLGoogleSheetsExecutions == "remover" {
		newSheetURLGoogleSheetsExecutions = ""
	} else if !validateGoogleSheetsURL(newSheetURLGoogleSheetsExecutions) {
		fmt.Println("URL inválida, campo ignorado.")
		newSheetURLGoogleSheetsExecutions = ""
	}

	// Delay de notificação (0 = desligado, 3-20 segundos para agrupar)
	currentDelay := account.NotificationDelaySeconds
	currentDelayStr := "(desligado)"
	if currentDelay > 0 {
		currentDelayStr = fmt.Sprintf("%d segundos", currentDelay)
	}
	fmt.Printf("\nDelay de notificação atual (ordens, stops e execuções): %s\n", currentDelayStr)
	fmt.Print("Novo valor (0=desligado, 3-20 segundos, Enter para manter): ")
	scanner.Scan()
	delayInput := strings.TrimSpace(scanner.Text())
	if delayInput == "cancelar" {
		return
	}
	newNotificationDelaySeconds := account.NotificationDelaySeconds
	if delayInput != "" {
		var d int
		if _, err := fmt.Sscanf(delayInput, "%d", &d); err == nil {
			if d == 0 {
				newNotificationDelaySeconds = 0
			} else if d >= 3 && d <= 20 {
				newNotificationDelaySeconds = d
			} else {
				fmt.Println("Erro: delay deve ser 0 ou um valor entre 3 e 20 segundos.")
				fmt.Println("\nPressione Enter para voltar ao menu principal...")
				scanner.Scan()
				return
			}
		} else {
			fmt.Println("Erro: valor inválido. Use 0 ou um número entre 3 e 20.")
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
			return
		}
	}

	// Verificar se a conta está sendo monitorada antes de editar
	wasMonitored := wsManager.IsConnectionActive(account.ID)

	// Atualizar conta (newMetadata == "" mantém o metadata atual)
	if err := manager.UpdateAccount(account.ID, newName, newApiKey, newApiSecret, newWebhook, newMarkEveryoneOrder, newMarkEveryoneWallet, newWebhookURLGoogleSheets, newSheetURLGoogleSheets, newWebhookURLExecutions, newSheetURLGoogleSheetsExecutions, newMarkEveryoneExecution, newMetadata, newNotificationDelaySeconds); err != nil {
		fmt.Printf("\nErro ao editar conta: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	} else {
		fmt.Println("\nConta editada com sucesso!")
		
		// Se a conta estava sendo monitorada, reiniciar o monitoramento
		if wasMonitored {
			fmt.Println("\nReiniciando monitoramento para aplicar as alterações...")
			// Parar o monitoramento atual
			wsManager.StopConnection(account.ID)
			// Aguardar um pouco para garantir que a conexão foi fechada
			// Reiniciar o monitoramento com os dados atualizados
			if err := wsManager.StartConnection(account.ID); err != nil {
				fmt.Printf("Aviso: Erro ao reiniciar monitoramento: %v\n", err)
				fmt.Println("Por favor, reinicie o monitoramento manualmente.")
			} else {
				fmt.Println("Monitoramento reiniciado com sucesso!")
			}
		}
		
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleStartWebSocket(wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Printf("%d. Todas as contas\n", len(accounts)+1)
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para monitorar (ou 0 para voltar): ")
	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())
	var index int
	if _, err := fmt.Sscanf(input, "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		return
	}

	if index == 0 {
		return
	}

	if index == len(accounts)+1 {
		// Iniciar todas as contas
		if err := wsManager.StartAllConnections(); err != nil {
			fmt.Printf("Erro ao iniciar monitoramento: %v\n", err)
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
		} else {
			fmt.Println("Todas as contas estão sendo monitoradas!")
			fmt.Println("\nPressione Enter para ver as contas monitoradas...")
			scanner.Scan()
			handleViewMonitoredAccounts(wsManager, scanner)
		}
	} else if index >= 1 && index <= len(accounts) {
		// Iniciar conta específica
		account := accounts[index-1]
		if err := wsManager.StartConnection(account.ID); err != nil {
			fmt.Printf("Erro ao iniciar monitoramento: %v\n", err)
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
		} else {
			fmt.Printf("Monitoramento iniciado para conta '%s'!\n", account.Name)
			fmt.Println("\nPressione Enter para ver as contas monitoradas...")
			scanner.Scan()
			handleViewMonitoredAccounts(wsManager, scanner)
		}
	} else {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleStartAllWebSockets(wsManager *WebSocketManager) {
	if err := wsManager.StartAllConnections(); err != nil {
		fmt.Printf("Erro ao iniciar WebSockets: %v\n", err)
	} else {
		fmt.Println("Todos os WebSockets iniciados!")
	}
}

func handleStopWebSocket(wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	activeAccounts := []*BybitAccount{}
	for _, acc := range accounts {
		if wsManager.IsConnectionActive(acc.ID) {
			activeAccounts = append(activeAccounts, acc)
		}
	}

	if len(activeAccounts) == 0 {
		fmt.Println("Nenhuma conexão ativa.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Conexões Ativas ===")
	for i, acc := range activeAccounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Printf("%d. Todas as contas\n", len(activeAccounts)+1)
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para parar monitoramento (ou 0 para voltar): ")
	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())
	var index int
	if _, err := fmt.Sscanf(input, "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		return
	}

	if index == 0 {
		return
	}

	if index == len(activeAccounts)+1 {
		// Parar todas as contas
		wsManager.StopAll()
		fmt.Println("Monitoramento de todas as contas parado!")
		fmt.Println("\nPressione Enter para ver as contas monitoradas...")
		scanner.Scan()
		handleViewMonitoredAccounts(wsManager, scanner)
	} else if index >= 1 && index <= len(activeAccounts) {
		// Parar conta específica
		account := activeAccounts[index-1]
		wsManager.StopConnection(account.ID)
		fmt.Printf("Monitoramento parado para conta '%s'!\n", account.Name)
		fmt.Println("\nPressione Enter para ver as contas monitoradas...")
		scanner.Scan()
		handleViewMonitoredAccounts(wsManager, scanner)
	} else {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleStopAllWebSockets(wsManager *WebSocketManager) {
	wsManager.StopAll()
	fmt.Println("Todos os WebSockets parados!")
}

func maskAPIKey(key string) string {
	if len(key) <= 8 {
		return "****"
	}
	return key[:4] + "****" + key[len(key)-4:]
}

func getStatusText(active bool) string {
	if active {
		return "Ativa"
	}
	return "Inativa"
}

func getBooleanText(value bool) string {
	if value {
		return "Sim"
	}
	return "Não"
}

func handleViewMonitoredAccounts(wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	monitoredAccounts := []*BybitAccount{}
	for _, acc := range accounts {
		if wsManager.IsConnectionActive(acc.ID) {
			monitoredAccounts = append(monitoredAccounts, acc)
		}
	}

	fmt.Println("\n=== Contas Monitoradas (WebSocket Ativo) ===")
	if len(monitoredAccounts) == 0 {
		fmt.Println("Nenhuma conta está sendo monitorada no momento.")
	} else {
		for i, acc := range monitoredAccounts {
			fmt.Printf("\n%d. Nome: %s\n", i+1, acc.Name)
			fmt.Printf("   API Key: %s\n", maskAPIKey(acc.APIKey))
			if acc.WebhookURL != "" {
				fmt.Printf("   Webhook Discord: Configurado\n")
			} else {
				fmt.Printf("   Webhook Discord: Não configurado (notificações no terminal)\n")
			}
			if acc.WebhookURLGoogleSheets != "" && acc.SheetURLGoogleSheets != "" {
				fmt.Printf("   Webhook Google Planilhas: Configurado\n")
			} else {
				fmt.Printf("   Webhook Google Planilhas: Não configurado\n")
			}
			fmt.Printf("   Status: Monitorando\n")
			fmt.Printf("   Marcar @everyone em ordens: %s\n", getBooleanText(acc.MarkEveryoneOrder))
			fmt.Printf("   Marcar @everyone em carteira: %s\n", getBooleanText(acc.MarkEveryoneWallet))
			if acc.WebhookURLExecutions != "" {
				fmt.Printf("   Webhook Discord execuções: Configurado\n")
			} else {
				fmt.Printf("   Webhook Discord execuções: Não configurado\n")
			}
			fmt.Printf("   Marcar @everyone em execuções: %s\n", getBooleanText(acc.MarkEveryoneExecution))
			if acc.SheetURLGoogleSheetsExecutions != "" {
				fmt.Printf("   Planilha Google execuções: Configurada\n")
			} else {
				fmt.Printf("   Planilha Google execuções: Não configurada\n")
			}
		}
		fmt.Printf("\nTotal: %d conta(s) sendo monitorada(s)\n", len(monitoredAccounts))
	}
	
	fmt.Println("\nPressione Enter para voltar ao menu principal...")
	scanner.Scan()
}

func handleViewLogs(manager *AccountManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para visualizar logs (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(scanner.Text(), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		return
	}

	if index == 0 {
		return
	}

	if index < 1 || index > len(accounts) {
		fmt.Println("Número inválido!")
		return
	}

	account := accounts[index-1]
	
	clearScreen()
	fmt.Println("\n=== Opções de Visualização ===")
	fmt.Println("1. Ver últimas 1000 linhas")
	fmt.Println("2. Tail (seguir logs em tempo real)")
	fmt.Println("0. Voltar ao menu anterior")
	fmt.Print("Escolha uma opção: ")
	scanner.Scan()
	viewChoice := strings.TrimSpace(scanner.Text())

	if viewChoice == "0" {
		return
	}

	switch viewChoice {
	case "1":
		viewLogFile(account.ID, account.Name, scanner)
	case "2":
		viewLogTail(account.ID, account.Name, scanner)
	default:
	}
}

func viewLogFile(accountID int64, accountName string, scanner *bufio.Scanner) {
	clearScreen()
	lines, err := readLogFile(accountID, 1000)
	if err != nil {
		fmt.Printf("Erro ao ler arquivo de log: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu anterior...")
		scanner.Scan()
		return
	}

	if len(lines) == 0 {
		fmt.Printf("\nNenhum log encontrado para a conta '%s'.\n", accountName)
		fmt.Println("\nPressione Enter para voltar ao menu anterior...")
		scanner.Scan()
		return
	}

	fmt.Printf("\n=== Logs da conta '%s' (últimas %d linhas) ===\n\n", accountName, len(lines))
	for _, line := range lines {
		fmt.Println(line)
	}
	fmt.Println("\n=== Fim dos logs ===")
	fmt.Println("\nPressione Enter para voltar ao menu anterior...")
	scanner.Scan()
}

func viewLogTail(accountID int64, accountName string, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Printf("\n=== Tail dos logs da conta '%s' ===\n", accountName)
	fmt.Println("\n═══════════════════════════════════════════════════════════")
	fmt.Println("  Pressione ENTER para parar e voltar ao menu principal")
	fmt.Println("═══════════════════════════════════════════════════════════\n")

	stopChan := make(chan struct{})
	lineChan := make(chan string, 100)

	// Goroutine para ler input do usuário
	go func() {
		scanner.Scan()
		close(stopChan)
	}()

	// Goroutine para fazer tail do arquivo
	go func() {
		err := tailLogFile(accountID, stopChan, func(line string) {
			select {
			case lineChan <- line:
			case <-stopChan:
				return
			}
		})
		if err != nil {
			fmt.Printf("Erro ao fazer tail do log: %v\n", err)
		}
		close(lineChan)
	}()

	// Ler e exibir linhas
	for {
		select {
		case <-stopChan:
			fmt.Println("\n=== Parando visualização de logs ===\n")
			return
		case line, ok := <-lineChan:
			if !ok {
				return
			}
			fmt.Println(line)
		}
	}
}

func handleManageSnapshots(manager *AccountManager, db *Database, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}
	if len(accounts) == 0 {
		fmt.Println("=== Gerenciar Snapshots do Banco ===")
		fmt.Println("\nNenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("=== Gerenciar Snapshots do Banco ===")
	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		platformLabel := acc.Platform
		if platformLabel == "" {
			platformLabel = "bybit"
		}
		fmt.Printf("%d. %s (ID: %d, %s)\n", i+1, acc.Name, acc.ID, platformLabel)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta (ou 0 para voltar): ")
	scanner.Scan()
	var accountIndex int
	if _, err := fmt.Sscanf(strings.TrimSpace(scanner.Text()), "%d", &accountIndex); err != nil {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}
	if accountIndex == 0 {
		return
	}
	if accountIndex < 1 || accountIndex > len(accounts) {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := accounts[accountIndex-1]
	accountID := account.ID

	rows, err := db.ListLastMessageSnapshots(accountID)
	if err != nil {
		fmt.Printf("Erro ao listar snapshots: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}
	if len(rows) == 0 {
		fmt.Printf("\nNenhum snapshot encontrado para a conta '%s' (ID: %d).\n", account.Name, accountID)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Printf("\n=== Snapshots da conta '%s' (ID: %d) ===\n", account.Name, accountID)
	for i, row := range rows {
		preview := row.Message
		if len(preview) > 80 {
			preview = preview[:80] + "..."
		}
		fmt.Printf("%d. [%s] %s | %s | %s\n", i+1, row.MessageType, row.Symbol, row.UpdatedAt, preview)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da linha para excluir (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(strings.TrimSpace(scanner.Text()), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}
	if index == 0 {
		return
	}
	if index < 1 || index > len(rows) {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	selected := rows[index-1]
	fmt.Printf("\nExcluir snapshot [%s] %s? (s/n): ", selected.MessageType, selected.Symbol)
	scanner.Scan()
	if strings.ToLower(strings.TrimSpace(scanner.Text())) != "s" {
		fmt.Println("Operação cancelada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if err := db.DeleteLastMessageSnapshot(accountID, selected.MessageType, selected.Symbol); err != nil {
		fmt.Printf("Erro ao excluir snapshot: %v\n", err)
	} else {
		fmt.Println("Snapshot excluído com sucesso!")
	}
	fmt.Println("\nPressione Enter para voltar ao menu principal...")
	scanner.Scan()
}

func handleManageHTTPSendQueue(db *Database, scanner *bufio.Scanner) {
	for {
		clearScreen()
		fmt.Println("=== Gerenciar Fila de Envios ===")
		fmt.Println("1. Listar por type")
		fmt.Println("2. Ver linha completa")
		fmt.Println("3. Reenviar agora")
		fmt.Println("4. Editar URL da row")
		fmt.Println("5. Apagar permanentemente")
		fmt.Println("0. Voltar ao menu principal")
		fmt.Print("\nEscolha uma opção: ")
		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "0":
			return
		case "1":
			handleListHTTPSendQueue(db, scanner)
		case "2":
			handleViewHTTPSendQueueItem(db, scanner)
		case "3":
			handleResendHTTPSendQueueItem(db, scanner)
		case "4":
			handleEditHTTPSendQueueURL(db, scanner)
		case "5":
			handleHardDeleteHTTPSendQueueItem(db, scanner)
		default:
		}
	}
}

func promptHTTPSendQueueType(scanner *bufio.Scanner) (string, bool) {
	fmt.Println("Type:")
	fmt.Println("1. discord")
	fmt.Println("2. google_sheets")
	fmt.Println("0. Voltar")
	fmt.Print("Opção: ")
	scanner.Scan()
	opt := strings.TrimSpace(scanner.Text())
	switch opt {
	case "1":
		return HTTPSendTypeDiscord, true
	case "2":
		return HTTPSendTypeGoogleSheets, true
	default:
		return "", false
	}
}

func previewText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func handleListHTTPSendQueue(db *Database, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Listar fila de envios ===")
	sendType, ok := promptHTTPSendQueueType(scanner)
	if !ok {
		return
	}

	fmt.Print("Incluir soft-deleted? (s/n, padrão: s): ")
	scanner.Scan()
	includeInput := strings.ToLower(strings.TrimSpace(scanner.Text()))
	includeDeleted := includeInput != "n" && includeInput != "nao" && includeInput != "não"

	items, err := db.ListHTTPSendQueue(sendType, includeDeleted)
	if err != nil {
		fmt.Printf("Erro ao listar fila: %v\n", err)
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}

	if len(items) == 0 {
		fmt.Printf("\nNenhum item na fila type=%s.\n", sendType)
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}

	fmt.Printf("\n=== Itens type=%s (%d) ===\n", sendType, len(items))
	for _, item := range items {
		deleted := "ativo"
		if item.DeletedAt.Valid && item.DeletedAt.String != "" {
			deleted = "soft-deleted@" + item.DeletedAt.String
		}
		fmt.Printf("ID %d | account=%d | attempts=%d | created=%s | %s\n", item.ID, item.AccountID, item.Attempts, item.CreatedAt, deleted)
		fmt.Printf("  URL: %s\n", previewText(item.URL, 100))
		fmt.Printf("  Payload: %s\n", previewText(item.Payload, 80))
		if item.LastError != "" {
			fmt.Printf("  LastError: %s\n", previewText(item.LastError, 120))
		}
		fmt.Println()
	}
	fmt.Println("Pressione Enter para voltar...")
	scanner.Scan()
}

func promptHTTPSendQueueID(scanner *bufio.Scanner) (int64, bool) {
	fmt.Print("Digite o ID da row: ")
	scanner.Scan()
	var id int64
	if _, err := fmt.Sscanf(strings.TrimSpace(scanner.Text()), "%d", &id); err != nil || id <= 0 {
		fmt.Println("ID inválido!")
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return 0, false
	}
	return id, true
}

func handleViewHTTPSendQueueItem(db *Database, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Ver linha completa ===")
	id, ok := promptHTTPSendQueueID(scanner)
	if !ok {
		return
	}

	item, err := db.GetHTTPSendQueueItem(id)
	if err != nil {
		fmt.Printf("Erro ao buscar item: %v\n", err)
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}
	if item == nil {
		fmt.Printf("Item id=%d não encontrado.\n", id)
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}

	deletedAt := "(null)"
	if item.DeletedAt.Valid {
		deletedAt = item.DeletedAt.String
	}
	fmt.Printf("\nID: %d\n", item.ID)
	fmt.Printf("AccountID: %d\n", item.AccountID)
	fmt.Printf("Type: %s\n", item.Type)
	fmt.Printf("URL: %s\n", item.URL)
	fmt.Printf("Payload:\n%s\n", item.Payload)
	fmt.Printf("LastError:\n%s\n", item.LastError)
	fmt.Printf("Attempts: %d\n", item.Attempts)
	fmt.Printf("CreatedAt: %s\n", item.CreatedAt)
	fmt.Printf("DeletedAt: %s\n", deletedAt)
	fmt.Println("\nPressione Enter para voltar...")
	scanner.Scan()
}

func handleResendHTTPSendQueueItem(db *Database, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Reenviar agora ===")
	id, ok := promptHTTPSendQueueID(scanner)
	if !ok {
		return
	}

	fmt.Printf("Reenviando item id=%d...\n", id)
	if err := SendHTTPQueueItemNow(db, id); err != nil {
		fmt.Printf("Falha no reenvio: %v\n", err)
	} else {
		fmt.Println("Reenvio concluído com sucesso (item removido da fila).")
	}
	fmt.Println("\nPressione Enter para voltar...")
	scanner.Scan()
}

func handleEditHTTPSendQueueURL(db *Database, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Editar URL da row ===")
	id, ok := promptHTTPSendQueueID(scanner)
	if !ok {
		return
	}

	item, err := db.GetHTTPSendQueueItem(id)
	if err != nil {
		fmt.Printf("Erro ao buscar item: %v\n", err)
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}
	if item == nil {
		fmt.Printf("Item id=%d não encontrado.\n", id)
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}

	fmt.Printf("URL atual: %s\n", item.URL)
	fmt.Print("Nova URL: ")
	scanner.Scan()
	newURL := strings.TrimSpace(scanner.Text())
	if newURL == "" || newURL == "0" || newURL == "cancelar" {
		fmt.Println("Operação cancelada.")
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}

	if err := db.UpdateHTTPSendURL(id, newURL); err != nil {
		fmt.Printf("Erro ao atualizar URL: %v\n", err)
	} else {
		fmt.Println("URL atualizada com sucesso.")
	}
	fmt.Println("\nPressione Enter para voltar...")
	scanner.Scan()
}

func handleHardDeleteHTTPSendQueueItem(db *Database, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Apagar permanentemente ===")
	id, ok := promptHTTPSendQueueID(scanner)
	if !ok {
		return
	}

	fmt.Printf("Apagar permanentemente o item id=%d? (s/n): ", id)
	scanner.Scan()
	if strings.ToLower(strings.TrimSpace(scanner.Text())) != "s" {
		fmt.Println("Operação cancelada.")
		fmt.Println("\nPressione Enter para voltar...")
		scanner.Scan()
		return
	}

	if err := db.HardDeleteHTTPSend(id); err != nil {
		fmt.Printf("Erro ao apagar: %v\n", err)
	} else {
		fmt.Println("Item apagado permanentemente.")
	}
	fmt.Println("\nPressione Enter para voltar...")
	scanner.Scan()
}


