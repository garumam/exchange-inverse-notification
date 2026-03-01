package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const delayBufferMaxUniqueItems = 50
const orderGroupCreatedTimeWindowMs = 2000 // 2 segundos para agrupar ordens pelo createdTime

// delayNotificationItem representa um item na lista de notificações do buffer de delay.
type delayNotificationItem struct {
	UpdatedTime      int64
	NotificationType string     // "orders_group", "simple_order", "cancelled_order", "order_moved", "untriggered_stop", "deactivated_stop", "stop_moved"
	Data             []OrderData
	OldPrice         float64    // para order_moved e stop_moved
	NewPrice         float64    // para order_moved e stop_moved
}

type WebSocketManager struct {
	accountManager               *AccountManager
	db                           *Database
	connections      map[int64]*WebSocketConnection
	mu               sync.RWMutex
	walletNotificationBuffers map[int64]*WalletNotification
	delayBuffers     map[int64]*DelayNotificationBuffer
	bufferMu                     sync.RWMutex
}

// DelayNotificationBuffer acumula ordens, stops e execuções quando notification_delay_seconds > 0.
type DelayNotificationBuffer struct {
	orders      map[string][]OrderData // orderId -> versões ordenadas por updatedTime
	stops       map[string][]OrderData // orderId -> versões ordenadas por updatedTime
	executions  []ExecutionData
	timer       *time.Timer
	accountID   int64
	delaySec    int
	mu          sync.Mutex
}

type WalletNotification struct {
	discordTimer *time.Timer // 15 min: notificação Discord (wallet)
	sheetsTimer  *time.Timer // 2 min: notificação Google Sheets
	mu           sync.Mutex
	accountID    int64
}

type WebSocketConnection struct {
	AccountID  int64
	Account    *BybitAccount
	Conn       *websocket.Conn
	StopChan   chan struct{}
	Running    bool
	mu         sync.Mutex
}

type BybitOrderMessage struct {
	ID           string      `json:"id"`
	Topic        string      `json:"topic"`
	CreationTime int64       `json:"creationTime"`
	Data         []OrderData `json:"data"`
}

type BybitExecutionMessage struct {
	ID           string          `json:"id"`
	Topic        string          `json:"topic"`
	CreationTime int64           `json:"creationTime"`
	Data         []ExecutionData `json:"data"`
}

type BybitPositionMessage struct {
	ID           string         `json:"id"`
	Topic        string         `json:"topic"`
	CreationTime int64          `json:"creationTime"`
	Data         []PositionData `json:"data"`
}

type BybitWalletMessage struct {
	ID           string       `json:"id"`
	Topic        string       `json:"topic"`
	CreationTime int64        `json:"creationTime"`
	Data         []WalletData `json:"data"`
}

type WalletData struct {
	AccountType            string        `json:"accountType"`
	AccountIMRate          string        `json:"accountIMRate"`
	AccountMMRate          string        `json:"accountMMRate"`
	TotalEquity            string        `json:"totalEquity"`
	TotalWalletBalance     string        `json:"totalWalletBalance"`
	TotalMarginBalance     string        `json:"totalMarginBalance"`
	TotalPerpUPL           string        `json:"totalPerpUPL"`
	TotalInitialMargin     string        `json:"totalInitialMargin"`
	TotalMaintenanceMargin string        `json:"totalMaintenanceMargin"`
	Coin                   []CoinBalance `json:"coin"`
}

type ExecutionData struct {
	Category      string `json:"category"`
	Symbol        string `json:"symbol"`
	ExecType      string `json:"execType"`
	ExecPrice     string `json:"execPrice"`
	ExecQty       string `json:"execQty"`
	ExecValue     string `json:"execValue"`
	Side          string `json:"side"`
	OrderID       string `json:"orderId"`
	OrderLinkID   string `json:"orderLinkId"`
	OrderType     string `json:"orderType"`
	CreateType    string `json:"createType"`
	MarkPrice     string `json:"markPrice"`
	ExecTime      string `json:"execTime"` // timestamp da execução em ms (API Bybit)
}

type PositionData struct {
	Symbol          string `json:"symbol"`
	Side            string `json:"side"`
	Size            string `json:"size"`
	EntryPrice      string `json:"entryPrice"`
	MarkPrice       string `json:"markPrice"`
	PositionValue   string `json:"positionValue"`
	PositionIM      string `json:"positionIM"`
	PositionMM      string `json:"positionMM"`
	StopLoss        string `json:"stopLoss"`
	TakeProfit      string `json:"takeProfit"`
	Category        string `json:"category"`
	PositionStatus  string `json:"positionStatus"`
}

type CoinBalance struct {
	Coin            string `json:"coin"`
	Equity          string `json:"equity"`
	UsdValue        string `json:"usdValue"`
}

type OrderData struct {
	Category      string `json:"category"`
	OrderID       string `json:"orderId"`
	OrderLinkID   string `json:"orderLinkId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	OrderType     string `json:"orderType"`
	OrderStatus   string `json:"orderStatus"`
	CancelType    string `json:"cancelType"`
	RejectReason  string `json:"rejectReason"`
	Price         string `json:"price"`
	AvgPrice      string `json:"avgPrice"`
	Qty           string `json:"qty"`
	CreatedTime   string `json:"createdTime"`
	UpdatedTime   string `json:"updatedTime"`
	ReduceOnly    bool   `json:"reduceOnly"`
	StopOrderType string `json:"stopOrderType"`
	TriggerPrice  string `json:"triggerPrice"`
	CreateType    string `json:"createType"`
}

func NewWebSocketManager(db *Database, accountManager *AccountManager) *WebSocketManager {
	return &WebSocketManager{
		accountManager:   accountManager,
		db:               db,
		connections:      make(map[int64]*WebSocketConnection),
		walletNotificationBuffers: make(map[int64]*WalletNotification),
		delayBuffers:     make(map[int64]*DelayNotificationBuffer),
	}
}

// getDisplayPrice retorna o preço correto a ser exibido para uma ordem
// Se for Market e tiver avgPrice, usa avgPrice
// Se for Limit preenchido e tiver avgPrice, usa avgPrice
// Caso contrário, usa Price
func getDisplayPrice(order OrderData) string {
	// Se for Market e tiver avgPrice, usar avgPrice
	if order.OrderType == "Market" && order.AvgPrice != "" && order.AvgPrice != "0" {
		return order.AvgPrice
	}

	if order.OrderType == "Limit" && order.AvgPrice != "" && order.AvgPrice != "0" && (order.OrderStatus == "Filled" || order.OrderStatus == "PartiallyFilled") {
		return order.AvgPrice
	}
	// Caso contrário, usar Price
	return order.Price
}

// hasValidDisplayPrice retorna true se a ordem tem preço de exibição válido (> 0).
// Usado para evitar notificações com preço zerado (ex: Market sem avgPrice).
func hasValidDisplayPrice(order OrderData) bool {
	s := getDisplayPrice(order)
	if s == "" {
		return false
	}
	p, err := strconv.ParseFloat(s, 64)
	return err == nil && p != 0
}

// getCoinUsdValue retorna o UsdValue da Coin na wallet e true se encontrado e válido.
func getCoinUsdValue(wallet *WalletData, coin string) (float64, bool) {
	if wallet == nil {
		return 0, false
	}
	for _, c := range wallet.Coin {
		if c.Coin == coin {
			v, err := strconv.ParseFloat(c.UsdValue, 64)
			return v, err == nil && v > 0
		}
	}
	return 0, false
}

// orderPctOfWallet retorna a string de percentual (ex: " (12.5% do saldo em BTC)") ou "" se não houver wallet/coin/UsdValue.
func orderPctOfWallet(wallet *WalletData, symbol string, orderQtyUSD float64) string {
	coin := symbolToCoin(symbol)
	usdValue, ok := getCoinUsdValue(wallet, coin)
	if !ok || orderQtyUSD <= 0 {
		return ""
	}
	pct := (orderQtyUSD / usdValue) * 100
	return fmt.Sprintf(" (%.2f%% da carteira)", pct)
}

func (wsm *WebSocketManager) StartConnection(accountID int64) error {
	wsm.mu.Lock()
	defer wsm.mu.Unlock()

	if _, exists := wsm.connections[accountID]; exists {
		return fmt.Errorf("conexão já está ativa para esta conta")
	}

	account, err := wsm.accountManager.GetAccount(accountID)
	if err != nil {
		return err
	}

	wsConn := &WebSocketConnection{
		AccountID: accountID,
		Account:   account,
		StopChan:  make(chan struct{}),
		Running:   true,
	}

	wsm.connections[accountID] = wsConn

	// Marcar como ativa no banco
	if err := wsm.accountManager.SetConnectionActive(accountID, true); err != nil {
		// Erro silencioso - tentar novamente na próxima vez
	}

	// Iniciar conexão em goroutine com tratamento de panic
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Imprimir no stderr PRIMEIRO (antes de tentar qualquer coisa)
				fmt.Fprintf(os.Stderr, "\n=== ERRO FATAL ===\n")
				fmt.Fprintf(os.Stderr, "A aplicação encontrou um erro fatal ao iniciar o monitoramento da conta '%s' (ID: %d)\n", account.Name, accountID)
				fmt.Fprintf(os.Stderr, "Erro: %v\n", r)
				
				// Tentar logar o panic (mas não bloquear se falhar)
				func() {
					defer func() {
						if r2 := recover(); r2 != nil {
							fmt.Fprintf(os.Stderr, "ERRO ao tentar logar o panic: %v\n", r2)
						}
					}()
					logger, _ := getLogger(accountID, account.Name)
					if logger != nil {
						logger.Log("PANIC fatal em runConnection (goroutine): %v", r)
					}
				}()
				
				// Tentar obter caminho do log (usar padrão comum)
				var logPath string
				if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
					logPath = filepath.Join(dataDir, "logs", fmt.Sprintf("account_%d.log", accountID))
				} else {
					logPath = filepath.Join("data", "logs", fmt.Sprintf("account_%d.log", accountID))
				}
				fmt.Fprintf(os.Stderr, "Verifique os logs em: %s\n", logPath)
				fmt.Fprintf(os.Stderr, "==================\n\n")
			}
		}()
		
		wsm.runConnection(wsConn)
	}()

	return nil
}

func (wsm *WebSocketManager) StopConnection(accountID int64) {
	wsm.mu.Lock()
	defer wsm.mu.Unlock()

	conn, exists := wsm.connections[accountID]
	if !exists {
		return
	}

	conn.mu.Lock()
	if conn.Running {
		close(conn.StopChan)
		conn.Running = false
		if conn.Conn != nil {
			conn.Conn.Close()
		}
	}
	conn.mu.Unlock()

	delete(wsm.connections, accountID)

	// Limpar buffers
	wsm.bufferMu.Lock()
	if walletNotificationBuffer, exists := wsm.walletNotificationBuffers[accountID]; exists {
		walletNotificationBuffer.mu.Lock()
		if walletNotificationBuffer.discordTimer != nil {
			walletNotificationBuffer.discordTimer.Stop()
		}
		if walletNotificationBuffer.sheetsTimer != nil {
			walletNotificationBuffer.sheetsTimer.Stop()
		}
		walletNotificationBuffer.mu.Unlock()
		delete(wsm.walletNotificationBuffers, accountID)
	}
	if delayBuf, exists := wsm.delayBuffers[accountID]; exists {
		delayBuf.mu.Lock()
		if delayBuf.timer != nil {
			delayBuf.timer.Stop()
		}
		delayBuf.mu.Unlock()
		delete(wsm.delayBuffers, accountID)
	}
	wsm.bufferMu.Unlock()

	// Fechar logger
	closeLogger(accountID)

	// Remover do banco
	wsm.accountManager.SetConnectionActive(accountID, false)
}

func (wsm *WebSocketManager) StopAll() {
	wsm.mu.Lock()
	defer wsm.mu.Unlock()

	for accountID, conn := range wsm.connections {
		conn.mu.Lock()
		if conn.Running {
			close(conn.StopChan)
			conn.Running = false
			if conn.Conn != nil {
				conn.Conn.Close()
			}
		}
		conn.mu.Unlock()

		wsm.accountManager.SetConnectionActive(accountID, false)
	}

	wsm.connections = make(map[int64]*WebSocketConnection)
}

func (wsm *WebSocketManager) IsConnectionActive(accountID int64) bool {
	wsm.mu.RLock()
	defer wsm.mu.RUnlock()

	conn, exists := wsm.connections[accountID]
	return exists && conn.Running
}

func (wsm *WebSocketManager) StartAllConnections() error {
	accounts, err := wsm.accountManager.ListAccounts()
	if err != nil {
		return err
	}

	for _, account := range accounts {
		if account.Active {
			if err := wsm.StartConnection(account.ID); err != nil {
				// Erro já será logado pelo logger na função StartConnection
			}
		}
	}

	return nil
}

func (wsm *WebSocketManager) RestoreConnections() error {
	accountIDs, err := wsm.accountManager.GetActiveConnections()
	if err != nil {
		return err
	}

	for _, accountID := range accountIDs {
		if err := wsm.StartConnection(accountID); err != nil {
			// Erro já será logado pelo logger na função StartConnection
		} else {
			// Conexão restaurada - já será logado pelo logger
		}
	}

	return nil
}

func (wsm *WebSocketManager) runConnection(wsConn *WebSocketConnection) {
	// Capturar panics para evitar crash silencioso
	defer func() {
		if r := recover(); r != nil {
			// Imprimir no stderr PRIMEIRO
			fmt.Fprintf(os.Stderr, "[PANIC] runConnection para conta %d: %v\n", wsConn.AccountID, r)
			
			// Tentar logar o panic (mas não bloquear se falhar)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar o panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em runConnection: %v", r)
				}
			}()
			
			// Re-throw para que seja visível
			panic(r)
		}
	}()

	maxRetries := 999999 // Reconexão infinita
	retryDelay := time.Second * 5
	maxRetryDelay := time.Minute * 1
	initialRetryDelay := retryDelay

	logger, err := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if err != nil {
		// Se não conseguir criar logger, pelo menos imprimir no stderr
		fmt.Fprintf(os.Stderr, "ERRO: Não foi possível criar logger para conta %d: %v\n", wsConn.AccountID, err)
	}

	consecutiveFailures := 0
	maxConsecutiveFailures := 10 // Após 10 falhas consecutivas, fazer limpeza forçada

	for retry := 0; retry < maxRetries; retry++ {
		select {
		case <-wsConn.StopChan:
			return
		default:
		}

		// Limpar conexão antiga antes de tentar nova conexão
		wsConn.mu.Lock()
		if wsConn.Conn != nil {
			// Fechar conexão antiga silenciosamente
			wsConn.Conn.Close()
			wsConn.Conn = nil
		}
		wsConn.mu.Unlock()

		// Se houver muitas falhas consecutivas, fazer uma limpeza mais agressiva
		if consecutiveFailures >= maxConsecutiveFailures {
			if logger != nil {
				logger.Log("⚠️ Muitas falhas consecutivas (%d), fazendo limpeza forçada e aguardando antes de reconectar...", consecutiveFailures)
			}
			// Resetar delay e aguardar mais tempo
			retryDelay = initialRetryDelay
			consecutiveFailures = 0
			select {
			case <-wsConn.StopChan:
				return
			case <-time.After(30 * time.Second):
			}
		}

		// Canal para receber sinal de sucesso da conexão
		successChan := make(chan bool, 1)
		
		// Iniciar conexão em goroutine para poder receber o sinal de sucesso
		errChan := make(chan error, 1)
		go func() {
			errChan <- wsm.connectAndListen(wsConn, successChan)
		}()

		// Aguardar sinal de sucesso ou erro
		select {
		case <-wsConn.StopChan:
			return
		case success := <-successChan:
			if success {
				// Conexão estabelecida com sucesso - resetar contadores e delays
				consecutiveFailures = 0
				retryDelay = initialRetryDelay
				retry = -1 // Resetar para -1 para que após retry++ volte para 0
				// if logger != nil {
				// 	logger.Log("✅ Conexão estabelecida com sucesso, retry resetado")
				// }
			}
			// Continuar para aguardar erro da conexão (quando ela cair)
		case err := <-errChan:
			// Erro antes de estabelecer conexão
			if err != nil {
				// Verificar se foi parado manualmente
				select {
				case <-wsConn.StopChan:
					return
				default:
				}

				consecutiveFailures++
				if logger != nil {
					logger.Log("Erro na conexão WebSocket (tentativa %d, falhas consecutivas: %d): %v", retry+1, consecutiveFailures, err)
				}

				// Exponential backoff com limite máximo
				select {
				case <-wsConn.StopChan:
					return
				case <-time.After(retryDelay):
					if retryDelay < maxRetryDelay {
						retryDelay *= 2
					}
				}
				continue
			}
		}

		// Aguardar erro da conexão (quando ela cair)
		select {
		case <-wsConn.StopChan:
			return
		case err := <-errChan:
			if err != nil {
				// Verificar se foi parado manualmente
				select {
				case <-wsConn.StopChan:
					return
				default:
				}

				consecutiveFailures++
				if logger != nil {
					logger.Log("Erro na conexão WebSocket (tentativa %d, falhas consecutivas: %d): %v", retry+1, consecutiveFailures, err)
				}

				// Exponential backoff com limite máximo
				select {
				case <-wsConn.StopChan:
					return
				case <-time.After(retryDelay):
					if retryDelay < maxRetryDelay {
						retryDelay *= 2
					}
				}
			} else {
				// Conexão fechada normalmente, verificar se deve reconectar
				select {
				case <-wsConn.StopChan:
					return
				default:
					// Reconectar após um delay curto
					if logger != nil {
						logger.Log("Conexão fechada, tentando reconectar...")
					}
					time.Sleep(initialRetryDelay)
				}
			}
		}
	}
}

// connectAndListen despacha para a implementação da plataforma (Bybit ou OKX).
func (wsm *WebSocketManager) connectAndListen(wsConn *WebSocketConnection, successChan chan<- bool) (err error) {
	if wsConn.Account.Platform == "okx" {
		return wsm.connectAndListenOKX(wsConn, successChan)
	}
	return wsm.connectAndListenBybit(wsConn, successChan)
}

func (wsm *WebSocketManager) pingLoop(conn *websocket.Conn, stopChan chan struct{}) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "PANIC em pingLoop: %v\n", r)
		}
	}()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Verificar se o canal foi fechado antes de tentar escrever
			select {
			case <-stopChan:
				return
			default:
			}
			
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				// Erro ao enviar ping, a conexão será detectada no loop principal
				// Não fazer nada, apenas retornar para parar o loop
				return
			}
		}
	}
}

func (wsm *WebSocketManager) handleMessage(wsConn *WebSocketConnection, message []byte) {
	// Capturar panics para evitar crash silencioso
	defer func() {
		if r := recover(); r != nil {
			logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
			if logger != nil {
				logger.Log("PANIC em handleMessage: %v", r)
			} else {
				fmt.Fprintf(os.Stderr, "PANIC em handleMessage (conta %d): %v\n", wsConn.AccountID, r)
			}
		}
	}()

	logger, logErr := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logErr != nil {
		fmt.Fprintf(os.Stderr, "ERRO: Não foi possível criar logger em handleMessage para conta %d: %v\n", wsConn.AccountID, logErr)
	}

	// Tentar parsear como mensagem de controle primeiro
	var controlMsg map[string]interface{}
	if err := json.Unmarshal(message, &controlMsg); err == nil {
		if op, ok := controlMsg["op"].(string); ok {
			if op == "auth" {
				if logger != nil {
					logger.Log("[DEBUG] Resposta de autenticação: %v", controlMsg)
				}
				return
			}
			if op == "subscribe" {
				if success, ok := controlMsg["success"].(bool); ok && success {
					// if logger != nil {
					// 	logger.Log("✅ Inscrição nos tópicos confirmada!")
					// }
				} else {
					if logger != nil {
						logger.Log("⚠️ Inscrição pode ter falhado: %v", controlMsg)
					}
				}
				return
			}
			if op == "ping" || op == "pong" {
				// Pings/pongs são normais, não logar
				return
			}
		}
		// Se tem campo "topic", pode ser uma mensagem de dados
		if topic, ok := controlMsg["topic"].(string); ok {
			if logger != nil {
				logger.Log("[DEBUG] Mensagem com tópico recebida: topic=%s", topic)
			}
		}
	}

	// Tentar parsear como mensagem de order
	var orderMsg BybitOrderMessage
	if err := json.Unmarshal(message, &orderMsg); err == nil && orderMsg.Topic == "order" {
		wsm.handleOrderMessage(wsConn, orderMsg)
		return
	}

	// Tentar parsear como mensagem de execution
	var execMsg BybitExecutionMessage
	if err := json.Unmarshal(message, &execMsg); err == nil && execMsg.Topic == "execution" {
		wsm.handleExecutionMessage(wsConn, execMsg)
		return
	}

	// Tentar parsear como mensagem de position
	var posMsg BybitPositionMessage
	if err := json.Unmarshal(message, &posMsg); err == nil && posMsg.Topic == "position" {
		wsm.handlePositionMessage(wsConn, posMsg)
		return
	}

	// Tentar parsear como mensagem de wallet
	var walletMsg BybitWalletMessage
	if err := json.Unmarshal(message, &walletMsg); err == nil && walletMsg.Topic == "wallet" {
		wsm.handleWalletMessage(wsConn, walletMsg)
		return
	}
}

func (wsm *WebSocketManager) handleOrderMessage(wsConn *WebSocketConnection, orderMsg BybitOrderMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handleOrderMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handleOrderMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de order recebida! Total de ordens: %d", len(orderMsg.Data))
	}

	for _, orderData := range orderMsg.Data {
		if logger != nil {
			jsonData, _ := json.Marshal(orderData)
			logger.Log("[DEBUG] Processando ordem - Category: %s, Status: %s, Symbol: %s | JSON: %s",
				orderData.Category, orderData.OrderStatus, orderData.Symbol, string(jsonData))
		}

		// Processar apenas ordens inverse
		if orderData.Category != "inverse" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - não é inverse (category: %s)", orderData.Category)
			}
			continue
		}

		// Ignorar ordens com rejectReason diferente de EC_NoError
		// Isso evita processar mensagens duplicadas quando uma ordem é executada
		// e ao mesmo tempo há uma tentativa de cancelamento
		if orderData.RejectReason != "" && orderData.RejectReason != "EC_NoError" && orderData.RejectReason != "EC_PerCancelRequest" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - rejectReason diferente de EC_NoError: %s", orderData.RejectReason)
			}
			continue
		}

		if orderData.OrderStatus == "Untriggered" {
			wsm.addStopToDelayBuffer(wsConn.AccountID, orderData, wsConn)
			continue
		}
		if orderData.OrderStatus == "Deactivated" {
			wsm.addStopToDelayBuffer(wsConn.AccountID, orderData, wsConn)
			continue
		}
		if orderData.CreateType == "CreateByStopOrder" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - CreateByStopOrder (status: %s)", orderData.OrderStatus)
			}
			continue
		}
		// Todas as ordens (New, Filled, Cancelled, etc.) vão para o buffer de delay
		wsm.addOrderToDelayBuffer(wsConn.AccountID, orderData, wsConn)

	}
}

// formatOrderGroupMessage formata uma mensagem para um grupo de ordens (uma ou várias). Usado por processDelayBuffer.
// wallet: última wallet da conta (pode ser nil); se tiver Coin da moeda da ordem, inclui % em relação ao UsdValue da Coin.
func formatOrderGroupMessage(wallet *WalletData, groupOrders []OrderData) string {
	if len(groupOrders) == 0 {
		return ""
	}
	firstOrder := groupOrders[0]
	reducePrefix := ""
	if firstOrder.ReduceOnly {
		reducePrefix = "Reduce "
	}
	var minPrice, maxPrice float64
	var totalQty float64
	var coinQty float64 // para preço médio ponderado: soma(preço * qty)
	for i, order := range groupOrders {
		priceStr := getDisplayPrice(order)
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			continue
		}
		qty, err := strconv.ParseFloat(order.Qty, 64)
		if err != nil {
			continue
		}
		if i == 0 {
			minPrice, maxPrice = price, price
		} else {
			if price < minPrice {
				minPrice = price
			}
			if price > maxPrice {
				maxPrice = price
			}
		}
		totalQty += qty
		coinQty += qty / price
	}
	var avgPrice float64
	if totalQty > 0 {
		avgPrice = totalQty / coinQty
	}
	pctSuffix := orderPctOfWallet(wallet, firstOrder.Symbol, totalQty)
	displayPrice := getDisplayPrice(firstOrder)
	var orderIcon string
	if firstOrder.Side == "Buy" {
		orderIcon = "🟢"
	} else {
		orderIcon = "🔴"
	}
	if len(groupOrders) == 1 {
		return fmt.Sprintf("%s Nova ordem aberta - %s %s%s %s @ %s (Qty: %s USD)%s",
			orderIcon, firstOrder.Symbol, reducePrefix, firstOrder.Side, firstOrder.OrderType, displayPrice, formatPriceCoin(totalQty), pctSuffix)
	}
	if minPrice == maxPrice {
		return fmt.Sprintf("%s %d ordens %s%s %s agrupadas - %s @ %s (Qty Total: %s USD)%s",
			orderIcon, len(groupOrders), reducePrefix, firstOrder.Side, firstOrder.OrderType, firstOrder.Symbol, displayPrice, formatPriceCoin(totalQty), pctSuffix)
	}

	return fmt.Sprintf("%s %d ordens %s%s %s agrupadas - %s\n   Range: %s até %s (Preço médio: %s)\n   Qty Total: %s USD%s",
		orderIcon, len(groupOrders), reducePrefix, firstOrder.Side, firstOrder.OrderType, firstOrder.Symbol,
		formatPriceCoin(minPrice), formatPriceCoin(maxPrice), formatPriceCoin(avgPrice), formatPriceCoin(totalQty), pctSuffix)
}

// formatOrderMovedMessage formata mensagem de ordem movida (preço alterado). Usado por processDelayBuffer.
func formatOrderMovedMessage(order OrderData, oldPrice, newPrice float64, wallet *WalletData) string {
	reducePrefix := ""
	if order.ReduceOnly {
		reducePrefix = "Reduce "
	}
	var orderIcon string
	if order.Side == "Buy" {
		orderIcon = "🟢"
	} else {
		orderIcon = "🔴"
	}
	qty, _ := strconv.ParseFloat(order.Qty, 64)
	pctSuffix := orderPctOfWallet(wallet, order.Symbol, qty)
	return fmt.Sprintf("📝 %s Ordem movida - %s %s%s %s\n   Preço: %s → %s (Qty: %s USD)%s",
		orderIcon, order.Symbol, reducePrefix, order.Side, order.OrderType, formatPriceCoin(oldPrice), formatPriceCoin(newPrice), formatPriceCoin(qty), pctSuffix)
}

// formatCancelMessage formata mensagem de cancelamentos agrupados.
func formatCancelMessage(orders []OrderData) string {
	if len(orders) == 0 {
		return ""
	}
	parts := []string{fmt.Sprintf("❌ %d ordens canceladas:", len(orders))}
	for _, order := range orders {
		reducePrefix := ""
		if order.ReduceOnly {
			reducePrefix = "Reduce "
		}
		displayPrice := getDisplayPrice(order)
		parts = append(parts, fmt.Sprintf("  • %s %s%s %s @ %s",
			order.Symbol, reducePrefix, order.Side, order.OrderType, displayPrice))
	}
	return strings.Join(parts, "\n")
}

// formatStopOrderMessage formata mensagem de stop Untriggered.
func formatStopOrderMessage(order OrderData, wallet *WalletData) string {
	reducePrefix := ""
	if order.ReduceOnly {
		reducePrefix = "Reduce "
	}

	// Converter triggerPrice e qty para float para formatação
	triggerPrice, err := strconv.ParseFloat(order.TriggerPrice, 64)
	if err != nil {
		triggerPrice = 0
	}

	qty, err := strconv.ParseFloat(order.Qty, 64)
	if err != nil {
		qty = 0
	}

	formattedQty := formatPriceCoin(qty)
	mensagemQty := "(Qty: " + formattedQty + " USD)"
	if formattedQty == "0" {
		mensagemQty = "(Qty: 100% da posição)"
	}
	pctSuffix := orderPctOfWallet(wallet, order.Symbol, qty)
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢"
	} else {
		stopIcon = "🔴"
	}
	return fmt.Sprintf("%s Stop %s%s %s - %s @ %s %s%s",
		stopIcon, reducePrefix, order.Side, order.OrderType, order.Symbol, formatPriceCoin(triggerPrice), mensagemQty, pctSuffix)
}

// formatStopMovedMessage formata mensagem de stop movido (trigger price alterado). Usado por processDelayBuffer.
func formatStopMovedMessage(order OrderData, oldPrice, newPrice float64, wallet *WalletData) string {
	reducePrefix := ""
	if order.ReduceOnly {
		reducePrefix = "Reduce "
	}
	qty, _ := strconv.ParseFloat(order.Qty, 64)
	formattedQty := formatPriceCoin(qty)
	mensagemQty := "(Qty: " + formattedQty + " USD)"
	if formattedQty == "0" {
		mensagemQty = "(Qty: 100% da posição)"
	}
	pctSuffix := orderPctOfWallet(wallet, order.Symbol, qty)
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢"
	} else {
		stopIcon = "🔴"
	}
	return fmt.Sprintf("📝 %s Stop movido - %s %s%s %s\n   Preço: %s → %s %s%s",
		stopIcon, order.Symbol, reducePrefix, order.Side, order.OrderType, formatPriceCoin(oldPrice), formatPriceCoin(newPrice), mensagemQty, pctSuffix)
}

// formatStopCancellationMessage formata mensagem de stop cancelado (Deactivated).
func formatStopCancellationMessage(order OrderData) string {
	reducePrefix := ""
	if order.ReduceOnly {
		reducePrefix = "Reduce "
	}

	// Converter triggerPrice e qty para float para formatação
	triggerPrice, err := strconv.ParseFloat(order.TriggerPrice, 64)
	if err != nil {
		triggerPrice = 0
	}

	qty, err := strconv.ParseFloat(order.Qty, 64)
	if err != nil {
		qty = 0
	}

	formattedQty := formatPriceCoin(qty)
	mensagemQty := "(Qty: " + formattedQty + " USD)"
	if formattedQty == "0" {
		mensagemQty = "(Qty: 100% da posição)"
	}
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢"
	} else {
		stopIcon = "🔴"
	}
	return fmt.Sprintf("❌ %s Stop %s%s %s **CANCELADO** - %s @ %s %s",
		stopIcon, reducePrefix, order.Side, order.OrderType, order.Symbol, formatPriceCoin(triggerPrice), mensagemQty)
}

// sortOrderVersionsByUpdatedTime ordena in-place por updatedTime (ms) crescente.
func sortOrderVersionsByUpdatedTime(versions []OrderData) {
	if len(versions) <= 1 {
		return
	}
	// sort.Slice
	for i := 0; i < len(versions); i++ {
		for j := i + 1; j < len(versions); j++ {
			ti, _ := strconv.ParseInt(versions[i].UpdatedTime, 10, 64)
			tj, _ := strconv.ParseInt(versions[j].UpdatedTime, 10, 64)
			if ti > tj {
				versions[i], versions[j] = versions[j], versions[i]
			}
		}
	}
}

func (wsm *WebSocketManager) getOrCreateDelayBuffer(accountID int64, delaySec int) *DelayNotificationBuffer {
	buf, exists := wsm.delayBuffers[accountID]
	if !exists {
		buf = &DelayNotificationBuffer{
			orders:     make(map[string][]OrderData),
			stops:      make(map[string][]OrderData),
			executions: []ExecutionData{},
			accountID:  accountID,
			delaySec:   delaySec,
		}
		wsm.delayBuffers[accountID] = buf
	}
	return buf
}

func (wsm *WebSocketManager) addOrderToDelayBuffer(accountID int64, order OrderData, wsConn *WebSocketConnection) {
	delaySec := wsConn.Account.NotificationDelaySeconds
	if delaySec <= 0 {
		delaySec = 2
	}
	wsm.bufferMu.Lock()
	buf := wsm.getOrCreateDelayBuffer(accountID, delaySec)
	wsm.bufferMu.Unlock()

	buf.mu.Lock()
	buf.orders[order.OrderID] = append(buf.orders[order.OrderID], order)
	sortOrderVersionsByUpdatedTime(buf.orders[order.OrderID])
	uniqueCount := len(buf.orders) + len(buf.stops) + len(buf.executions)
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	if uniqueCount >= delayBufferMaxUniqueItems {
		buf.mu.Unlock()
		go wsm.processDelayBuffer(accountID, wsConn)
		return
	}
	buf.timer = time.AfterFunc(time.Duration(delaySec)*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processDelayBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processDelayBuffer(accountID, wsConn)
	})
	buf.mu.Unlock()
}

func (wsm *WebSocketManager) addStopToDelayBuffer(accountID int64, order OrderData, wsConn *WebSocketConnection) {
	delaySec := wsConn.Account.NotificationDelaySeconds
	if delaySec <= 0 {
		delaySec = 2
	}
	wsm.bufferMu.Lock()
	buf := wsm.getOrCreateDelayBuffer(accountID, delaySec)
	wsm.bufferMu.Unlock()

	buf.mu.Lock()
	buf.stops[order.OrderID] = append(buf.stops[order.OrderID], order)
	sortOrderVersionsByUpdatedTime(buf.stops[order.OrderID])
	uniqueCount := len(buf.orders) + len(buf.stops) + len(buf.executions)
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	if uniqueCount >= delayBufferMaxUniqueItems {
		buf.mu.Unlock()
		go wsm.processDelayBuffer(accountID, wsConn)
		return
	}
	buf.timer = time.AfterFunc(time.Duration(delaySec)*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processDelayBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processDelayBuffer(accountID, wsConn)
	})
	buf.mu.Unlock()
}

func (wsm *WebSocketManager) addExecutionToDelayBuffer(accountID int64, exec ExecutionData, wsConn *WebSocketConnection) {
	delaySec := wsConn.Account.NotificationDelaySeconds
	if delaySec <= 0 {
		delaySec = 2
	}
	wsm.bufferMu.Lock()
	buf := wsm.getOrCreateDelayBuffer(accountID, delaySec)
	wsm.bufferMu.Unlock()

	buf.mu.Lock()
	buf.executions = append(buf.executions, exec)
	uniqueCount := len(buf.orders) + len(buf.stops) + len(buf.executions)
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	if uniqueCount >= delayBufferMaxUniqueItems {
		buf.mu.Unlock()
		go wsm.processDelayBuffer(accountID, wsConn)
		return
	}
	buf.timer = time.AfterFunc(time.Duration(delaySec)*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processDelayBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processDelayBuffer(accountID, wsConn)
	})
	buf.mu.Unlock()
}

// processDelayBuffer processa o buffer de delay (cópia, reset, depois regras 1-10). Deve ser chamado com buffer já liberado.
func (wsm *WebSocketManager) processDelayBuffer(accountID int64, wsConn *WebSocketConnection) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processDelayBuffer para conta %d: %v\n", accountID, r)
		}
	}()

	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	wsConn = conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buf, exists := wsm.delayBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}
	buf.mu.Lock()
	// 1. Cópia e reset
	ordersCopy := make(map[string][]OrderData)
	for k, v := range buf.orders {
		vers := make([]OrderData, len(v))
		copy(vers, v)
		ordersCopy[k] = vers
	}
	stopsCopy := make(map[string][]OrderData)
	for k, v := range buf.stops {
		vers := make([]OrderData, len(v))
		copy(vers, v)
		stopsCopy[k] = vers
	}
	executionsCopy := make([]ExecutionData, len(buf.executions))
	copy(executionsCopy, buf.executions)
	buf.orders = make(map[string][]OrderData)
	buf.stops = make(map[string][]OrderData)
	buf.executions = nil
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	buf.mu.Unlock()
	wsm.bufferMu.Unlock()

	// Regra 1: ordens preparadas + movedOrderIDs + movedOrderPrices (para formatOrderMovedMessage no delay buffer)
	var preparedOrders []OrderData
	movedOrderIDs := make(map[string]bool)
	movedOrderPrices := make(map[string]struct{ Old, New float64 })
	for _, versions := range ordersCopy {
		sortOrderVersionsByUpdatedTime(versions)

		oldest, newest := versions[0], versions[len(versions)-1]

		existingOrderJSON, _ := wsm.accountManager.GetOrder(oldest.OrderID)
		var existingOrder OrderData
		hasExistingOrder := false
		if existingOrderJSON != "" {
			if json.Unmarshal([]byte(existingOrderJSON), &existingOrder) == nil {
				hasExistingOrder = true
			}
		}

		if oldest.OrderStatus == "New" && newest.OrderStatus == "Cancelled" {
			if hasExistingOrder {
				// caso onde foi cancelado mas antes moveu a ordem por algum motivo, notificar a ordem movida e o cancelamento
				prevOrder := versions[len(versions)-2]
				if getDisplayPrice(existingOrder) != getDisplayPrice(prevOrder) {
					if o, errO := strconv.ParseFloat(getDisplayPrice(existingOrder), 64); errO == nil {
						if n, errN := strconv.ParseFloat(getDisplayPrice(prevOrder), 64); errN == nil {
							movedOrderIDs[prevOrder.OrderID] = true
							movedOrderPrices[prevOrder.OrderID] = struct{ Old, New float64 }{o, n}
							preparedOrders = append(preparedOrders, prevOrder)
						}
					}
				}
				preparedOrders = append(preparedOrders, newest)
				continue
			} else {
				continue
			}
		}

		// Processar abertura de ordem ou cancelamento
		// Verificar se é Limit executada rapidamente (até 3 segundos entre criação e atualização)
		// Verificar se a ordem Limit foi movida para outro preço
		if newest.OrderType == "Limit" && (newest.OrderStatus == "Filled" || newest.OrderStatus == "PartiallyFilled") {
			isLimitExecutedQuickly := false
			isLimitMoved := false

			createdTime, err1 := strconv.ParseInt(newest.CreatedTime, 10, 64)
			updatedTime, err2 := strconv.ParseInt(newest.UpdatedTime, 10, 64)
			if err1 == nil && err2 == nil {
				timeDiff := updatedTime - createdTime
				if timeDiff >= 0 && timeDiff <= 3000 { // Diferença de até 3 segundos (3000ms)
					isLimitExecutedQuickly = true
				}
			}

			if hasExistingOrder {
				oldPriceStr := getDisplayPrice(existingOrder)
				newPriceStr := getDisplayPrice(newest)
				if oldPriceStr != newPriceStr {
					isLimitMoved = true
					movedOrderIDs[newest.OrderID] = true
					if o, errO := strconv.ParseFloat(oldPriceStr, 64); errO == nil {
						if n, errN := strconv.ParseFloat(newPriceStr, 64); errN == nil {
							movedOrderPrices[newest.OrderID] = struct{ Old, New float64 }{o, n}
						}
					}
				}
			}

			if isLimitExecutedQuickly || isLimitMoved {
				preparedOrders = append(preparedOrders, newest)
			}

			continue
		}

		if oldest.OrderStatus == "New" && newest.OrderStatus == "New" {
			if hasExistingOrder && getDisplayPrice(existingOrder) != getDisplayPrice(newest) {
				movedOrderIDs[newest.OrderID] = true
				if o, errO := strconv.ParseFloat(getDisplayPrice(existingOrder), 64); errO == nil {
					if n, errN := strconv.ParseFloat(getDisplayPrice(newest), 64); errN == nil {
						movedOrderPrices[newest.OrderID] = struct{ Old, New float64 }{o, n}
					}
				}
			} else if !hasExistingOrder && getDisplayPrice(oldest) != getDisplayPrice(newest) {
				movedOrderIDs[newest.OrderID] = true
			}
		}

		preparedOrders = append(preparedOrders, newest)
	}

	// Regra 2: stops preparados + stopMovedPrices (para formatStopMovedMessage no delay buffer)
	var preparedStops []OrderData
	stopMovedPrices := make(map[string]struct{ Old, New float64 })
	for _, versions := range stopsCopy {
		sortOrderVersionsByUpdatedTime(versions)
		oldest, newest := versions[0], versions[len(versions)-1]

		existingStopOrderJSON, _ := wsm.accountManager.GetOrder(oldest.OrderID)
		var existingStopOrder OrderData
		hasExistingStopOrder := false
		if existingStopOrderJSON != "" {
			if json.Unmarshal([]byte(existingStopOrderJSON), &existingStopOrder) == nil {
				hasExistingStopOrder = true
			}
		}

		if oldest.OrderStatus == "Untriggered" && newest.OrderStatus == "Deactivated" {
			if hasExistingStopOrder {
				prevStop := versions[len(versions)-2]
				if existingStopOrder.TriggerPrice != prevStop.TriggerPrice {
					if o, errO := strconv.ParseFloat(existingStopOrder.TriggerPrice, 64); errO == nil {
						if n, errN := strconv.ParseFloat(prevStop.TriggerPrice, 64); errN == nil {
							stopMovedPrices[prevStop.OrderID] = struct{ Old, New float64 }{o, n}
							preparedStops = append(preparedStops, prevStop)
						}
					}
				}
				preparedStops = append(preparedStops, newest)
				continue
			} else {
				continue
			}
		}

		// Várias versões todas Untriggered: verificar se trigger price mudou (stop movido)
		if oldest.OrderStatus == "Untriggered" && newest.OrderStatus == "Untriggered" && hasExistingStopOrder && existingStopOrder.TriggerPrice != newest.TriggerPrice {
			if o, errO := strconv.ParseFloat(existingStopOrder.TriggerPrice, 64); errO == nil {
				if n, errN := strconv.ParseFloat(newest.TriggerPrice, 64); errN == nil {
					stopMovedPrices[newest.OrderID] = struct{ Old, New float64 }{o, n}
				}
			}
		}
		preparedStops = append(preparedStops, newest)
	}

	// Regra 4: lista de notificações de ordens (ordenar por updatedTime, agrupar por 2s createdTime)
	sort.Slice(preparedOrders, func(i, j int) bool {
		ti, _ := strconv.ParseInt(preparedOrders[i].UpdatedTime, 10, 64)
		tj, _ := strconv.ParseInt(preparedOrders[j].UpdatedTime, 10, 64)
		return ti < tj
	})
	var orderNotifications []delayNotificationItem
	for i := 0; i < len(preparedOrders); {
		order := preparedOrders[i]
		orderUpdatedTime, _ := strconv.ParseInt(order.UpdatedTime, 10, 64)
		orderCreatedTime, _ := strconv.ParseInt(order.CreatedTime, 10, 64)

		if order.OrderStatus == "Cancelled" {
			orderNotifications = append(orderNotifications, delayNotificationItem{
				UpdatedTime:      orderUpdatedTime,
				NotificationType: "cancelled_order",
				Data:             []OrderData{order},
			})
			i++
			continue
		}

		if movedOrderIDs[order.OrderID] {
			item := delayNotificationItem{UpdatedTime: orderUpdatedTime, Data: []OrderData{order}}
			if prices, ok := movedOrderPrices[order.OrderID]; ok {
				item.NotificationType = "order_moved"
				item.OldPrice = prices.Old
				item.NewPrice = prices.New
			} else {
				item.NotificationType = "simple_order"
			}
			orderNotifications = append(orderNotifications, item)
			i++
			continue
		}

		// Tentar agrupar com ordens seguintes (mesmo Symbol, ReduceOnly, Side, OrderType e createdTime dentro de 2s)
		group := []OrderData{order}
		minUpdated := orderUpdatedTime
		j := i + 1
		for j < len(preparedOrders) {
			next := preparedOrders[j]
			if next.OrderStatus == "Cancelled" || movedOrderIDs[next.OrderID] {
				break
			}
			orderReduce := ""
			if order.ReduceOnly {
				orderReduce = "Reduce"
			}
			nextReduce := ""
			if next.ReduceOnly {
				nextReduce = "Reduce"
			}
			orderKey := fmt.Sprintf("%s_%s_%s_%s", order.Symbol, orderReduce, order.Side, order.OrderType)
			nextKey := fmt.Sprintf("%s_%s_%s_%s", next.Symbol, nextReduce, next.Side, next.OrderType)
			if nextKey != orderKey {
				break
			}
			nextCreated, _ := strconv.ParseInt(next.CreatedTime, 10, 64)
			if nextCreated < orderCreatedTime || nextCreated > orderCreatedTime+orderGroupCreatedTimeWindowMs {
				// fora da janela 2s a partir da primeira do grupo
				break
			}
			group = append(group, next)
			nu, _ := strconv.ParseInt(next.UpdatedTime, 10, 64)
			if nu < minUpdated {
				minUpdated = nu
			}
			j++
		}

		if len(group) == 1 {
			orderNotifications = append(orderNotifications, delayNotificationItem{
				UpdatedTime:      orderUpdatedTime,
				NotificationType: "simple_order",
				Data:             group,
			})
		} else {
			orderNotifications = append(orderNotifications, delayNotificationItem{
				UpdatedTime:      minUpdated,
				NotificationType: "orders_group",
				Data:             group,
			})
		}
		i = j
	}

	// Regra 5: adicionar stops à lista (ignorar triggerPrice 0)
	for _, stop := range preparedStops {
		triggerPrice, _ := strconv.ParseFloat(stop.TriggerPrice, 64)
		if triggerPrice == 0 {
			continue
		}
		uTime, _ := strconv.ParseInt(stop.UpdatedTime, 10, 64)
		if stop.OrderStatus == "Deactivated" {
			orderNotifications = append(orderNotifications, delayNotificationItem{
				UpdatedTime:      uTime,
				NotificationType: "deactivated_stop",
				Data:             []OrderData{stop},
			})
		} else if prices, ok := stopMovedPrices[stop.OrderID]; ok {
			item := delayNotificationItem{
				UpdatedTime:      uTime,
				NotificationType: "stop_moved",
				Data:             []OrderData{stop},
			}

			item.OldPrice = prices.Old
			item.NewPrice = prices.New
			
			orderNotifications = append(orderNotifications, item)
		} else {
			orderNotifications = append(orderNotifications, delayNotificationItem{
				UpdatedTime:      uTime,
				NotificationType: "untriggered_stop",
				Data:             []OrderData{stop},
			})
		}
	}

	// Regra 6: ordenar lista por updatedTime
	sort.Slice(orderNotifications, func(i, j int) bool {
		return orderNotifications[i].UpdatedTime < orderNotifications[j].UpdatedTime
	})

	// Regra 7: agrupar cancelled_order consecutivos
	for i := 0; i < len(orderNotifications); i++ {
		if orderNotifications[i].NotificationType != "cancelled_order" || len(orderNotifications[i].Data) == 0 {
			continue
		}
		merged := orderNotifications[i].Data
		for j := i + 1; j < len(orderNotifications) && orderNotifications[j].NotificationType == "cancelled_order"; j++ {
			merged = append(merged, orderNotifications[j].Data...)
			orderNotifications[j].Data = nil
		}
		orderNotifications[i].Data = merged
	}

	// Atualizar banco: ordens e stops
	for _, item := range orderNotifications {
		for _, o := range item.Data {
			orderJSON, _ := json.Marshal(o)
			if item.NotificationType == "cancelled_order" || item.NotificationType == "deactivated_stop" {
				_ = wsm.accountManager.DeleteOrder(o.OrderID)
			} else if item.NotificationType == "untriggered_stop" || item.NotificationType == "simple_order" || item.NotificationType == "orders_group" || item.NotificationType == "order_moved" || item.NotificationType == "stop_moved" {
				if o.OrderStatus != "Filled" && o.OrderStatus != "PartiallyFilled" {
					_ = wsm.accountManager.SaveOrder(o.OrderID, accountID, string(orderJSON))
				} else {
					_ = wsm.accountManager.DeleteOrder(o.OrderID)
				}
			}
		}
	}

	// Regra 8 e 9: montar texto e enviar uma mensagem
	// Buscar última wallet da conta para exibir % da ordem em relação ao saldo da moeda
	var lastWallet *WalletData
	sinceWallet := time.Now().Add(-7 * 24 * time.Hour)
	if walletRows, err := wsm.db.GetWalletSnapshotsUpdatedSince(accountID, sinceWallet); err == nil && len(walletRows) > 0 {
		lastWallet = mergeWalletSnapshotRows(walletRows)
	}
	var parts []string
	for _, item := range orderNotifications {
		if len(item.Data) == 0 {
			continue
		}
		switch item.NotificationType {
		case "orders_group", "simple_order":
			parts = append(parts, formatOrderGroupMessage(lastWallet, item.Data))
		case "order_moved":
			parts = append(parts, formatOrderMovedMessage(item.Data[0], item.OldPrice, item.NewPrice, lastWallet))
		case "cancelled_order":
			var toNotify []OrderData
			for _, o := range item.Data {
				if hasValidDisplayPrice(o) {
					toNotify = append(toNotify, o)
				}
			}
			if len(toNotify) > 0 {
				parts = append(parts, formatCancelMessage(toNotify))
			}
		case "untriggered_stop":
			parts = append(parts, formatStopOrderMessage(item.Data[0], lastWallet))
		case "stop_moved":
			parts = append(parts, formatStopMovedMessage(item.Data[0], item.OldPrice, item.NewPrice, lastWallet))
		case "deactivated_stop":
			parts = append(parts, formatStopCancellationMessage(item.Data[0]))
		}
	}
	if len(parts) > 0 {
		messageText := strings.Join(parts, "\n\n")
		wsm.sendNotificationWithType(wsConn, messageText, true, false)
	}

	// Regra 10: execuções (delay para notificação de ordens chegar ao Discord antes)
	if len(executionsCopy) > 0 {
		time.Sleep(2 * time.Second)
		wsm.flushExecutions(wsConn, executionsCopy)
	}
}

func (wsm *WebSocketManager) handleExecutionMessage(wsConn *WebSocketConnection, execMsg BybitExecutionMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handleExecutionMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handleExecutionMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de execution recebida! Total de execuções: %d", len(execMsg.Data))
	}

	for _, execData := range execMsg.Data {
		// Processar apenas execuções inverse
		if execData.Category != "inverse" {
			if logger != nil {
				logger.Log("[DEBUG] Execução ignorada - não é inverse (category: %s)", execData.Category)
			}
			continue
		}

		// Processar apenas execuções do tipo Trade
		if execData.ExecType != "Trade" {
			if logger != nil {
				logger.Log("[DEBUG] Execução ignorada - não é Trade (execType: %s)", execData.ExecType)
			}
			continue
		}

		if logger != nil {
			jsonData, _ := json.Marshal(execData)

			logger.Log("[DEBUG] Processando execução - Symbol: %s, Side: %s, ExecPrice: %s | JSON: %s",
				execData.Symbol, execData.Side, execData.ExecPrice, string(jsonData))
		}

		// Adicionar ao buffer de execution (inicia/reseta timer de 15 minutos)
		wsm.addWalletNotificationToBuffer(wsConn.AccountID, wsConn)

		wsm.addExecutionToDelayBuffer(wsConn.AccountID, execData, wsConn)

	}
}

func (wsm *WebSocketManager) handlePositionMessage(wsConn *WebSocketConnection, posMsg BybitPositionMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handlePositionMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handlePositionMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de position recebida! Total de posições: %d", len(posMsg.Data))
	}

	// Processar apenas posições inverse
	for _, posData := range posMsg.Data {
		if posData.Category != "inverse" {
			if logger != nil {
				logger.Log("[DEBUG] Posição ignorada - não é inverse (category: %s)", posData.Category)
			}
			continue
		}

		// Persistir última mensagem de position no banco (logo após validar)
		if jsonData, err := json.Marshal(posData); err == nil {
			if err := wsm.db.SaveLastMessageSnapshot(wsConn.AccountID, "position", posData.Symbol, string(jsonData)); err != nil && logger != nil {
				logger.Log("Erro ao salvar snapshot de position no banco: %v", err)
			}
		}
	}
}

func (wsm *WebSocketManager) handleWalletMessage(wsConn *WebSocketConnection, walletMsg BybitWalletMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handleWalletMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handleWalletMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de wallet recebida! Total de wallets: %d", len(walletMsg.Data))
	}

	// Processar apenas wallets UNIFIED
	for _, walletData := range walletMsg.Data {
		if walletData.AccountType != "UNIFIED" {
			if logger != nil {
				logger.Log("[DEBUG] Wallet ignorado - não é UNIFIED (accountType: %s)", walletData.AccountType)
			}
			continue
		}

		// Persistir última mensagem de wallet no banco: uma linha por Coin (logo após validar)
		for _, coin := range walletData.Coin {
			walletCopy := walletData
			walletCopy.Coin = []CoinBalance{coin}
			if jsonData, err := json.Marshal(walletCopy); err == nil {
				if err := wsm.db.SaveLastMessageSnapshot(wsConn.AccountID, "wallet", coin.Coin, string(jsonData)); err != nil && logger != nil {
					logger.Log("Erro ao salvar snapshot de wallet no banco: %v", err)
				}
			}
		}

		// Agendar notificação Google Sheets em 2 minutos (dados lidos do banco na hora)
		if wsConn.Account.WebhookURLGoogleSheets != "" && wsConn.Account.SheetURLGoogleSheets != "" {
			wsm.resetSheetsTimer(wsConn.AccountID, wsConn)
		}
	}
}

func (wsm *WebSocketManager) addWalletNotificationToBuffer(accountID int64, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.walletNotificationBuffers[accountID]
	if !exists {
		buffer = &WalletNotification{accountID: accountID}
		wsm.walletNotificationBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.discordTimer != nil {
		buffer.discordTimer.Stop()
	}
	buffer.discordTimer = time.AfterFunc(15*time.Minute, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processWalletNotification (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processWalletNotification(accountID, wsConn)
	})
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Execução recebida, iniciando/resetando timer de 15 minutos para Discord")
	}
}

func (wsm *WebSocketManager) resetSheetsTimer(accountID int64, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.walletNotificationBuffers[accountID]
	if !exists {
		buffer = &WalletNotification{accountID: accountID}
		wsm.walletNotificationBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.sheetsTimer != nil {
		buffer.sheetsTimer.Stop()
	}
	buffer.sheetsTimer = time.AfterFunc(2*time.Minute, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processSheetsNotification (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processSheetsNotification(accountID)
	})
}

// mergeWalletSnapshotRows usa a linha mais recente (primeira) como base e junta as Coin das demais.
// As linhas devem vir ordenadas por updated_at DESC.
func mergeWalletSnapshotRows(rows []WalletSnapshotRow) *WalletData {
	if len(rows) == 0 {
		return nil
	}
	var base WalletData
	if err := json.Unmarshal([]byte(rows[0].Message), &base); err != nil {
		return nil
	}
	hasSymbol := func(w *WalletData, symbol string) bool {
		for _, c := range w.Coin {
			if c.Coin == symbol {
				return true
			}
		}
		return false
	}
	for i := 1; i < len(rows); i++ {
		var w WalletData
		if err := json.Unmarshal([]byte(rows[i].Message), &w); err != nil || len(w.Coin) == 0 {
			continue
		}
		c := w.Coin[0]
		if !hasSymbol(&base, c.Coin) {
			base.Coin = append(base.Coin, c)
		}
	}
	return &base
}

func (wsm *WebSocketManager) processWalletNotification(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processWalletNotification para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processWalletNotification: %v", r)
				}
			}()
		}
	}()

	// Verificar se a conexão ainda está ativa
	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	activeConn := conn
	wsm.mu.RUnlock()

	wsConn = activeConn

	// Buscar wallets atualizadas nos últimos 17 minutos no banco
	sinceWallet := time.Now().Add(-17 * time.Minute)
	walletRows, err := wsm.db.GetWalletSnapshotsUpdatedSince(accountID, sinceWallet)
	if err != nil || len(walletRows) == 0 {
		return
	}

	lastWallet := mergeWalletSnapshotRows(walletRows)
	if lastWallet == nil {
		return
	}

	// Buscar posições no banco (referentes às coins da wallet)
	positionRows, err := wsm.db.GetPositionSnapshots(accountID)
	if err != nil {
		return
	}
	positions := make(map[string]*PositionData)
	for _, row := range positionRows {
		var pos PositionData
		if err := json.Unmarshal([]byte(row.Message), &pos); err == nil {
			positions[row.Symbol] = &pos
		}
	}

	// Obter valor total da carteira do wallet
	totalEquity, err := strconv.ParseFloat(lastWallet.TotalEquity, 64)
	if err != nil {
		// Fallback para totalWalletBalance
		totalEquity, err = strconv.ParseFloat(lastWallet.TotalWalletBalance, 64)
		if err != nil {
			// Não foi possível obter valor da carteira - não processar
			return
		}
	}

	// Função auxiliar para calcular proteção de uma posição
	calculatePositionValues := func(position *PositionData, totalEquityCoin float64) (longUSD, protecaoUSD, expostoUSD float64) {
		// Converter position.Size de string para float64
		size, err := strconv.ParseFloat(position.Size, 64)
		if err != nil {
			size = 0
		}

		if position.Side == "Sell" {
			protecaoUSD = size
			longUSD = 0
		} else {
			protecaoUSD = 0
			longUSD = size
		}

		// Exposto = total da moeda - protegido
		expostoUSD = totalEquityCoin - size
		return
	}

	// Calcular valores por moeda e preparar mensagens
	var totalProtecaoUSD float64
	var totalLongUSD float64
	var totalExposicaoUSD float64
	var coinMessages []string // Mensagens por moeda para usar no else se necessário
	var totalValidPositions int = 0

	// Processar todas as posições para calcular totais e criar mensagens
	for symbol, position := range positions {
		// Extrair moeda do símbolo (ex: BTCUSD -> BTC, ETHUSD -> ETH)
		coin := symbol
		if strings.HasSuffix(symbol, "USD") {
			coin = symbol[:len(symbol)-3]
		} else if strings.HasSuffix(symbol, "USDT") {
			coin = symbol[:len(symbol)-4]
		} else if strings.HasSuffix(symbol, "USDC") {
			coin = symbol[:len(symbol)-4]
		}

		var totalEquityPerCoin float64
		for _, coinBalance := range lastWallet.Coin {
			if coinBalance.Coin == coin {
				equity, err := strconv.ParseFloat(coinBalance.UsdValue, 64)
				if err == nil {
					totalEquityPerCoin = equity
				}
				break
			}
		}

		// ignorar moedas com balance inferior a 10 USD
		if (totalEquityPerCoin < 10) {
			continue
		}

		totalValidPositions++

		// Calcular valores da posição
		longPosUSD, protecaoPosUSD, expostoPosUSD := calculatePositionValues(position, totalEquityPerCoin)
		totalProtecaoUSD += protecaoPosUSD
		totalLongUSD += longPosUSD
		totalExposicaoUSD += expostoPosUSD

		// Calcular % protegida para esta posição
		var percentProtegidaPos float64 = 0.0
		if protecaoPosUSD > 0 && totalEquityPerCoin > 0 {
			percentProtegidaPos = (protecaoPosUSD / totalEquityPerCoin) * 100
		}

		// Calcular % longada para esta posição
		var percentLongadaPos float64 = 0.0
		if longPosUSD > 0 && totalEquityPerCoin > 0 {
			percentLongadaPos = (longPosUSD / totalEquityPerCoin) * 100
		}

		// Criar mensagem da moeda (para usar no else se necessário)
		var coinMsgParts []string
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("📌 %s (%s):", coin, symbol))
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  💰 Total: $%s USD", formatPriceCoin(totalEquityPerCoin)))
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  🛡️ Protegido: $%s USD", formatPriceCoin(protecaoPosUSD)))
		if longPosUSD > 0 {
			coinMsgParts = append(coinMsgParts, fmt.Sprintf("  📈 Posição Long: $%s USD", formatPriceCoin(longPosUSD)))
		}
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  ⚠️ Exposto: $%s USD", formatPriceCoin(expostoPosUSD)))
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  📈 %% Protegida: %s%%", formatPriceCoin(percentProtegidaPos)))
		if longPosUSD > 0 {
			coinMsgParts = append(coinMsgParts, fmt.Sprintf("  📊 %% Longada: %s%%", formatPriceCoin(percentLongadaPos)))
		}
		coinMsgParts = append(coinMsgParts, "")
		coinMessages = append(coinMessages, strings.Join(coinMsgParts, "\n"))
	}

	// Construir mensagem
	var messageParts []string

	for _, coinMsg := range coinMessages {
		messageParts = append(messageParts, coinMsg)
	}

	// retornar o resumo geral da carteira apenas se tiver mais de uma posição válida ou nenhuma posição válida
	if totalValidPositions != 1 {
		messageParts = append(messageParts, "📊 Resumo Geral:")
		messageParts = append(messageParts, fmt.Sprintf("  💰 Carteira Total: $%s USD", formatPriceCoin(totalEquity)))
		messageParts = append(messageParts, fmt.Sprintf("  🛡️ Proteção Total: $%s USD", formatPriceCoin(totalProtecaoUSD)))
		if totalLongUSD > 0 {
			messageParts = append(messageParts, fmt.Sprintf("  📈 Long Total: $%s USD", formatPriceCoin(totalLongUSD)))
		}
		messageParts = append(messageParts, fmt.Sprintf("  ⚠️ Exposição Total: $%s USD", formatPriceCoin(totalExposicaoUSD)))

		// Calcular % protegida geral
		var percentProtegidaGeral float64
		if totalEquity > 0 {
			percentProtegidaGeral = (totalProtecaoUSD / totalEquity) * 100
		}
		messageParts = append(messageParts, fmt.Sprintf("  📈 %% Protegida: %s%%", formatPriceCoin(percentProtegidaGeral)))
	
		// Calcular % longada geral
		if totalLongUSD > 0 {
			var percentLongadaGeral float64
			if totalEquity > 0 {
				percentLongadaGeral = (totalLongUSD / totalEquity) * 100
			}
			messageParts = append(messageParts, fmt.Sprintf("  📊 %% Longada: %s%%", formatPriceCoin(percentLongadaGeral)))
		}
	}

	messageText := strings.Join(messageParts, "\n")

	// Enviar notificação (carteira)
	wsm.sendNotificationWithType(wsConn, messageText, false, true)
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Notificação de posição enviada após 15 minutos sem execuções")
	}
}

func (wsm *WebSocketManager) processSheetsNotification(accountID int64) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processSheetsNotification para conta %d: %v\n", accountID, r)
		}
	}()

	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	wsConn := conn
	wsm.mu.RUnlock()

	if wsConn.Account.WebhookURLGoogleSheets == "" || wsConn.Account.SheetURLGoogleSheets == "" {
		return
	}

	sinceWallet := time.Now().Add(-3 * time.Minute)
	walletRows, err := wsm.db.GetWalletSnapshotsUpdatedSince(accountID, sinceWallet)
	if err != nil || len(walletRows) == 0 {
		return
	}

	lastWallet := mergeWalletSnapshotRows(walletRows)
	if lastWallet == nil {
		return
	}

	positionRows, err := wsm.db.GetPositionSnapshots(accountID)
	if err != nil {
		return
	}
	positions := make(map[string]*PositionData)
	for _, row := range positionRows {
		var pos PositionData
		if err := json.Unmarshal([]byte(row.Message), &pos); err == nil {
			positions[row.Symbol] = &pos
		}
	}

	calculatePositionValues := func(position *PositionData, totalEquityCoin float64) (longUSD, protecaoUSD, expostoUSD float64) {
		size, err := strconv.ParseFloat(position.Size, 64)
		if err != nil {
			size = 0
		}
		if position.Side == "Sell" {
			protecaoUSD = size
			longUSD = 0
		} else {
			protecaoUSD = 0
			longUSD = size
		}
		expostoUSD = totalEquityCoin - size
		return
	}

	logger, _ := getLogger(accountID, wsConn.Account.Name)
	now := getBrasiliaTime()
	dateTimeStr := now.Format("02/01/2006 15:04")
	headers := []string{"data", "moeda", "total_moeda", "total_dolar", "total_protegido", "total_exposto", "total_long"}

	var webhookPayloads []struct {
		coin    string
		columns []interface{}
		headers []string
	}

	for _, coinBalance := range lastWallet.Coin {
		coin := coinBalance.Coin
		var position *PositionData
		for posSymbol, pos := range positions {
			posCoin := posSymbol
			if strings.HasSuffix(posSymbol, "USD") {
				posCoin = posSymbol[:len(posSymbol)-3]
			} else if strings.HasSuffix(posSymbol, "USDT") {
				posCoin = posSymbol[:len(posSymbol)-4]
			} else if strings.HasSuffix(posSymbol, "USDC") {
				posCoin = posSymbol[:len(posSymbol)-4]
			}
			if posCoin == coin {
				position = pos
				break
			}
		}

		equity, _ := strconv.ParseFloat(coinBalance.Equity, 64)
		usdValue, _ := strconv.ParseFloat(coinBalance.UsdValue, 64)
		var protecaoUSD, expostoUSD, longUSD float64
		if position != nil {
			longUSD, protecaoUSD, expostoUSD = calculatePositionValues(position, usdValue)
		} else {
			expostoUSD = usdValue
		}

		columns := []interface{}{
			dateTimeStr,
			coinBalance.Coin,
			equity,
			usdValue,
			protecaoUSD,
			expostoUSD,
			longUSD,
		}
		webhookPayloads = append(webhookPayloads, struct {
			coin    string
			columns []interface{}
			headers []string
		}{coin: coinBalance.Coin, columns: columns, headers: headers})
	}

	// Enviar webhooks em goroutine para não bloquear a thread principal
	webhookURL := wsConn.Account.WebhookURLGoogleSheets
	sheetURL := wsConn.Account.SheetURLGoogleSheets
	go func() {
		for _, p := range webhookPayloads {
			if err := sendGoogleSheetsWebhook(webhookURL, sheetURL, p.coin, p.columns, p.headers); err != nil {
				if logger != nil {
					logger.Log("Erro ao enviar webhook do Google Sheets para %s: %v", p.coin, err)
				}
			}
		}
	}()
}

// formatExecTimeToBrasilia converte timestamp em ms (string) para data no formato Brasil "DD/MM/YYYY HH:MM". Se inválido, usa time.Now() em Brasília.
func formatExecTimeToBrasilia(execTimeMs string) string {
	ms, err := strconv.ParseInt(execTimeMs, 10, 64)
	if err != nil {
		return getBrasiliaTime().Format("02/01/2006 15:04")
	}
	t := time.UnixMilli(ms)
	if loc, err := time.LoadLocation("America/Sao_Paulo"); err == nil {
		return t.In(loc).Format("02/01/2006 15:04")
	}
	brasiliaOffset := -3 * 60 * 60
	brasiliaTZ := time.FixedZone("BRT", brasiliaOffset)
	return t.In(brasiliaTZ).Format("02/01/2006 15:04")
}

// formatQtyCoin formata quantidade/valor em moeda sem notação científica e sem casas decimais fixas (remove zeros à direita).
func formatQtyCoin(v float64) string {
	s := fmt.Sprintf("%.15f", v)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}

// formatPriceCoin formata preço em moeda sem notação científica e sem casas decimais fixas (remove zeros à direita).
func formatPriceCoin(v float64) string {
	s := fmt.Sprintf("%.2f", v)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}

// symbolToCoin extrai a moeda do símbolo (ex: BTCUSD -> BTC).
func symbolToCoin(symbol string) string {
	if strings.HasSuffix(symbol, "USD") {
		return symbol[:len(symbol)-3]
	}
	if strings.HasSuffix(symbol, "USDT") || strings.HasSuffix(symbol, "USDC") {
		return symbol[:len(symbol)-4]
	}
	return symbol
}

// flushExecutions envia a lista de execuções para Discord e Google Sheets. Usado por processDelayBuffer.
func (wsm *WebSocketManager) flushExecutions(wsConn *WebSocketConnection, executions []ExecutionData) {
	if len(executions) == 0 {
		return
	}
	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if wsConn.Account.WebhookURLExecutions != "" {
		var parts []string
		for _, e := range executions {
			coin := symbolToCoin(e.Symbol)
			price, _ := strconv.ParseFloat(e.ExecPrice, 64)
			qtyUsd, _ := strconv.ParseFloat(e.ExecQty, 64)
			stopText := ""
			if e.CreateType == "CreateByStopOrder" {
				stopText = "Stop "
			}
			parts = append(parts, fmt.Sprintf("%s - %s %s %s%s | Preço: %s | USD: %s",
				formatExecTimeToBrasilia(e.ExecTime), coin, e.Side, stopText, e.OrderType, formatPriceCoin(price), formatPriceCoin(qtyUsd)))
		}
		messageText := strings.Join(parts, "\n")
		go wsm.sendExecutionNotification(wsConn, messageText)
	}

	if wsConn.Account.WebhookURLGoogleSheets != "" && wsConn.Account.SheetURLGoogleSheetsExecutions != "" {
		byCoin := make(map[string][]ExecutionData)
		for _, e := range executions {
			coin := symbolToCoin(e.Symbol)
			byCoin[coin] = append(byCoin[coin], e)
		}
		webhookURL := wsConn.Account.WebhookURLGoogleSheets
		sheetURLExec := wsConn.Account.SheetURLGoogleSheetsExecutions
		for coin, execs := range byCoin {
			coinCopy := coin
			execsCopy := make([]ExecutionData, len(execs))
			copy(execsCopy, execs)
			go func() {
				if err := wsm.sendGoogleSheetsExecutionWebhook(webhookURL, sheetURLExec, coinCopy, execsCopy); err != nil && logger != nil {
					logger.Log("Erro ao enviar webhook de execuções para %s: %v", coinCopy, err)
				}
			}()
		}
	}
}

func (wsm *WebSocketManager) sendExecutionNotification(wsConn *WebSocketConnection, messageText string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] sendExecutionNotification para conta %d: %v\n", wsConn.AccountID, r)
		}
	}()
	if wsConn.Account.WebhookURLExecutions == "" {
		return
	}
	everyoneTag := ""
	if wsConn.Account.MarkEveryoneExecution {
		everyoneTag = "@everyone "
	}

	discordMsg := fmt.Sprintf("%s🔔 Execuções\n%s", everyoneTag, messageText)
	if err := sendDiscordWebhook(wsConn.Account.WebhookURLExecutions, discordMsg); err != nil {
		logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
		if logger != nil {
			logger.Log("Erro ao enviar webhook de execuções: %v", err)
		}
	}
}

// ExecutionRow representa uma linha no payload de execuções do Google Sheets.
type ExecutionRow struct {
	Columns []interface{} `json:"columns"`
}

func (wsm *WebSocketManager) sendGoogleSheetsExecutionWebhook(webhookURL, sheetURLExecutions, coin string, executions []ExecutionData) error {
	if webhookURL == "" || sheetURLExecutions == "" {
		return fmt.Errorf("webhook URL ou sheet URL execuções está vazia")
	}
	sheetID, err := extractSheetID(sheetURLExecutions)
	if err != nil {
		return fmt.Errorf("erro ao extrair ID da planilha: %w", err)
	}
	headers := []string{"data", "moeda", "operação", "tipo", "preço", "total_moeda", "total_dolar"}
	var rows []ExecutionRow
	for _, e := range executions {

		stopText := ""
		if e.CreateType == "CreateByStopOrder" {
			stopText = "Stop "
		}

		price, _ := strconv.ParseFloat(e.ExecPrice, 64)
		qtyUsd, _ := strconv.ParseFloat(e.ExecQty, 64)
		valCoin, _ := strconv.ParseFloat(e.ExecValue, 64)
		columns := []interface{}{
			formatExecTimeToBrasilia(e.ExecTime),
			coin,
			e.Side,
			stopText + e.OrderType,
			price,
			valCoin,
			qtyUsd,
		}
		rows = append(rows, ExecutionRow{Columns: columns})
	}
	payload := map[string]interface{}{
		"sheet_id": sheetID,
		"symbol":   coin + "_exec",
		"headers":  headers,
		"rows":     rows,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("erro ao serializar payload: %w", err)
	}
	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("erro ao enviar requisição: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}
	return nil
}

func (wsm *WebSocketManager) sendNotification(wsConn *WebSocketConnection, messageText string) {
	wsm.sendNotificationWithType(wsConn, messageText, false, false)
}

func (wsm *WebSocketManager) sendNotificationWithType(wsConn *WebSocketConnection, messageText string, isOrder bool, isWallet bool) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] sendNotification para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em sendNotification: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	
	alertIcon := "🔔" // Altere aqui para escolher outro ícone
	
	// Verificar se deve adicionar @everyone
	everyoneTag := ""
	if isOrder && wsConn.Account.MarkEveryoneOrder {
		everyoneTag = "@everyone "
	} else if isWallet && wsConn.Account.MarkEveryoneWallet {
		everyoneTag = "@everyone "
	}
	
	// Obter data/hora atual no horário de Brasília (funciona no Windows e Linux)
	now := getBrasiliaTime()
	timeStamp := fmt.Sprintf("🕘  %s - %s (Horário de Brasília)",
		now.Format("02/01/2006"),
		now.Format("15:04"))
	
	if wsConn.Account.WebhookURL != "" {
		// Enviar para Discord em goroutine para não bloquear o fluxo principal
		// Discord remove quebras de linha no início, então precisamos ter conteúdo antes
		webhookURL := wsConn.Account.WebhookURL
		discordMsg := fmt.Sprintf("%s%s\n%s\n\n%s", everyoneTag, alertIcon, messageText, timeStamp)
		go func() {
			if err := sendDiscordWebhook(webhookURL, discordMsg); err != nil {
				if logger != nil {
					logger.Log("Erro ao enviar webhook, notificação: %s", messageText)
				}
			}
		}()
	}
	// Quando não há webhook, não fazer nada (não logar nem imprimir)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sendDiscordWebhook(webhookURL, message string) error {
	payload := map[string]string{
		"content": message,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return nil
}

// validateGoogleSheetsWebhookURL valida se a URL do webhook do Google Sheets está no formato correto
func validateGoogleSheetsWebhookURL(url string) bool {
	if url == "" {
		return true // URL vazia é válida (opcional)
	}
	// Validar formato: https://script.google.com/macros/s/.../exec
	matched, _ := regexp.MatchString(`^https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec$`, url)
	return matched
}

// validateGoogleSheetsURL valida se a URL da planilha do Google Sheets está no formato correto
func validateGoogleSheetsURL(url string) bool {
	if url == "" {
		return true // URL vazia é válida (opcional)
	}
	// Validar formato: https://docs.google.com/spreadsheets/d/.../edit...
	matched, _ := regexp.MatchString(`^https://docs\.google\.com/spreadsheets/d/[A-Za-z0-9_-]+/`, url)
	return matched
}

// extractSheetID extrai o ID da planilha da URL do Google Sheets
func extractSheetID(sheetURL string) (string, error) {
	if sheetURL == "" {
		return "", fmt.Errorf("URL da planilha está vazia")
	}
	
	// Padrão: /spreadsheets/d/{ID}/
	re := regexp.MustCompile(`/spreadsheets/d/([A-Za-z0-9_-]+)`)
	matches := re.FindStringSubmatch(sheetURL)
	if len(matches) < 2 {
		return "", fmt.Errorf("não foi possível extrair o ID da planilha da URL: %s", sheetURL)
	}
	
	return matches[1], nil
}

// sendGoogleSheetsWebhook envia dados para o webhook do Google Sheets
func sendGoogleSheetsWebhook(webhookURL, sheetURL, symbol string, columns []interface{}, headers []string) error {
	if webhookURL == "" || sheetURL == "" {
		return fmt.Errorf("webhook URL ou sheet URL está vazia")
	}

	// Extrair ID da planilha
	sheetID, err := extractSheetID(sheetURL)
	if err != nil {
		return fmt.Errorf("erro ao extrair ID da planilha: %w", err)
	}

	// Montar payload
	payload := map[string]interface{}{
		"sheet_id": sheetID,
		"symbol":   symbol,
		"columns":  columns,
	}

	// Adicionar headers apenas se fornecido
	if len(headers) > 0 {
		payload["headers"] = headers
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("erro ao serializar payload: %w", err)
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("erro ao enviar requisição: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return nil
}
