package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketManager struct {
	accountManager            *AccountManager
	db                        *Database
	connections               map[int64]*WebSocketConnection
	mu                        sync.RWMutex
	orderBuffers              map[int64]*OrderBuffer
	cancelBuffers             map[int64]*CancelBuffer
	executionBuffers          map[int64]*ExecutionBuffer
	executionNotificationBuffers map[int64]*ExecutionNotificationBuffer
	bufferMu                  sync.RWMutex
}

type ExecutionNotificationBuffer struct {
	executions []ExecutionData
	timer      *time.Timer
	mu         sync.Mutex
	accountID  int64
}

type OrderBuffer struct {
	orders    []OrderData
	timer     *time.Timer
	mu        sync.Mutex
	accountID int64
}

type CancelBuffer struct {
	orders    []OrderData
	timer     *time.Timer
	mu        sync.Mutex
	accountID int64
}

type ExecutionBuffer struct {
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
		accountManager:               accountManager,
		db:                           db,
		connections:                  make(map[int64]*WebSocketConnection),
		orderBuffers:                 make(map[int64]*OrderBuffer),
		cancelBuffers:                make(map[int64]*CancelBuffer),
		executionBuffers:             make(map[int64]*ExecutionBuffer),
		executionNotificationBuffers: make(map[int64]*ExecutionNotificationBuffer),
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
	if orderBuffer, exists := wsm.orderBuffers[accountID]; exists {
		if orderBuffer.timer != nil {
			orderBuffer.timer.Stop()
		}
		delete(wsm.orderBuffers, accountID)
	}
	if cancelBuffer, exists := wsm.cancelBuffers[accountID]; exists {
		if cancelBuffer.timer != nil {
			cancelBuffer.timer.Stop()
		}
		delete(wsm.cancelBuffers, accountID)
	}
	if executionBuffer, exists := wsm.executionBuffers[accountID]; exists {
		executionBuffer.mu.Lock()
		if executionBuffer.discordTimer != nil {
			executionBuffer.discordTimer.Stop()
		}
		if executionBuffer.sheetsTimer != nil {
			executionBuffer.sheetsTimer.Stop()
		}
		executionBuffer.mu.Unlock()
		delete(wsm.executionBuffers, accountID)
	}
	if execNotifBuffer, exists := wsm.executionNotificationBuffers[accountID]; exists {
		execNotifBuffer.mu.Lock()
		if execNotifBuffer.timer != nil {
			execNotifBuffer.timer.Stop()
		}
		execNotifBuffer.mu.Unlock()
		delete(wsm.executionNotificationBuffers, accountID)
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

		// Processar stops Untriggered primeiro (sem delay)
		if orderData.OrderStatus == "Untriggered" {
			wsm.processStopOrder(wsConn, orderData)
			continue
		}

		// Processar cancelamentos de stops não triggerados (Deactivated)
		// Permitir processar mesmo se for CreateByStopOrder
		if orderData.OrderStatus == "Deactivated" {
			wsm.processStopCancellation(wsConn, orderData)
			continue
		}

		// Ignorar ordens criadas por stop order (exceto stops Untriggered e Deactivated já processados acima)
		if orderData.CreateType == "CreateByStopOrder" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - CreateByStopOrder (status: %s)", orderData.OrderStatus)
			}
			continue
		}

		// Processar abertura de ordem ou cancelamento
		// Verificar se é Limit executada rapidamente (até 3 segundos entre criação e atualização)
		// Verificar se a ordem Limit foi movida para outro preço
		isLimitExecutedQuickly := false
		isLimitMoved := false
		if orderData.OrderType == "Limit" && (orderData.OrderStatus == "Filled" || orderData.OrderStatus == "PartiallyFilled") {
			createdTime, err1 := strconv.ParseInt(orderData.CreatedTime, 10, 64)
			updatedTime, err2 := strconv.ParseInt(orderData.UpdatedTime, 10, 64)
			if err1 == nil && err2 == nil {
				timeDiff := updatedTime - createdTime
				if timeDiff >= 0 && timeDiff <= 3000 { // Diferença de até 3 segundos (3000ms)
					isLimitExecutedQuickly = true
				}
			}

			// Verificar se ordem existe no banco
			existingOrderJSON, err := wsm.accountManager.GetOrder(orderData.OrderID)
			if err == nil && existingOrderJSON != "" {
				// Ordem existe, verificar se o preço mudou
				var existingOrder OrderData
				if err := json.Unmarshal([]byte(existingOrderJSON), &existingOrder); err == nil {
					// Usar getDisplayPrice para obter os preços corretos
					oldPriceStr := getDisplayPrice(existingOrder)
					newPriceStr := getDisplayPrice(orderData)
					
					// Comparar preços usando os valores corretos
					if oldPriceStr != newPriceStr {
						isLimitMoved = true
					}
				}

				if !isLimitMoved {
					// Remover do banco se existir
					if err := wsm.accountManager.DeleteOrder(orderData.OrderID); err != nil {
						if logger != nil {
							logger.Log("[DEBUG] Erro ao remover ordem preenchida %s do banco: %v", orderData.OrderID, err)
						}
					}
				}
			}
		}

		if orderData.OrderStatus == "New" || (orderData.OrderType == "Market" && (orderData.OrderStatus == "Filled" || orderData.OrderStatus == "PartiallyFilled")) || isLimitExecutedQuickly || isLimitMoved {
			wsm.addOrderToBuffer(wsConn.AccountID, orderData, wsConn)
		} else if orderData.OrderStatus == "Cancelled" || (orderData.CancelType != "" && orderData.StopOrderType != "Stop" && orderData.OrderStatus != "Filled" && orderData.OrderStatus != "PartiallyFilled") {
			// Excluir stops do processamento de cancelamento normal
			wsm.addCancelToBuffer(wsConn.AccountID, orderData, wsConn)
		}
	}
}

func (wsm *WebSocketManager) addOrderToBuffer(accountID int64, order OrderData, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.orderBuffers[accountID]
	if !exists {
		buffer = &OrderBuffer{
			orders:    []OrderData{},
			accountID: accountID,
		}
		wsm.orderBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Adicionar ordem ao buffer
	buffer.orders = append(buffer.orders, order)

	logger, _ := getLogger(accountID, wsConn.Account.Name)

	// Se é a primeira ordem, iniciar timer de 2 segundos
	if len(buffer.orders) == 1 {
		buffer.timer = time.AfterFunc(2*time.Second, func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[PANIC] processOrderBuffer (timer) para conta %d: %v\n", accountID, r)
				}
			}()
			wsm.processOrderBuffer(accountID, wsConn)
		})
		if logger != nil {
			logger.Log("[DEBUG] Primeira ordem recebida, iniciando timer de 2s para agrupamento")
		}
	} else {
		// Se já existe timer, resetar para mais 2 segundos
		if buffer.timer != nil {
			if !buffer.timer.Stop() {
				// Timer já foi executado, não fazer nada
			}
		}
		buffer.timer = time.AfterFunc(2*time.Second, func() {
			wsm.processOrderBuffer(accountID, wsConn)
		})
		if logger != nil {
			logger.Log("[DEBUG] Ordem adicional recebida, resetando timer de 2s (total: %d ordens)", len(buffer.orders))
		}
	}
}

func (wsm *WebSocketManager) addCancelToBuffer(accountID int64, order OrderData, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.cancelBuffers[accountID]
	if !exists {
		buffer = &CancelBuffer{
			orders:    []OrderData{},
			accountID: accountID,
		}
		wsm.cancelBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Adicionar cancelamento ao buffer
	buffer.orders = append(buffer.orders, order)

	// Resetar timer de 2 segundos a cada nova ordem cancelada
	if buffer.timer != nil {
		buffer.timer.Stop()
	}
	buffer.timer = time.AfterFunc(2*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processCancelBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processCancelBuffer(accountID, wsConn)
	})
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Cancelamento recebido, resetando timer de 2s (total: %d cancelamentos)", len(buffer.orders))
	}
}

func (wsm *WebSocketManager) processOrderBuffer(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processOrderBuffer para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processOrderBuffer: %v", r)
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
	// Usar a conexão atual do mapa (pode ter mudado)
	activeConn := conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buffer, exists := wsm.orderBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}

	buffer.mu.Lock()
	if len(buffer.orders) == 0 {
		buffer.mu.Unlock()
		wsm.bufferMu.Unlock()
		return
	}

	// Copiar ordens e limpar buffer
	orders := make([]OrderData, len(buffer.orders))
	copy(orders, buffer.orders)
	buffer.orders = []OrderData{} // Limpar buffer
	buffer.mu.Unlock()

	delete(wsm.orderBuffers, accountID)
	wsm.bufferMu.Unlock()

	if len(orders) == 0 {
		return
	}

	// Filtrar ordens duplicadas: manter apenas a mais recente por orderId
	orderMap := make(map[string]OrderData)
	for _, order := range orders {
		existingOrder, exists := orderMap[order.OrderID]
		if !exists {
			// Primeira ocorrência deste orderId
			orderMap[order.OrderID] = order
		} else {
			// Comparar updatedTime para manter a mais recente
			existingUpdatedTime, err1 := strconv.ParseInt(existingOrder.UpdatedTime, 10, 64)
			currentUpdatedTime, err2 := strconv.ParseInt(order.UpdatedTime, 10, 64)
			
			// Se houver erro ao parsear, manter a existente
			if err1 != nil || err2 != nil {
				continue
			}
			
			// Se a ordem atual é mais recente, substituir
			if currentUpdatedTime > existingUpdatedTime {
				orderMap[order.OrderID] = order
			}
		}
	}

	// Converter o mapa de volta para slice
	orders = make([]OrderData, 0, len(orderMap))
	for _, order := range orderMap {
		orders = append(orders, order)
	}

	if len(orders) == 0 {
		return
	}

	// Usar a conexão ativa
	wsConn = activeConn

	// Mapa para rastrear ordens que foram notificadas como movidas
	movedOrders := make(map[string]bool)

	// Verificar ordens no banco e detectar mudanças de preço
	for _, order := range orders {
		// Serializar ordem para JSON
		orderJSON, err := json.Marshal(order)
		if err != nil {
			logger, _ := getLogger(accountID, wsConn.Account.Name)
			if logger != nil {
				logger.Log("[DEBUG] Erro ao serializar ordem %s: %v", order.OrderID, err)
			}
			continue
		}

		// Verificar se ordem já existe no banco
		existingOrderJSON, err := wsm.accountManager.GetOrder(order.OrderID)
		if err == nil && existingOrderJSON != "" {
			// Ordem existe, verificar se o preço mudou
			var existingOrder OrderData
			if err := json.Unmarshal([]byte(existingOrderJSON), &existingOrder); err == nil {
				// Usar getDisplayPrice para obter os preços corretos
				oldPriceStr := getDisplayPrice(existingOrder)
				newPriceStr := getDisplayPrice(order)
				
				// Comparar preços usando os valores corretos
				if oldPriceStr != newPriceStr {
					oldPrice, errOld := strconv.ParseFloat(oldPriceStr, 64)
					newPrice, errNew := strconv.ParseFloat(newPriceStr, 64)
					// Só notificar "ordem movida" se ambos os preços forem diferentes de 0
					if errOld != nil || errNew != nil || oldPrice == 0 || newPrice == 0 {
						// Salvar no banco abaixo; não marcar como movida
					} else {
						// Preço mudou - criar notificação especial
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

						qty, err := strconv.ParseFloat(order.Qty, 64)
						if err != nil {
							continue
						}

						messageText := fmt.Sprintf("📝 %s Ordem movida - %s %s%s %s\n   Preço: %s → %s (Qty: %s USD)",
							orderIcon, order.Symbol, reducePrefix, order.Side, order.OrderType, formatPriceCoin(oldPrice), formatPriceCoin(newPrice), formatPriceCoin(qty))

						wsm.sendNotificationWithType(wsConn, messageText, true, false)
						movedOrders[order.OrderID] = true

						logger, _ := getLogger(accountID, wsConn.Account.Name)
						if logger != nil {
							logger.Log("[DEBUG] Ordem %s movida de %.2f para %.2f", order.OrderID, oldPrice, newPrice)
						}
					}
				}
			}
		}

		orderNewPriceStr := getDisplayPrice(order)
		orderNewPrice, errNewOrder := strconv.ParseFloat(orderNewPriceStr, 64)

		if order.OrderType != "Market" && order.OrderStatus != "Filled" && order.OrderStatus != "PartiallyFilled" && errNewOrder == nil && orderNewPrice != 0 {
			// Salvar/atualizar ordem no banco
			if err := wsm.accountManager.SaveOrder(order.OrderID, accountID, string(orderJSON)); err != nil {
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("[DEBUG] Erro ao salvar ordem %s no banco: %v", order.OrderID, err)
				}
			}
		} else {
			// Remover do banco se existir
			if err := wsm.accountManager.DeleteOrder(order.OrderID); err != nil {
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("[DEBUG] Erro ao remover ordem preenchida %s do banco: %v", order.OrderID, err)
				}
			}
		}
	}

	// Filtrar ordens que foram notificadas como movidas
	finalOrders := make([]OrderData, 0)
	for _, order := range orders {
		if !movedOrders[order.OrderID] {
			finalOrders = append(finalOrders, order)
		}
	}
	orders = finalOrders

	if len(orders) == 0 {
		return
	}

	// Agrupar ordens por tipo (Side + OrderType + ReduceOnly)
	groups := make(map[string][]OrderData)
	for _, order := range orders {
		reducePrefix := ""
		if order.ReduceOnly {
			reducePrefix = "Reduce"
		}
		key := fmt.Sprintf("%s_%s_%s_%s", order.Symbol, reducePrefix, order.Side, order.OrderType)
		groups[key] = append(groups[key], order)
	}

	// Processar cada grupo
	for key, groupOrders := range groups {
		if len(groupOrders) == 0 {
			continue
		}

		// Pegar informações do primeiro para usar como base
		firstOrder := groupOrders[0]
		reducePrefix := ""
		if firstOrder.ReduceOnly {
			reducePrefix = "Reduce "
		}

		// Calcular range de preços e quantidade total
		var minPrice, maxPrice float64
		var totalQty float64

		for i, order := range groupOrders {
			// Usar avgPrice se for Market, senão usar Price
			priceStr := getDisplayPrice(order)
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				// Erro ao parsear preço - pular esta ordem
				continue
			}
			qty, err := strconv.ParseFloat(order.Qty, 64)
			if err != nil {
				// Erro ao parsear quantidade - pular esta ordem
				continue
			}

			if i == 0 {
				minPrice = price
				maxPrice = price
			} else {
				if price < minPrice {
					minPrice = price
				}
				if price > maxPrice {
					maxPrice = price
				}
			}
			totalQty += qty
		}

		// Obter preço de exibição para a primeira ordem
		displayPrice := getDisplayPrice(firstOrder)

		var orderIcon string
		if firstOrder.Side == "Buy" {
			orderIcon = "🟢" // Círculo verde para Buy
		} else {
			orderIcon = "🔴" // Círculo vermelho para Sell
		}

		// Construir mensagem
		var messageText string
		if len(groupOrders) == 1 {
			// Uma única ordem
			messageText = fmt.Sprintf("%s Nova ordem aberta - %s %s%s %s @ %s (Qty: %s USD)",
				orderIcon, firstOrder.Symbol, reducePrefix, firstOrder.Side, firstOrder.OrderType, displayPrice, formatPriceCoin(totalQty))
		} else {
			// Múltiplas ordens agrupadas (scale orders)
			if minPrice == maxPrice {
				// Todas no mesmo preço
				messageText = fmt.Sprintf("%s %d ordens %s%s %s agrupadas - %s @ %s (Qty Total: %s USD)",
					orderIcon, len(groupOrders), reducePrefix, firstOrder.Side, firstOrder.OrderType, firstOrder.Symbol, displayPrice, formatPriceCoin(totalQty))
			} else {
				// Range de preços
				messageText = fmt.Sprintf("%s %d ordens %s%s %s agrupadas - %s\n   Range: %s até %s\n   Qty Total: %s USD",
					orderIcon, len(groupOrders), reducePrefix, firstOrder.Side, firstOrder.OrderType, firstOrder.Symbol,
					formatPriceCoin(minPrice), formatPriceCoin(maxPrice), formatPriceCoin(totalQty))
			}
		}

		// Enviar notificação (ordem)
		wsm.sendNotificationWithType(wsConn, messageText, true, false)
		logger, _ := getLogger(accountID, wsConn.Account.Name)
		if logger != nil {
			logger.Log("[DEBUG] Processado grupo %s: %d ordens", key, len(groupOrders))
		}
	}
}

func (wsm *WebSocketManager) processCancelBuffer(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processCancelBuffer para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processCancelBuffer: %v", r)
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
	// Usar a conexão atual do mapa (pode ter mudado)
	activeConn := conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buffer, exists := wsm.cancelBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}

	buffer.mu.Lock()
	if len(buffer.orders) == 0 {
		buffer.mu.Unlock()
		wsm.bufferMu.Unlock()
		return
	}

	// Copiar ordens e limpar buffer
	orders := make([]OrderData, len(buffer.orders))
	copy(orders, buffer.orders)
	buffer.orders = []OrderData{} // Limpar buffer
	buffer.mu.Unlock()

	delete(wsm.cancelBuffers, accountID)
	wsm.bufferMu.Unlock()

	if len(orders) == 0 {
		return
	}

	// Usar a conexão ativa
	wsConn = activeConn

	// Remover todas as ordens do banco (sempre)
	for _, order := range orders {
		if err := wsm.accountManager.DeleteOrder(order.OrderID); err != nil {
			logger, _ := getLogger(accountID, wsConn.Account.Name)
			if logger != nil {
				logger.Log("[DEBUG] Erro ao remover ordem %s do banco: %v", order.OrderID, err)
			}
		}
	}

	// Notificar apenas ordens com preço válido (> 0), para evitar "Market @ 0"
	var ordersToNotify []OrderData
	for _, order := range orders {
		if hasValidDisplayPrice(order) {
			ordersToNotify = append(ordersToNotify, order)
		}
	}
	if len(ordersToNotify) == 0 {
		return
	}

	// Construir mensagem agrupada
	messageParts := []string{fmt.Sprintf("❌ %d ordens canceladas:", len(ordersToNotify))}
	for _, order := range ordersToNotify {
		reducePrefix := ""
		if order.ReduceOnly {
			reducePrefix = "Reduce "
		}
		displayPrice := getDisplayPrice(order)
		messageParts = append(messageParts, fmt.Sprintf("  • %s %s%s %s @ %s",
			order.Symbol, reducePrefix, order.Side, order.OrderType, displayPrice))
	}

	messageText := strings.Join(messageParts, "\n")

	// Enviar notificação (ordem)
	wsm.sendNotificationWithType(wsConn, messageText, true, false)
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Processado %d cancelamentos agrupados", len(orders))
	}
}

func (wsm *WebSocketManager) processStopOrder(wsConn *WebSocketConnection, order OrderData) {
	// Processar stop order imediatamente (sem delay)
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

	// Verificar se stop já existe no banco e detectar mudança de preço
	existingOrderJSON, err := wsm.accountManager.GetOrder(order.OrderID)
	orderMoved := false
	if err == nil && existingOrderJSON != "" {
		// Stop existe, verificar se o triggerPrice mudou
		var existingOrder OrderData
		if err := json.Unmarshal([]byte(existingOrderJSON), &existingOrder); err == nil {
			// Comparar triggerPrice; só notificar "stop movido" se triggerPrice != 0
			if existingOrder.TriggerPrice != order.TriggerPrice && triggerPrice != 0 {
				var stopIcon string
				if order.Side == "Buy" {
					stopIcon = "🟢"
				} else {
					stopIcon = "🔴"
				}

				oldPrice, _ := strconv.ParseFloat(existingOrder.TriggerPrice, 64)
				newPrice, _ := strconv.ParseFloat(order.TriggerPrice, 64)

				messageText := fmt.Sprintf("📝 %s Stop movido - %s %s%s %s\n   Preço: %s → %s (Qty: %s USD)",
					stopIcon, order.Symbol, reducePrefix, order.Side, order.OrderType, formatPriceCoin(oldPrice), formatPriceCoin(newPrice), formatPriceCoin(qty))

				wsm.sendNotificationWithType(wsConn, messageText, true, false)
				orderMoved = true

				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("[DEBUG] Stop %s movido de %.2f para %.2f", order.OrderID, oldPrice, newPrice)
				}
			}
		}
	}

	// Salvar/atualizar stop no banco
	orderJSON, err := json.Marshal(order)
	if err == nil {
		// Se a ordem está Filled ou PartiallyFilled, remover do banco e não processar
		if order.OrderStatus != "Filled" && order.OrderStatus != "PartiallyFilled" && triggerPrice != 0 {
			if err := wsm.accountManager.SaveOrder(order.OrderID, wsConn.AccountID, string(orderJSON)); err != nil {
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("[DEBUG] Erro ao salvar stop %s no banco: %v", order.OrderID, err)
				}
			}
		} else {
			if err := wsm.accountManager.DeleteOrder(order.OrderID); err != nil {
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("[DEBUG] Erro ao remover stop preenchido %s do banco: %v", order.OrderID, err)
				}
			}
		}
	}

	// Se a ordem foi notificada como movida, não enviar notificação normal
	if orderMoved {
		return
	}

	// Cancelar notificação se triggerPrice for 0
	if triggerPrice == 0 {
		return
	}

	// Escolher ícone baseado no Side (Buy = verde, Sell = vermelho)
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢" // Círculo verde para Buy
	} else {
		stopIcon = "🔴" // Círculo vermelho para Sell
	}

	// Formatar mensagem do stop
	messageText := fmt.Sprintf("%s Stop %s%s %s - %s @ %s (Qty: %s USD)",
		stopIcon, reducePrefix, order.Side, order.OrderType, order.Symbol, formatPriceCoin(triggerPrice), formatPriceCoin(qty))

	// Enviar notificação imediatamente (ordem)
	wsm.sendNotificationWithType(wsConn, messageText, true, false)
	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Stop order processado - %s %s%s @ %.2f", order.Symbol, reducePrefix, order.Side, triggerPrice)
	}
}

func (wsm *WebSocketManager) processStopCancellation(wsConn *WebSocketConnection, order OrderData) {
	// Processar cancelamento de stop não triggerado imediatamente (sem delay)
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

	// Remover stop do banco se existir
	if err := wsm.accountManager.DeleteOrder(order.OrderID); err != nil {
		logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
		if logger != nil {
			logger.Log("[DEBUG] Erro ao remover stop %s do banco: %v", order.OrderID, err)
		}
	}

	// Se triggerPrice for 0, cancelar notificação
	if triggerPrice == 0 {
		return
	}

	// Escolher ícone baseado no Side (Buy = verde, Sell = vermelho)
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢" // Círculo verde para Buy
	} else {
		stopIcon = "🔴" // Círculo vermelho para Sell
	}

	// Formatar mensagem do cancelamento de stop
	messageText := fmt.Sprintf("❌ %s Stop %s%s %s **CANCELADO** - %s @ %s (Qty: %s USD)",
		stopIcon, reducePrefix, order.Side, order.OrderType, order.Symbol, formatPriceCoin(triggerPrice), formatPriceCoin(qty))

	// Enviar notificação imediatamente (ordem)
	wsm.sendNotificationWithType(wsConn, messageText, true, false)
	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Stop cancelado processado - %s %s%s @ %.2f", order.Symbol, reducePrefix, order.Side, triggerPrice)
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
		wsm.addExecutionToBuffer(wsConn.AccountID, wsConn)
		// Adicionar ao buffer de notificação de execuções (5s: Discord + Google Sheets)
		wsm.addExecutionToNotificationBuffer(wsConn.AccountID, execData, wsConn)
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

func (wsm *WebSocketManager) addExecutionToBuffer(accountID int64, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.executionBuffers[accountID]
	if !exists {
		buffer = &ExecutionBuffer{accountID: accountID}
		wsm.executionBuffers[accountID] = buffer
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
				fmt.Fprintf(os.Stderr, "[PANIC] processExecutionBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processExecutionBuffer(accountID, wsConn)
	})
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Execução recebida, iniciando/resetando timer de 15 minutos para Discord")
	}
}

func (wsm *WebSocketManager) resetSheetsTimer(accountID int64, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.executionBuffers[accountID]
	if !exists {
		buffer = &ExecutionBuffer{accountID: accountID}
		wsm.executionBuffers[accountID] = buffer
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

func (wsm *WebSocketManager) addExecutionToNotificationBuffer(accountID int64, exec ExecutionData, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buf, exists := wsm.executionNotificationBuffers[accountID]
	if !exists {
		buf = &ExecutionNotificationBuffer{accountID: accountID}
		wsm.executionNotificationBuffers[accountID] = buf
	}
	wsm.bufferMu.Unlock()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	if buf.timer != nil {
		buf.timer.Stop()
	}
	buf.executions = append(buf.executions, exec)
	buf.timer = time.AfterFunc(5*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] flushExecutionNotificationBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.flushExecutionNotificationBuffer(accountID)
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

func (wsm *WebSocketManager) processExecutionBuffer(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processExecutionBuffer para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processExecutionBuffer: %v", r)
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

func (wsm *WebSocketManager) flushExecutionNotificationBuffer(accountID int64) {
	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	wsConn := conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buf, exists := wsm.executionNotificationBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}
	buf.mu.Lock()
	executions := make([]ExecutionData, len(buf.executions))
	copy(executions, buf.executions)
	buf.executions = buf.executions[:0]
	if buf.timer != nil {
		buf.timer.Stop()
		buf.timer = nil
	}
	buf.mu.Unlock()
	wsm.bufferMu.Unlock()

	if len(executions) == 0 {
		return
	}

	logger, _ := getLogger(accountID, wsConn.Account.Name)

	// Discord: uma mensagem com todas as execuções
	if wsConn.Account.WebhookURLExecutions != "" {
		var parts []string
		for _, e := range executions {
			coin := symbolToCoin(e.Symbol)
			price, _ := strconv.ParseFloat(e.ExecPrice, 64)
			qtyUsd, _ := strconv.ParseFloat(e.ExecQty, 64)    // valor em USD

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

	// Google Sheets: agrupar por moeda, uma requisição por moeda
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
