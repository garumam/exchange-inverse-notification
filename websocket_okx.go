package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const okxPrivateWSURL = "wss://ws.okx.com:8443/ws/v5/private"
const okxBusinessWSURL = "wss://ws.okx.com:8443/ws/v5/business"
const okxContractValueUsd = 100

// okxBusinessRunner: garante uma única goroutine de conexão business por conta e evita duplicação.
var (
	okxBusinessMu      sync.Mutex
	okxBusinessRunning = make(map[int64]bool) // accountID -> runner ativo
)

// okxMetadata representa o JSON em metadata para OKX (passphrase).
type okxMetadata struct {
	Passphrase string `json:"passphrase"`
}

func getOKXPassphrase(metadata string) (string, error) {
	if metadata == "" {
		return "", fmt.Errorf("metadata vazio: passphrase OKX é obrigatório")
	}
	var m okxMetadata
	if err := json.Unmarshal([]byte(metadata), &m); err != nil {
		return "", fmt.Errorf("metadata inválido: %w", err)
	}
	p := strings.TrimSpace(m.Passphrase)
	if p == "" {
		return "", fmt.Errorf("passphrase não encontrado no metadata")
	}
	return p, nil
}

// connectAndListenOKX conecta ao WebSocket privado da OKX, faz login, inscreve e lê mensagens.
// Apenas instType SWAP (equivalente inverse Bybit). Account e positions: só processa event_update.
func (wsm *WebSocketManager) connectAndListenOKX(wsConn *WebSocketConnection, successChan chan<- bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] connectAndListenOKX para conta %d: %v\n", wsConn.AccountID, r)
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	passphrase, err := getOKXPassphrase(wsConn.Account.Metadata)
	if err != nil {
		if logger != nil {
			logger.Log("OKX passphrase inválido: %v", err)
		}
		return err
	}

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(okxPrivateWSURL, nil)
	if err != nil {
		if logger != nil {
			logger.Log("Erro ao conectar OKX: %v", err)
		}
		return fmt.Errorf("erro ao conectar OKX: %w", err)
	}

	wsConn.mu.Lock()
	if wsConn.Conn != nil {
		wsConn.Conn.Close()
	}
	wsConn.Conn = conn
	wsConn.mu.Unlock()

	defer func() {
		wsConn.mu.Lock()
		if wsConn.Conn == conn {
			wsConn.Conn.Close()
			wsConn.Conn = nil
		}
		wsConn.mu.Unlock()
		conn.Close()
	}()

	if err := wsm.loginOKX(conn, wsConn.Account, passphrase); err != nil {
		if logger != nil {
			logger.Log("Erro no login OKX: %v", err)
		}
		return fmt.Errorf("erro no login OKX: %w", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := wsm.subscribeOKX(conn); err != nil {
		if logger != nil {
			logger.Log("Erro ao inscrever OKX: %v", err)
		}
		return fmt.Errorf("erro ao inscrever OKX: %w", err)
	}

	// Conexão paralela ao endpoint business para canal orders-algo (stops).
	// Só inicia se ainda não houver runner para esta conta; se a principal caiu e reconectou e a business segue ativa, não duplica.
	wsm.ensureOKXBusinessConnection(wsConn, passphrase)

	if successChan != nil {
		select {
		case successChan <- true:
		default:
		}
	}

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	pingStopChan := make(chan struct{})
	go wsm.pingLoop(conn, pingStopChan)
	defer close(pingStopChan)

	for {
		select {
		case <-wsConn.StopChan:
			return nil
		default:
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case <-wsConn.StopChan:
					return nil
				default:
				}
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					if logger != nil {
						logger.Log("Conexão OKX fechada normalmente pelo servidor")
					}
					return nil
				}
				wsConn.mu.Lock()
				if wsConn.Conn == conn {
					wsConn.Conn = nil
				}
				wsConn.mu.Unlock()
				return fmt.Errorf("erro na leitura OKX: %w", err)
			}
			if messageType != websocket.TextMessage {
				continue
			}
			wsm.handleOKXMessage(wsConn, message, logger)
		}
	}
}

// loginOKX envia op "login" com apiKey, passphrase, timestamp (segundos), sign (Base64 HMAC-SHA256).
func (wsm *WebSocketManager) loginOKX(conn *websocket.Conn, account *BybitAccount, passphrase string) error {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	prehash := ts + "GET" + "/users/self/verify"
	sign := signOKX(prehash, strings.TrimSpace(account.APISecret))
	loginMsg := map[string]interface{}{
		"op": "login",
		"args": []map[string]string{
			{
				"apiKey":     strings.TrimSpace(account.APIKey),
				"passphrase": passphrase,
				"timestamp":  ts,
				"sign":       sign,
			},
		},
	}
	if err := conn.WriteJSON(loginMsg); err != nil {
		return err
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var resp map[string]interface{}
	if err := conn.ReadJSON(&resp); err != nil {
		return fmt.Errorf("erro ao ler resposta do login: %w", err)
	}
	if event, _ := resp["event"].(string); event == "error" {
		code, _ := resp["code"].(string)
		msg, _ := resp["msg"].(string)
		return fmt.Errorf("login OKX falhou: %s %s", code, msg)
	}
	if event, _ := resp["event"].(string); event != "login" {
		return fmt.Errorf("resposta de login inesperada: %v", resp)
	}
	return nil
}

func signOKX(prehash, secret string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(prehash))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// subscribeOKX inscreve em account, positions (SWAP), orders (SWAP). Sem instFamily.
func (wsm *WebSocketManager) subscribeOKX(conn *websocket.Conn) error {
	args := []map[string]interface{}{
		{"channel": "account", "extraParams": "{\"updateInterval\":\"0\"}"},
		{"channel": "positions", "instType": "SWAP"},
		{"channel": "orders", "instType": "SWAP"},
	}
	msg := map[string]interface{}{
		"op":   "subscribe",
		"args": args,
	}
	return conn.WriteJSON(msg)
}

// subscribeOKXBusiness inscreve no canal orders-algo (trigger/stops) no endpoint business.
func (wsm *WebSocketManager) subscribeOKXBusiness(conn *websocket.Conn) error {
	msg := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]interface{}{
			{"channel": "orders-algo", "instType": "SWAP"},
		},
	}
	return conn.WriteJSON(msg)
}

// ensureOKXBusinessConnection inicia a goroutine de conexão business (orders-algo) apenas se ainda não
// existir uma runner para esta conta. Se a conexão principal cair e reconectar, e a business ainda
// estiver ativa, não duplica. A runner faz retry independente quando a conexão business cair.
func (wsm *WebSocketManager) ensureOKXBusinessConnection(wsConn *WebSocketConnection, passphrase string) {
	okxBusinessMu.Lock()
	if okxBusinessRunning[wsConn.AccountID] {
		okxBusinessMu.Unlock()
		return
	}
	okxBusinessRunning[wsConn.AccountID] = true
	okxBusinessMu.Unlock()

	go wsm.runOKXBusinessWithRetry(wsConn, passphrase)
}

// runOKXBusinessWithRetry mantém a conexão OKX business com o mesmo processo de reconexão da principal
// (backoff exponencial, retries). Sai apenas quando wsConn.StopChan for fechado.
func (wsm *WebSocketManager) runOKXBusinessWithRetry(wsConn *WebSocketConnection, passphrase string) {
	defer func() {
		okxBusinessMu.Lock()
		delete(okxBusinessRunning, wsConn.AccountID)
		okxBusinessMu.Unlock()
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	retryDelay := 5 * time.Second
	maxRetryDelay := 1 * time.Minute
	initialRetryDelay := retryDelay
	consecutiveFailures := 0
	const maxConsecutiveFailures = 10

	for {
		select {
		case <-wsConn.StopChan:
			return
		default:
		}

		var stopped, wasConnected bool
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[PANIC] OKX business para conta %d: %v\n", wsConn.AccountID, r)
					if logger != nil {
						logger.Log("PANIC na conexão OKX business (reiniciando fluxo de reconexão): %v", r)
					}
					stopped = false
					wasConnected = true
				}
			}()
			stopped, wasConnected = wsm.connectAndListenOKXBusiness(wsConn, passphrase)
		}()
		if stopped {
			return
		}

		select {
		case <-wsConn.StopChan:
			return
		default:
		}

		if wasConnected {
			consecutiveFailures = 0
			retryDelay = initialRetryDelay
		} else {
			consecutiveFailures++
		}
		if logger != nil {
			logger.Log("Conexão OKX business caiu, reconectando em %v (falhas consecutivas: %d)...", retryDelay, consecutiveFailures)
		}

		if consecutiveFailures >= maxConsecutiveFailures {
			if logger != nil {
				logger.Log("Muitas falhas consecutivas na OKX business (%d), aguardando 30s antes de reconectar...", consecutiveFailures)
			}
			consecutiveFailures = 0
			retryDelay = initialRetryDelay
			select {
			case <-wsConn.StopChan:
				return
			case <-time.After(30 * time.Second):
			}
			continue
		}

		select {
		case <-wsConn.StopChan:
			return
		case <-time.After(retryDelay):
			if retryDelay < maxRetryDelay {
				retryDelay *= 2
			}
		}
	}
}

// connectAndListenOKXBusiness conecta ao WebSocket business da OKX, faz login, inscreve em orders-algo e lê mensagens.
// Retorna (stopped=true) se saiu por StopChan; (stopped=false, wasConnected=X) se saiu por erro (wasConnected indica se já tinha conectado, para resetar backoff).
func (wsm *WebSocketManager) connectAndListenOKXBusiness(wsConn *WebSocketConnection, passphrase string) (stopped bool, wasConnected bool) {
	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.Dial(okxBusinessWSURL, nil)
	if err != nil {
		if logger != nil {
			logger.Log("Erro ao conectar OKX business: %v", err)
		}
		return false, false
	}
	defer conn.Close()

	if err := wsm.loginOKX(conn, wsConn.Account, passphrase); err != nil {
		if logger != nil {
			logger.Log("Erro no login OKX business: %v", err)
		}
		return false, false
	}
	time.Sleep(500 * time.Millisecond)

	if err := wsm.subscribeOKXBusiness(conn); err != nil {
		if logger != nil {
			logger.Log("Erro ao inscrever OKX business orders-algo: %v", err)
		}
		return false, false
	}

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	pingStopChan := make(chan struct{})
	go wsm.pingLoop(conn, pingStopChan)
	defer close(pingStopChan)

	for {
		select {
		case <-wsConn.StopChan:
			return true, false
		default:
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case <-wsConn.StopChan:
					return true, false
				default:
				}
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					if logger != nil {
						logger.Log("Conexão OKX business fechada normalmente")
					}
					return false, true
				}
				if logger != nil {
					logger.Log("Erro na leitura OKX business: %v", err)
				}
				return false, true
			}
			if messageType != websocket.TextMessage {
				continue
			}
			wsm.handleOKXAlgoMessage(wsConn, message, logger)
		}
	}
}

// handleOKXMessage processa uma mensagem OKX: log raw, depois normaliza e chama handlers Bybit.
func (wsm *WebSocketManager) handleOKXMessage(wsConn *WebSocketConnection, raw []byte, logger interface{ Log(string, ...interface{}) }) {
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return
	}
	event, _ := generic["event"].(string)
	if event == "login" || event == "subscribe" || event == "unsubscribe" || event == "error" || event == "channel-conn-count" || event == "channel-conn-count-error" {
		return
	}

	arg, _ := generic["arg"].(map[string]interface{})
	channel, _ := arg["channel"].(string)

	eventType, _ := generic["eventType"].(string)
	if channel == "account" || channel == "positions" {
		if eventType != "event_update" {
			return
		}
	}

	// Log de todas as mensagens originais válidas da OKX (dados de push)
	if channel == "account" || channel == "positions" || channel == "orders" {
		if logger != nil {
			logger.Log("[OKX raw] %s", string(raw))
		}
	}

	dataSlice, ok := generic["data"].([]interface{})
	if !ok || len(dataSlice) == 0 {
		return
	}

	switch channel {
	case "account":
		wsm.processOKXAccount(wsConn, dataSlice)
	case "positions":
		wsm.processOKXPositions(wsConn, dataSlice)
	case "orders":
		wsm.processOKXOrders(wsConn, dataSlice, logger)
	}
}

// handleOKXAlgoMessage processa mensagens do canal orders-algo (endpoint business). Só envia para handleOrderMessage ordens com state live (Untriggered) ou canceled/order_failed/partially_failed (Deactivated).
func (wsm *WebSocketManager) handleOKXAlgoMessage(wsConn *WebSocketConnection, raw []byte, logger interface{ Log(string, ...interface{}) }) {
	var generic map[string]interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return
	}
	event, _ := generic["event"].(string)
	if event == "login" || event == "subscribe" || event == "unsubscribe" || event == "error" || event == "channel-conn-count" || event == "channel-conn-count-error" {
		return
	}
	arg, _ := generic["arg"].(map[string]interface{})
	channel, _ := arg["channel"].(string)
	if channel != "orders-algo" {
		return
	}
	dataSlice, ok := generic["data"].([]interface{})
	if !ok || len(dataSlice) == 0 {
		return
	}
	if logger != nil {
		logger.Log("[OKX raw] orders-algo %s", string(raw))
	}
	var orders []OrderData
	for _, d := range dataSlice {
		obj, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		instType, _ := obj["instType"].(string)
		if instType != "SWAP" {
			continue
		}
		orderData, ok := okxAlgoOrderToBybit(obj)
		if !ok {
			continue
		}
		orders = append(orders, orderData)
	}
	if len(orders) > 0 {
		msg := BybitOrderMessage{Data: orders}
		wsm.handleOrderMessage(wsConn, msg)
	}
}

// okxAlgoOrderToBybit converte um item do canal orders-algo para OrderData. Retorna (order, true) apenas para state live (Untriggered) ou canceled/order_failed/partially_failed (Deactivated).
func okxAlgoOrderToBybit(obj map[string]interface{}) (OrderData, bool) {
	instId, _ := obj["instId"].(string)
	state, _ := obj["state"].(string)
	algoId, _ := obj["algoId"].(string)
	side, _ := obj["side"].(string)
	sz, _ := obj["sz"].(string)
	notionalUsd, _ := obj["notionalUsd"].(string)
	triggerPx, _ := obj["triggerPx"].(string)
	slTriggerPx, _ := obj["slTriggerPx"].(string)
	tpTriggerPx, _ := obj["tpTriggerPx"].(string)
	ordPx, _ := obj["ordPx"].(string)
	reduceOnly, _ := obj["reduceOnly"].(string)
	cTime, _ := obj["cTime"].(string)
	uTime, _ := obj["uTime"].(string)

	symbol := okxInstIdToSymbol(instId)
	qty := notionalUsd
	if qty == "" {
		qty = sz
	}
	triggerPrice := triggerPx
	if triggerPrice == "" && slTriggerPx != "" {
		triggerPrice = slTriggerPx
	}
	if triggerPrice == "" && tpTriggerPx != "" {
		triggerPrice = tpTriggerPx
	}
	orderStatus := ""
	switch state {
	case "live":
		orderStatus = "Untriggered"
	case "canceled", "order_failed", "partially_failed":
		orderStatus = "Deactivated"
	default:
		return OrderData{}, false
	}
	orderType := "Limit"
	if ordPx == "" || ordPx == "-1" {
		orderType = "Market"
	}
	price := ordPx
	if price == "-1" {
		price = ""
	}

	orderData := OrderData{
		Category:      "inverse",
		OrderID:       "algo_" + algoId, // concatenando o algo_ para evitar colisão com os ids da ordem pois é salvo no banco
		Symbol:        symbol,
		Side:          okxSideToBybit(side),
		OrderType:     orderType,
		OrderStatus:   orderStatus,
		Price:         price,
		Qty:           qty,
		CreatedTime:   cTime,
		UpdatedTime:   uTime,
		ReduceOnly:    reduceOnly == "true",
		RejectReason:  "EC_NoError",
		StopOrderType: "Stop",
		TriggerPrice:  triggerPrice,
	}
	return orderData, true
}

func okxInstIdToSymbol(instId string) string {
	// BTC-USD-SWAP -> BTCUSD
	s := strings.ReplaceAll(instId, "-", "")
	if strings.HasSuffix(strings.ToUpper(s), "SWAP") {
		s = s[:len(s)-4]
	}
	return s
}

func okxStateToOrderStatus(state string) string {
	switch state {
	case "live":
		return "New"
	case "partially_filled":
		return "PartiallyFilled"
	case "filled":
		return "Filled"
	case "canceled", "mmp_canceled":
		return "Cancelled"
	default:
		return state
	}
}

func okxSideToBybit(side string) string {
	if side == "buy" {
		return "Buy"
	}
	if side == "sell" {
		return "Sell"
	}
	return side
}

func okxNormalizePositionNumber(pos string) string {
	clean := strings.ReplaceAll(pos, "-", "")
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return "0" // ou outro valor default em caso de erro
	}
	result := f * float64(okxContractValueUsd)
	return strconv.FormatFloat(result, 'f', -1, 64)
}

func okxPositionSideToBybit(side string, pos string) string {
	if side == "long" {
		return "Buy"
	}
	if side == "short" {
		return "Sell"
	}
	if side == "net" {
		f, err := strconv.ParseFloat(pos, 64)
		if err != nil {
			return "Sell" // ou um valor default
		}
		if f > 0 {
			return "Buy"
		}
		return "Sell"
	}
	return side
}

func okxOrdTypeToBybit(ordType string) string {
	switch ordType {
	case "limit":
		return "Limit"
	case "market":
		return "Market"
	default:
		return ordType
	}
}

func (wsm *WebSocketManager) processOKXAccount(wsConn *WebSocketConnection, dataSlice []interface{}) {
	for _, d := range dataSlice {
		obj, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		details, _ := obj["details"].([]interface{})
		if len(details) == 0 {
			continue
		}
		var coins []CoinBalance
		for _, det := range details {
			detMap, ok := det.(map[string]interface{})
			if !ok {
				continue
			}
			ccy, _ := detMap["ccy"].(string)
			eq, _ := detMap["eq"].(string)
			eqUsd, _ := detMap["eqUsd"].(string)

			coins = append(coins, CoinBalance{
				Coin:                ccy,
				Equity:              eq,
				UsdValue:            eqUsd,
			})
		}
		totalEq, _ := obj["totalEq"].(string)
		wd := WalletData{
			AccountType:           "UNIFIED",
			TotalEquity:           totalEq,
			TotalWalletBalance:    totalEq,
			Coin:                  coins,
		}
		msg := BybitWalletMessage{Data: []WalletData{wd}}
		wsm.handleWalletMessage(wsConn, msg)
	}
}

func (wsm *WebSocketManager) processOKXPositions(wsConn *WebSocketConnection, dataSlice []interface{}) {
	var positions []PositionData
	for _, d := range dataSlice {
		obj, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		instType, _ := obj["instType"].(string)
		if instType != "SWAP" {
			continue
		}
		instId, _ := obj["instId"].(string)
		pos, _ := obj["pos"].(string)
		if pos == "" || pos == "0" {
			continue
		}
		posSide, _ := obj["posSide"].(string)
		side := okxPositionSideToBybit(posSide, pos)
		avgPx, _ := obj["avgPx"].(string)
		markPx, _ := obj["markPx"].(string)

		symbol := okxInstIdToSymbol(instId)
		positions = append(positions, PositionData{
			Symbol:        symbol,
			Side:          side,
			Size:          okxNormalizePositionNumber(pos),
			EntryPrice:    avgPx,
			MarkPrice:     markPx,
			Category:      "inverse",
		})
	}
	if len(positions) > 0 {
		msg := BybitPositionMessage{Data: positions}
		wsm.handlePositionMessage(wsConn, msg)
	}
}

// okxOrderToBybit converte uma ordem OKX para o formato Bybit, incluindo mapeamento de stops (source "7").
func okxOrderToBybit(obj map[string]interface{}, symbol string) (orderData OrderData, isStopTriggeredFill bool) {
	ordId, _ := obj["ordId"].(string)
	state, _ := obj["state"].(string)
	side, _ := obj["side"].(string)
	ordType, _ := obj["ordType"].(string)
	px, _ := obj["px"].(string)
	avgPx, _ := obj["avgPx"].(string)
	notionalUsd, _ := obj["notionalUsd"].(string)
	sz, _ := obj["sz"].(string)
	cTime, _ := obj["cTime"].(string)
	uTime, _ := obj["uTime"].(string)
	reduceOnly, _ := obj["reduceOnly"].(string)
	source, _ := obj["source"].(string)
	slTriggerPx, _ := obj["slTriggerPx"].(string)
	tpTriggerPx, _ := obj["tpTriggerPx"].(string)
	lastPx, _ := obj["lastPx"].(string)

	qty := notionalUsd
	if qty == "" {
		qty = sz
	}

	orderStatus := okxStateToOrderStatus(state)
	stopOrderType := ""
	triggerPrice := ""
	createType := ""

	isStop := source == "7" // "7" = The normal order triggered by the TP/SL order (OKX)

	if isStop {
		switch state {
		case "live":
			// Stop ainda não executado → Bybit "Untriggered"
			orderStatus = "Untriggered"
			stopOrderType = "Stop"
			if slTriggerPx != "" {
				triggerPrice = slTriggerPx
			} else if tpTriggerPx != "" {
				triggerPrice = tpTriggerPx
			} else {
				triggerPrice = lastPx
			}
		case "canceled", "mmp_canceled":
			// Stop cancelado → Bybit "Deactivated"
			orderStatus = "Deactivated"
			stopOrderType = "Stop"
			if slTriggerPx != "" {
				triggerPrice = slTriggerPx
			} else if tpTriggerPx != "" {
				triggerPrice = tpTriggerPx
			} else {
				triggerPrice = lastPx
			}
		case "filled", "partially_filled":
			// Stop executado → marcar como CreateByStopOrder para não duplicar notificação de ordem
			orderStatus = "Triggered"
			createType = "CreateByStopOrder"
			isStopTriggeredFill = true
		}
	}

	orderData = OrderData{
		Category:      "inverse",
		OrderID:       ordId,
		Symbol:        symbol,
		Side:          okxSideToBybit(side),
		OrderType:     okxOrdTypeToBybit(ordType),
		OrderStatus:   orderStatus,
		Price:         px,
		AvgPrice:      avgPx,
		Qty:           qty,
		CreatedTime:   cTime,
		UpdatedTime:   uTime,
		ReduceOnly:    reduceOnly == "true",
		RejectReason:  "EC_NoError",
		StopOrderType: stopOrderType,
		TriggerPrice:  triggerPrice,
		CreateType:    createType,
	}
	return orderData, isStopTriggeredFill
}

func (wsm *WebSocketManager) processOKXOrders(wsConn *WebSocketConnection, dataSlice []interface{}, logger interface{ Log(string, ...interface{}) }) {
	var orders []OrderData
	var executions []ExecutionData
	for _, d := range dataSlice {
		obj, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		instType, _ := obj["instType"].(string)
		if instType != "SWAP" {
			continue
		}
		instId, _ := obj["instId"].(string)
		state, _ := obj["state"].(string)
		side, _ := obj["side"].(string)
		ordType, _ := obj["ordType"].(string)
		fillSz, _ := obj["fillSz"].(string)
		fillPx, _ := obj["fillPx"].(string)
		fillTime, _ := obj["fillTime"].(string)
		tradeId, _ := obj["tradeId"].(string)

		symbol := okxInstIdToSymbol(instId)
		orderData, isStopTriggeredFill := okxOrderToBybit(obj, symbol)
		// Não enviar lifecycle de stop (criação/alteracao/remocao) pelo canal orders; isso vem do canal orders-algo (business).
		source, _ := obj["source"].(string)
		if source != "7" {
			orders = append(orders, orderData)
		}

		// Execução: quando há fill (tradeId + fillSz/fillPx). Para exibição em USD usamos fillNotionalUsd se disponível.
		execQty := fillSz

		if (state == "filled" || state == "partially_filled") && tradeId != "" && execQty != "" && fillPx != "" {

			execQtyF, _ := strconv.ParseFloat(execQty, 64)
			execQtyF = execQtyF * okxContractValueUsd
			fillPxF, _ := strconv.ParseFloat(fillPx, 64)
			execValue := "0"
			if fillPxF != 0 {
				execValue = strconv.FormatFloat(execQtyF/fillPxF, 'f', 8, 64)
			}

			execQty = strconv.Itoa(int(execQtyF))
			
			createType := ""

			if isStopTriggeredFill {
				createType = "CreateByStopOrder"
			}
			
			executions = append(executions, ExecutionData{
				Category:   "inverse",
				Symbol:     symbol,
				ExecType:   "Trade",
				ExecPrice:  fillPx,
				ExecQty:    execQty,
				ExecValue:  execValue,
				Side:       okxSideToBybit(side),
				OrderID:    orderData.OrderID,
				OrderType:  okxOrdTypeToBybit(ordType),
				ExecTime:   fillTime,
				CreateType: createType,
			})
		}
		_ = isStopTriggeredFill
	}
	if len(orders) > 0 {
		msg := BybitOrderMessage{Data: orders}
		wsm.handleOrderMessage(wsConn, msg)
	}
	for _, exec := range executions {
		msg := BybitExecutionMessage{Data: []ExecutionData{exec}}
		wsm.handleExecutionMessage(wsConn, msg)
	}
}
