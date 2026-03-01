package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const bybitWSURL = "wss://stream.bybit.com/v5/private"

// connectAndListenBybit conecta ao WebSocket da Bybit, autentica, inscreve e lê mensagens.
func (wsm *WebSocketManager) connectAndListenBybit(wsConn *WebSocketConnection, successChan chan<- bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] connectAndListenBybit para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar o panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em connectAndListenBybit: %v", r)
				}
			}()
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	logger, logErr := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logErr != nil {
		fmt.Fprintf(os.Stderr, "ERRO: Não foi possível criar logger para conta %d: %v\n", wsConn.AccountID, logErr)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(bybitWSURL, nil)
	if err != nil {
		if logger != nil {
			logger.Log("Erro ao conectar: %v", err)
		}
		return fmt.Errorf("erro ao conectar: %w", err)
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

	if err := wsm.authenticateBybit(conn, wsConn.Account); err != nil {
		if logger != nil {
			logger.Log("Erro na autenticação: %v", err)
		}
		return fmt.Errorf("erro na autenticação: %w", err)
	}

	time.Sleep(1 * time.Second)

	subscribeMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{"order", "execution", "position", "wallet"},
	}
	if err := conn.WriteJSON(subscribeMsg); err != nil {
		if logger != nil {
			logger.Log("Erro ao inscrever: %v", err)
		}
		return fmt.Errorf("erro ao inscrever: %w", err)
	}

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
						logger.Log("Conexão fechada normalmente pelo servidor")
					}
					return nil
				}
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					if logger != nil {
						logger.Log("Erro inesperado de fechamento: %v", err)
					}
					wsConn.mu.Lock()
					if wsConn.Conn == conn {
						wsConn.Conn = nil
					}
					wsConn.mu.Unlock()
					return fmt.Errorf("erro ao ler mensagem: %w", err)
				}
				wsConn.mu.Lock()
				if wsConn.Conn == conn {
					wsConn.Conn = nil
				}
				wsConn.mu.Unlock()
				return fmt.Errorf("erro na leitura: %w", err)
			}
			if messageType == websocket.TextMessage {
				wsm.handleMessage(wsConn, message)
			}
		}
	}
}

func (wsm *WebSocketManager) authenticateBybit(conn *websocket.Conn, account *BybitAccount) error {
	apiKey := strings.TrimSpace(account.APIKey)
	apiSecret := strings.TrimSpace(account.APISecret)
	expires := time.Now().UnixNano()/1e6 + 10000
	signatureString := fmt.Sprintf("GET/realtime%d", expires)
	mac := hmac.New(sha256.New, []byte(apiSecret))
	mac.Write([]byte(signatureString))
	signature := hex.EncodeToString(mac.Sum(nil))
	authMsg := map[string]interface{}{
		"req_id": uuid.New().String(),
		"op":     "auth",
		"args":   []interface{}{apiKey, expires, signature},
	}
	jsonData, err := json.Marshal(authMsg)
	if err != nil {
		return fmt.Errorf("erro ao serializar mensagem de autenticação: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, jsonData); err != nil {
		return fmt.Errorf("erro ao enviar mensagem de autenticação: %w", err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var authResponse map[string]interface{}
	if err := conn.ReadJSON(&authResponse); err != nil {
		return fmt.Errorf("erro ao ler resposta de autenticação: %w", err)
	}
	if success, ok := authResponse["success"].(bool); ok && !success {
		retMsg, _ := authResponse["ret_msg"].(string)
		return fmt.Errorf("autenticação falhou: %s (resposta: %v)", retMsg, authResponse)
	}
	if success, ok := authResponse["success"].(bool); ok && success {
		logger, _ := getLogger(account.ID, account.Name)
		if logger != nil {
			logger.Log("✅ Autenticação bem-sucedida")
		}
		return nil
	}
	return fmt.Errorf("resposta de autenticação inesperada: %v", authResponse)
}
