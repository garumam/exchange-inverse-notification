package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// HTTPQueue gerencia workers de envio por type (discord / google_sheets).
type HTTPQueue struct {
	db      *Database
	wake    map[string]chan struct{}
	started bool
	mu      sync.Mutex
}

var globalHTTPQueue *HTTPQueue

// NewHTTPQueue cria a fila e prepara canais de wake por type.
func NewHTTPQueue(db *Database) *HTTPQueue {
	q := &HTTPQueue{
		db: db,
		wake: map[string]chan struct{}{
			HTTPSendTypeDiscord:      make(chan struct{}, 1),
			HTTPSendTypeGoogleSheets: make(chan struct{}, 1),
		},
	}
	globalHTTPQueue = q
	return q
}

// Start inicia um worker dedicado por type, com restart após panic.
func (q *HTTPQueue) Start() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started {
		return
	}
	q.started = true
	q.startWorker(HTTPSendTypeDiscord)
	q.startWorker(HTTPSendTypeGoogleSheets)
}

// startWorker sobe uma goroutine que reinicia runWorker se houver panic.
func (q *HTTPQueue) startWorker(sendType string) {
	go func() {
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						logHTTPQueue(0, "[http_send_queue] panic no worker type=%s: %v", sendType, r)
					}
				}()
				q.runWorker(sendType)
			}()
			// Evita loop apertado se panicar de forma recorrente.
			time.Sleep(5 * time.Second)
		}
	}()
}

func (q *HTTPQueue) signal(sendType string) {
	ch, ok := q.wake[sendType]
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Enqueue valida, persiste e acorda o worker do type.
func (q *HTTPQueue) Enqueue(accountID int64, sendType, url, payload string) error {
	_, err := q.db.EnqueueHTTPSend(accountID, sendType, url, payload)
	if err != nil {
		return err
	}
	q.signal(sendType)
	return nil
}

func (q *HTTPQueue) runWorker(sendType string) {
	wake := q.wake[sendType]
	for {
		for {
			item, err := q.db.ClaimNextHTTPSend(sendType)
			if err != nil {
				logHTTPQueue(0, "[http_send_queue] erro ao ler fila type=%s: %v", sendType, err)
				time.Sleep(5 * time.Second)
				break
			}
			if item == nil {
				break
			}

			if item.Attempts > 0 {
				time.Sleep(time.Duration(item.Attempts) * 2 * time.Second)
			}

			if err := postHTTPSendPayload(item.URL, item.Payload); err != nil {
				errText := err.Error()
				if markErr := q.db.MarkHTTPSendFailure(item.ID, errText); markErr != nil {
					logHTTPQueue(item.AccountID, "[http_send_queue] erro ao marcar falha id=%d: %v", item.ID, markErr)
				}
				logHTTPQueue(item.AccountID, "[http_send_queue] falha type=%s id=%d: %v", sendType, item.ID, err)
				continue
			}

			if err := q.db.MarkHTTPSendSuccess(item.ID); err != nil {
				logHTTPQueue(item.AccountID, "[http_send_queue] erro ao remover id=%d após sucesso: %v", item.ID, err)
			}
		}

		select {
		case <-wake:
		case <-time.After(5 * time.Second):
		}
	}
}

func postHTTPSendPayload(url, payload string) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		if len(body) > 0 {
			return fmt.Errorf("status code: %d body: %s", resp.StatusCode, string(body))
		}
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}
	return nil
}

// SendHTTPQueueItemNow envia imediatamente um item (uso do menu), sem zerar attempts/last_error.
func SendHTTPQueueItemNow(db *Database, id int64) error {
	item, err := db.GetHTTPSendQueueItem(id)
	if err != nil {
		return err
	}
	if item == nil {
		return fmt.Errorf("item id=%d não encontrado", id)
	}

	if err := postHTTPSendPayload(item.URL, item.Payload); err != nil {
		logHTTPQueue(item.AccountID, "[http_send_queue] reenvio manual falhou id=%d: %v", item.ID, err)
		if markErr := db.MarkHTTPSendFailure(item.ID, err.Error()); markErr != nil {
			return fmt.Errorf("falha no envio: %v; erro ao registrar falha: %w", err, markErr)
		}
		return err
	}
	logHTTPQueue(item.AccountID, "[http_send_queue] reenvio manual ok id=%d", item.ID)
	return db.MarkHTTPSendSuccess(item.ID)
}

func enqueueHTTPSend(accountID int64, sendType, url, payload string) error {
	if globalHTTPQueue == nil {
		return fmt.Errorf("fila HTTP não inicializada")
	}
	return globalHTTPQueue.Enqueue(accountID, sendType, url, payload)
}
