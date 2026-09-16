package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	maxLogLines = 5000
)

// getBrasiliaTime retorna o horário atual no fuso horário de Brasília (UTC-3)
// Funciona tanto no Windows quanto no Linux usando offset fixo quando necessário
func getBrasiliaTime() time.Time {
	// Tentar carregar timezone IANA (funciona no Linux)
	if loc, err := time.LoadLocation("America/Sao_Paulo"); err == nil {
		return time.Now().In(loc)
	}
	// Fallback para Windows: usar offset fixo UTC-3 (horário de Brasília)
	// Brasil não tem mais horário de verão desde 2019, então UTC-3 é fixo
	brasiliaOffset := -3 * 60 * 60 // UTC-3 em segundos
	brasiliaTZ := time.FixedZone("BRT", brasiliaOffset)
	return time.Now().In(brasiliaTZ)
}

type Logger struct {
	accountID int64
	accountName string
	file       *os.File
	writer    *bufio.Writer
	mu        sync.Mutex
	lineCount int
}

var loggers = make(map[int64]*Logger)
var loggersMu sync.RWMutex

func getLogger(accountID int64, accountName string) (*Logger, error) {
	loggersMu.RLock()
	if logger, exists := loggers[accountID]; exists {
		loggersMu.RUnlock()
		return logger, nil
	}
	loggersMu.RUnlock()

	loggersMu.Lock()
	defer loggersMu.Unlock()

	// Verificar novamente após adquirir lock exclusivo
	if logger, exists := loggers[accountID]; exists {
		return logger, nil
	}

	// Obter diretório de logs (usar mesmo padrão do database para compatibilidade com Docker)
	logsDir := getLogsDir()
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		// Tentar criar diretório alternativo se falhar
		altLogsDir := "./logs"
		if err2 := os.MkdirAll(altLogsDir, 0755); err2 != nil {
			return nil, fmt.Errorf("erro ao criar diretório de logs (%v) e diretório alternativo (%v): %w", err, err2, err)
		}
		logsDir = altLogsDir
	}

	// Nome do arquivo de log: account_{id}.log
	logFileName := filepath.Join(logsDir, fmt.Sprintf("account_%d.log", accountID))
	
	// Abrir arquivo em modo append
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		// No Windows, pode haver problemas com permissões, tentar criar em local alternativo
		if runtime.GOOS == "windows" {
			altLogFileName := filepath.Join(".", fmt.Sprintf("account_%d.log", accountID))
			if altFile, altErr := os.OpenFile(altLogFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644); altErr == nil {
				file = altFile
				err = nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("erro ao abrir arquivo de log '%s': %w", logFileName, err)
		}
	}

	logger := &Logger{
		accountID:   accountID,
		accountName: accountName,
		file:       file,
		writer:     bufio.NewWriter(file),
		lineCount:  0,
	}

	// Contar linhas existentes no arquivo
	if err := logger.countExistingLines(); err != nil {
		file.Close()
		return nil, fmt.Errorf("erro ao contar linhas do log: %w", err)
	}

	loggers[accountID] = logger
	return logger, nil
}

// countExistingLines conta as linhas existentes no arquivo e atualiza lineCount
func (l *Logger) countExistingLines() error {
	// Reposicionar para o início do arquivo para contar
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(l.file)
	count := 0
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	l.lineCount = count

	// Reposicionar para o final do arquivo para continuar escrevendo
	if _, err := l.file.Seek(0, 2); err != nil {
		return err
	}

	return nil
}

// rotateLog rotaciona o arquivo de log quando atinge o limite
// Renomeia o arquivo atual para _archive.log e cria um novo arquivo zerado
func (l *Logger) rotateLog() error {
	logsDir := getLogsDir()
	currentLogFile := filepath.Join(logsDir, fmt.Sprintf("account_%d.log", l.accountID))
	archiveLogFile := filepath.Join(logsDir, fmt.Sprintf("account_%d_archive.log", l.accountID))

	// Fechar arquivo e writer atuais
	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			return err
		}
	}
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return err
		}
	}

	// Remover archive existente se houver
	if _, err := os.Stat(archiveLogFile); err == nil {
		if err := os.Remove(archiveLogFile); err != nil {
			return fmt.Errorf("erro ao remover archive existente: %w", err)
		}
	}

	// Renomear arquivo atual para archive
	if _, err := os.Stat(currentLogFile); err == nil {
		if err := os.Rename(currentLogFile, archiveLogFile); err != nil {
			return fmt.Errorf("erro ao renomear arquivo para archive: %w", err)
		}
	}

	// Criar novo arquivo zerado
	file, err := os.OpenFile(currentLogFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("erro ao criar novo arquivo de log: %w", err)
	}

	// Atualizar referências do logger
	l.file = file
	l.writer = bufio.NewWriter(file)
	l.lineCount = 0

	return nil
}

func (l *Logger) Log(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Obter timestamp no horário de Brasília (funciona no Windows e Linux)
	now := getBrasiliaTime()
	timestamp := now.Format("2006-01-02 15:04:05")

	// Formatar mensagem
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] %s\n", timestamp, message)

	// Escrever no arquivo
	if _, err := l.writer.WriteString(logLine); err != nil {
		// Se houver erro, tentar continuar
		return
	}

	if err := l.writer.Flush(); err != nil {
		return
	}

	l.lineCount++

	// Verificar se precisa rotacionar (quando atingir exatamente 5000 linhas)
	if l.lineCount >= maxLogLines {
		if err := l.rotateLog(); err != nil {
			// Log de erro silencioso - continuar mesmo se falhar
			return
		}
	}
}

// logHTTPQueue grava no sistema de logs padrão da conta (account_{id}.log).
// Se accountID <= 0, usa o arquivo account_0.log (fila/sistema).
func logHTTPQueue(accountID int64, format string, args ...interface{}) {
	if accountID < 0 {
		accountID = 0
	}
	logger, err := getLogger(accountID, "http_queue")
	if err != nil || logger == nil {
		return
	}
	logger.Log(format, args...)
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.writer != nil {
		if err := l.writer.Flush(); err != nil {
			return err
		}
	}

	if l.file != nil {
		return l.file.Close()
	}

	return nil
}

func closeLogger(accountID int64) {
	loggersMu.Lock()
	defer loggersMu.Unlock()

	if logger, exists := loggers[accountID]; exists {
		logger.Close()
		delete(loggers, accountID)
	}
}

func getLogsDir() string {
	// Verificar se existe variável de ambiente
	if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
		return filepath.Join(dataDir, "logs")
	}
	
	// Verificar se existe diretório ./data/logs
	if _, err := os.Stat("./data/logs"); err == nil {
		return "./data/logs"
	}
	
	// Verificar se existe diretório ./data
	if _, err := os.Stat("./data"); err == nil {
		logsPath := "./data/logs"
		os.MkdirAll(logsPath, 0755)
		return logsPath
	}
	
	// Criar diretório data/logs se não existir
	logsPath := "./data/logs"
	if err := os.MkdirAll(logsPath, 0755); err == nil {
		return logsPath
	}
	
	// Fallback para ./logs
	return "./logs"
}

func getLogFilePath(accountID int64) string {
	return filepath.Join(getLogsDir(), fmt.Sprintf("account_%d.log", accountID))
}

func readLogFile(accountID int64, lines int) ([]string, error) {
	logFilePath := getLogFilePath(accountID)
	
	file, err := os.Open(logFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var allLines []string
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Retornar as últimas N linhas
	if len(allLines) > lines {
		return allLines[len(allLines)-lines:], nil
	}

	return allLines, nil
}

func tailLogFile(accountID int64, stopChan chan struct{}, callback func(string)) error {
	logFilePath := getLogFilePath(accountID)
	
	// Ler linhas existentes primeiro (últimas 50)
	allLines, err := readLogFile(accountID, 50)
	if err == nil {
		for _, line := range allLines {
			callback(line)
		}
	}

	// Agora monitorar novas linhas
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastPos int64 = 0
	if fileInfo, err := os.Stat(logFilePath); err == nil {
		lastPos = fileInfo.Size()
	}

	for {
		select {
		case <-stopChan:
			return nil
		case <-ticker.C:
			// Verificar se arquivo cresceu
			fileInfo, err := os.Stat(logFilePath)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return err
			}

			if fileInfo.Size() > lastPos {
				// Abrir arquivo e ler novas linhas
				file, err := os.Open(logFilePath)
				if err != nil {
					continue
				}

				// Ir para a posição onde paramos
				if _, err := file.Seek(lastPos, 0); err != nil {
					file.Close()
					continue
				}

				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.TrimSpace(line) != "" {
						callback(line)
					}
				}
				
				// Atualizar última posição
				lastPos, _ = file.Seek(0, 1)
				file.Close()
			}
		}
	}
}

