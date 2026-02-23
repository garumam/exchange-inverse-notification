# Notificador de Operações Bybit

Aplicação em Go para monitorar ordens da Bybit via WebSocket e notificar através de webhooks do Discord ou no terminal.

## Funcionalidades

- ✅ Cadastro de múltiplas contas Bybit
- ✅ Gerenciamento de contas (listar, remover)
- ✅ Conexão WebSocket para stream `order.inverse`
- ✅ Notificações via Discord webhook ou terminal
- ✅ Reconexão automática em caso de queda
- ✅ Persistência de conexões ativas no SQLite
- ✅ Restauração automática de conexões ao reiniciar
- ✅ Interface CLI simples e intuitiva

## Requisitos

- Go 1.21 ou superior
- SQLite3 (incluído via driver CGO)

## Instalação

### Desenvolvimento

1. Clone o repositório:
```bash
git clone <repo-url>
cd notificar_operacoes_bybit
```

2. Instale as dependências:
```bash
go mod download
go mod tidy
```

3. Execute a aplicação:
```bash
go run .
```

### Docker (Desenvolvimento)

```bash
# Build da imagem
docker-compose build

# Executar de forma interativa (permite digitar no terminal)
docker-compose run --rm bybit-notifier

# OU executar em background (sem interação)
docker-compose up -d
```

### Build para Produção

#### Usando Docker (Recomendado - Não precisa ter Go instalado)

Esta é a forma mais fácil de gerar builds sem precisar instalar o Go. Compile cada plataforma separadamente:

##### Build para Windows

**Windows PowerShell:**
```powershell
.\build-docker-windows.ps1
```

**Linux:**
```bash
chmod +x build-docker-windows.sh
./build-docker-windows.sh
```

**Ou usando docker-compose diretamente:**
```bash
docker-compose -f docker-compose.build.windows.yml build
docker-compose -f docker-compose.build.windows.yml run --rm builder
```

##### Build para Linux

**Windows PowerShell:**
```powershell
.\build-docker-linux.ps1
```

**Linux:**
```bash
chmod +x build-docker-linux.sh
./build-docker-linux.sh
```

**Ou usando docker-compose diretamente:**
```bash
docker-compose -f docker-compose.build.linux.yml build
docker-compose -f docker-compose.build.linux.yml run --rm builder
```

#### Build Local (requer Go instalado)

**Windows:**
```powershell
.\build.ps1
```

**Linux:**
```bash
chmod +x build.sh
./build.sh
```

Os executáveis serão gerados na pasta `bin/`:
- `bybit-notifier-windows.exe` - Windows
- `bybit-notifier-linux` - Linux

## Executando o Aplicativo Buildado

### Windows

1. Navegue até a pasta `bin/`:
```powershell
cd bin
```

2. Execute o arquivo:
```powershell
.\bybit-notifier-windows.exe
```

**Ou execute diretamente do diretório raiz:**
```powershell
.\bin\bybit-notifier-windows.exe
```

### Linux

1. Navegue até a pasta `bin/`:
```bash
cd bin
```

2. Torne o arquivo executável (se necessário):
```bash
chmod +x bybit-notifier-linux
```

3. Execute o arquivo:
```bash
./bybit-notifier-linux
```

**Ou execute diretamente do diretório raiz:**
```bash
chmod +x bin/bybit-notifier-linux
./bin/bybit-notifier-linux
```

**Nota:** O aplicativo criará automaticamente a pasta `data/` no diretório atual para armazenar o banco de dados SQLite e os logs.

## Executando em Produção (Linux)

Para executar o aplicativo como um serviço systemd em produção no Linux, siga os passos abaixo:

### Pré-requisitos

Instale o `screen` (necessário para executar o serviço em background):
```bash
sudo apt install screen
```

Para ver processos rodando no screen:
```bash
screen -ls
```

### Configuração do Serviço systemd

1. **Criar o arquivo de serviço:**
```bash
sudo vi /etc/systemd/system/bybit-notifier.service
```

2. **Adicione o seguinte conteúdo no arquivo** (substitua `USERNAME` pelo seu usuário Linux e ajuste os caminhos conforme necessário):

```ini
[Unit]
Description=Bybit Notifier Service
After=network.target

[Service]
Type=forking
User=USERNAME
WorkingDirectory=/home/USERNAME/bybit_notifier
ExecStart=/usr/bin/screen -dmS bybit /home/USERNAME/bybit_notifier/bybit-notifier-linux
ExecStop=/usr/bin/screen -S bybit -X quit
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Importante:** Ajuste os seguintes valores no arquivo acima:
- `User=USERNAME` - Substitua pelo seu usuário Linux
- `WorkingDirectory=/home/USERNAME/bybit_notifier` - Substitua `USERNAME` pelo seu usuário e ajuste o caminho onde o executável está localizado
- `ExecStart=/home/USERNAME/bybit_notifier/bybit-notifier-linux` - Ajuste o caminho completo para o executável `bybit-notifier-linux`

3. **Reinicie o daemon do systemd:**
```bash
sudo systemctl daemon-reload
```

4. **Habilitar para iniciar automaticamente no boot:**
```bash
sudo systemctl enable bybit-notifier
```

5. **Iniciar o serviço agora:**
```bash
sudo systemctl start bybit-notifier
```

6. **Verificar o status do serviço:**
```bash
sudo systemctl status bybit-notifier
```

### Gerenciamento do Serviço

**Para abrir o app e interagir com ele:**
```bash
screen -r bybit

ou -rd caso retorne a mensagem que já está attached

screen -rd bybit
```

**Para manter em background (desanexar do screen):**
- Pressione `Ctrl+A` e depois `D`

**Outros comandos úteis:**
```bash
# Parar o serviço
sudo systemctl stop bybit-notifier

# Reiniciar o serviço
sudo systemctl restart bybit-notifier

# Ver logs do serviço
sudo journalctl -u bybit-notifier -f

# Desabilitar inicialização automática
sudo systemctl disable bybit-notifier
```

## Uso

1. Execute o aplicativo
2. Use o menu para:
   - **Cadastrar conta**: Adicione nome, API Key, API Secret e webhook Discord (opcional)
   - **Listar contas**: Veja todas as contas cadastradas (API Secret não é exibido)
   - **Remover conta**: Remova uma conta específica
   - **Iniciar WebSocket**: Inicie monitoramento para uma conta específica ou todas
   - **Parar WebSocket**: Pare o monitoramento de uma conta ou todas

## Integração com Google Planilhas

O aplicativo suporta integração com Google Planilhas para salvar automaticamente os dados das operações monitoradas. Para configurar:

1. Configure um script no Google Apps Script seguindo o guia: [**Configuração do Webhook Google Planilhas**](GOOGLE_SHEETS_SETUP.md)
2. Ao cadastrar ou editar uma conta, forneça:
   - **Webhook URL Google Planilhas**: Link gerado pelo Google Apps Script
   - **URL da planilha do Google**: URL da planilha onde os dados serão salvos

> **Nota**: Se você preencher o Webhook URL Google Planilhas, a URL da planilha se torna obrigatória.

## Notificações

O aplicativo monitora apenas ordens do tipo `inverse` e notifica quando:
- Uma nova ordem é aberta (`New` ou `PartiallyFilled`)
- Uma ordem é cancelada (`Cancelled`)

Se um webhook Discord foi configurado, a notificação será enviada para o Discord. Caso contrário, será exibida no terminal com a mensagem "Carteira 24H atualizada".

## Estrutura do Banco de Dados

O SQLite armazena:
- **bybit_accounts**: Contas cadastradas
- **active_connections**: Conexões ativas (restauradas automaticamente ao reiniciar)

## Segurança

- API Secret nunca é exibido na listagem
- API Key é mascarado (mostra apenas primeiros e últimos 4 caracteres)
- Dados sensíveis armazenados localmente no SQLite

## Desenvolvimento

### Estrutura do Projeto

```
.
├── main.go                           # Ponto de entrada e CLI
├── database.go                       # Gerenciamento do SQLite
├── account.go                        # Gerenciamento de contas
├── websocket.go                      # Cliente WebSocket Bybit
├── logger.go                         # Sistema de logs
├── Dockerfile                        # Build para Docker (execução)
├── Dockerfile.build.windows          # Build para Docker (apenas Windows)
├── Dockerfile.build.linux            # Build para Docker (apenas Linux)
├── docker-compose.yml                # Configuração Docker
├── docker-compose.build.windows.yml  # Configuração Docker para build Windows
├── docker-compose.build.linux.yml    # Configuração Docker para build Linux
├── build.sh                          # Script de build local (Linux)
├── build.ps1                         # Script de build local (Windows)
├── build-docker-windows.sh           # Script de build Windows via Docker (Linux)
├── build-docker-windows.ps1          # Script de build Windows via Docker (Windows)
├── build-docker-linux.sh             # Script de build Linux via Docker (Linux)
├── build-docker-linux.ps1            # Script de build Linux via Docker (Windows)
├── README.md                          # Este arquivo
└── GOOGLE_SHEETS_SETUP.md            # Guia de configuração do Google Planilhas
```

## Licença

Este projeto é fornecido "como está" para uso pessoal.

