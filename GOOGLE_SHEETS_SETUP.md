# Configuração do Webhook Google Planilhas

Este guia explica como configurar um script no Google Apps Script para receber dados do Notificador de Operações Bybit e salvá-los automaticamente em uma planilha do Google.

## Pré-requisitos

- Conta Google (Gmail)
- Acesso ao Google Sheets
- Acesso ao Google Apps Script

## Passo a Passo

### 1. Acessar o Google Apps Script

1. Acesse: https://script.google.com/home
2. Faça login com sua conta Google, se necessário

### 2. Criar um Novo Projeto

1. Clique em **"Novo projeto"** (ou **"New project"**)
2. Um novo projeto será criado com o nome padrão "Meu projeto" (ou "Untitled project")

### 3. Configurar o Código

1. Renomeie o projeto para um nome descritivo (ex: "Bybit Notifier Webhook")
2. Cole o código abaixo no editor:

```javascript
function doPost(e) {
  var response = {};
  
  try {    
    var fullData = JSON.parse(e.postData.contents);
    var ss = SpreadsheetApp.openById(fullData.sheet_id);
    var sheetName = fullData.symbol;
    
    // 1. Tenta buscar a aba pelo nome do symbol
    var sheet = ss.getSheetByName(sheetName);

    // 2. Se não existir, verifica se pode reaproveitar a aba padrão
    if (!sheet) {
      var allSheets = ss.getSheets();
      
      // Critério: Se só existe 1 aba, ela não tem o nome do symbol E está vazia
      if (allSheets.length === 1 && allSheets[0].getLastRow() === 0) {
        sheet = allSheets[0];
        sheet.setName(sheetName); // Renomeia a aba padrão para o nome do symbol
      } else {
        // Caso contrário, cria uma nova aba normalmente
        sheet = ss.insertSheet(sheetName);
      }
      
      // inserir headers na primeira vez
      if (fullData.headers) {
        sheet.appendRow(fullData.headers);
      }
    }

    // 3. Insere os dados
    sheet.appendRow(fullData.columns);
    
    response = {
      "status": "success",
      "message": "Dados inseridos com sucesso",
      "tab": sheetName,
      "reused_default": ss.getSheets().length === 1 // Indica se reaproveitou a aba inicial
    };

  } catch (err) {
    response = {
      "status": "error",
      "message": err.message
    };
  }

  return ContentService.createTextOutput(JSON.stringify(response))
                       .setMimeType(ContentService.MimeType.JSON);
}
```

3. Clique em **"Salvar"** (ou pressione `Ctrl+S` / `Cmd+S`)

### 4. Implantar o Script como App da Web

1. Clique no botão **"Implantar"** (ou **"Deploy"**) no canto superior direito
2. Selecione **"Nova implantação"** (ou **"New deployment"**)
3. Clique no ícone de engrenagem ⚙️ ao lado de **"Selecionar tipo"** (ou **"Select type"**)
4. Selecione **"App da Web"** (ou **"Web app"**)

### 5. Configurar as Permissões

Configure as seguintes opções:

- **Executar como**: Selecione **"Eu"** (ou **"Me"**)
- **Quem pode acessar**: Selecione **"Qualquer pessoa"** (ou **"Anyone"**)

> **Importante**: A opção "Qualquer pessoa" permite que o aplicativo receba requisições do Notificador Bybit sem autenticação. Isso é necessário para o funcionamento do webhook.

### 6. Autorizar o Script

1. Clique em **"Autorizar acesso"** (ou **"Authorize access"**)
2. Uma nova janela será aberta pedindo permissões
3. Selecione sua conta Google
4. Clique em **"Avançado"** (ou **"Advanced"**)
5. Clique em **"Ir para [nome do projeto] (não seguro)"** (ou **"Go to [project name] (unsafe)"**)
   > Esta mensagem aparece porque o script não foi verificado pelo Google. É seguro continuar se você criou o script.
6. Clique em **"Permitir"** (ou **"Allow"**)

### 7. Obter o Link do Webhook

1. Após autorizar, você será redirecionado de volta para a tela de implantação
2. Clique em **"Implantar"** (ou **"Deploy"**)
3. Uma nova janela será exibida com o link do webhook
4. **Copie o link gerado** - ele será algo como:
   ```
   https://script.google.com/macros/s/AKfycby.../exec
   ```
5. Este é o link que você usará como **Webhook URL Google Planilhas** no Notificador Bybit

### 8. Obter a URL da Planilha do Google

1. Crie uma nova planilha no Google Sheets ou abra uma existente
2. A URL da planilha estará na barra de endereços do navegador
3. **Exemplo de URL de planilha:**
   ```
   https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=0
   ```
4. **Copie a URL completa** - ela será usada como **URL da planilha do Google** no Notificador Bybit

> **Dica**: Crie uma planilha nova e vazia. O script criará automaticamente abas para cada símbolo (par de negociação) conforme os dados forem recebidos.

## Configuração no Notificador Bybit

Ao cadastrar ou editar uma conta no Notificador Bybit, você precisará fornecer:

1. **Webhook URL Google Planilhas**: O link copiado no passo 7 acima
2. **URL da planilha do Google**: A URL copiada no passo 8 acima

> **Nota**: Se você preencher o Webhook URL Google Planilhas, a URL da planilha se torna obrigatória.

## Como Funciona

- O script recebe dados via POST do Notificador Bybit
- Os dados são organizados por símbolo (par de negociação)
- Cada símbolo recebe sua própria aba na planilha
- Se a planilha estiver vazia (apenas a aba padrão), ela será renomeada para o primeiro símbolo recebido
- Novas abas são criadas automaticamente para novos símbolos
- Os dados são inseridos linha por linha conforme as operações são monitoradas

## Solução de Problemas

### Erro: "Script não autorizado"
- Certifique-se de ter autorizado o script corretamente no passo 6
- Tente criar uma nova implantação e autorizar novamente

### Erro: "Acesso negado"
- Verifique se a opção "Quem pode acessar" está configurada como "Qualquer pessoa"
- Crie uma nova implantação se necessário

### Dados não aparecem na planilha
- Verifique se a URL da planilha está correta
- Certifique-se de que você tem permissão de edição na planilha
- Verifique os logs do Notificador Bybit para ver se há erros

### Abas não são criadas
- Verifique se o script foi salvo corretamente
- Certifique-se de que a URL do webhook está correta
- Tente criar uma nova implantação

## Segurança

- O link do webhook é público, mas apenas o Notificador Bybit saberá enviar dados no formato correto
- Mantenha a URL da planilha privada se contiver informações sensíveis
- Você pode revogar o acesso a qualquer momento excluindo a implantação no Google Apps Script

## Referências

- [Documentação do Google Apps Script](https://developers.google.com/apps-script)
- [Documentação do Google Sheets API](https://developers.google.com/sheets/api)
- [Voltar para o README principal](README.md)

