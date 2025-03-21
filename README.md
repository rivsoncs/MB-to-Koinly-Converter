# Conversor de Extratos Mercado Bitcoin para Koinly

Este repositório contém **dois scripts em Python** que convertem extratos CSV da **Mercado Bitcoin** para um formato compatível com o [Koinly](https://koinly.io/). Dessa forma, você pode importar e analisar facilmente suas transações (compras, vendas, depósitos, saques etc.) no Koinly.

---

## :sparkles: Índice

1. [Descrição Geral](#descrição-geral)
2. [Requisitos](#requisitos)
3. [Scripts Disponíveis](#scripts-disponíveis)
   - [1) mb_to_koinly_legado.py (Extrato Antigo)](#1-mb_to_koinly_legadopy-extrato-antigo)
   - [2) mb_to_koinly_novo.py (Extrato Novo)](#2-mb_to_koinly_novopy-extrato-novo)
4. [Como Executar](#como-executar)
5. [Layout Esperado nos Arquivos](#layout-esperado-nos-arquivos)
   - [Extrato Antigo (Legado)](#extrato-antigo-legado)
   - [Extrato Novo](#extrato-novo)
6. [Observações e Dicas](#observações-e-dicas)
7. [Licença](#licença)

---

## Descrição Geral

A **Mercado Bitcoin** fornece extratos em CSV com diferentes formatações, dependendo da época ou do tipo de relatório que você seleciona. Aqui temos:

- **Script Legado**: criado para o modelo de extrato onde há colunas como `Data, Categoria, Moeda, Quantidade, Saldo`, e as operações aparecem como “Execução de ordem”, “Depósito”, “Saque/Retirada” etc.
- **Script Novo**: voltado ao modelo onde cada linha traz `Ativo, Operação Tipo (TRADING-IN, TRADING-OUT, WALLET-IN, WALLET-OUT, CASH-OUT)`, “Preço BRL”, “Liquido BRL”, “Bruto BRL”, etc.

Cada script lê o CSV original e gera um **CSV de saída** no formato **Sent/Received** aceito pelo Koinly, com colunas como:

```
Date,Sent Amount,Sent Currency,Received Amount,Received Currency,
Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,Label,Description,TxHash
```

---

## Requisitos

- **Python 3.7+** (ou superior)
- **pandas**: para manipular o CSV. Instale via:
  ```bash
  pip install pandas
  # ou
  pip3 install pandas
  ```

Caso queira personalizar o script (por exemplo, remover debug prints ou mudar rótulos “Trade”, “Withdrawal” etc.), fique à vontade para editar o código.

---

## Scripts Disponíveis

### 1) `mb_to_koinly_legado.py` (Extrato Antigo)

- **Objetivo**: converter o **extrato antigo** do MB, no qual as transações aparecem em linhas com `Categoria` = “Execução de ordem”, “Depósito”, “Saque/Retirada” e, às vezes, “Taxa de Saque/Retirada”.
- **Cabeçalho esperado** no CSV:
  ```
  Data, Categoria, Moeda, Quantidade, Saldo
  ```
- **Lógica**:
  - “Depósito” → preenche `Received` no Koinly  
  - “Saque/Retirada” → preenche `Sent` no Koinly, associando taxa se houver  
  - “Execução de ordem” → agrupa duas linhas: uma negativa, outra positiva (quantidade < 0 e > 0) para formar uma única transação de compra/venda.

### 2) `mb_to_koinly_novo.py` (Extrato Novo)

- **Objetivo**: converter o **extrato novo** do MB, no qual cada linha traz `TRADING-IN`, `TRADING-OUT`, `WALLET-IN`, `WALLET-OUT`, “CASH-OUT”, etc.
- **Cabeçalho esperado** no CSV:
  ```
  Ativo,Operação Tipo,Operação Data/Hora,Preço BRL,Liquido BRL,Bruto BRL,Liquido Cripto,Bruto Cripto
  ```
- **Lógica**:
  - `TRADING-OUT` → Venda de cripto (Sent cripto, Received BRL)
  - `TRADING-IN` → Compra de cripto (Sent BRL, Received cripto)
  - `WALLET-OUT` → Retirada de cripto (Sent cripto)
  - `WALLET-IN` → Depósito de cripto (Received cripto)
  - `CASH-OUT` (com Ativo = BRL) → Saque de fiat (Sent BRL)
  - Usa colunas “Liquido” e “Bruto” para identificar a taxa (Fee).

Cada script já possui **auto-detecção** do formato de data, conversão de vírgula decimal (`0,69`) para ponto decimal (`0.69`) e similares.

---

## Como Executar

1. **Obtenha o CSV** do Mercado Bitcoin (antigo ou novo).  
2. Salve-o no mesmo diretório do script correspondente.  
3. Rode no terminal:

```bash
python mb_to_koinly_legado.py
```

ou

```bash
python mb_to_koinly_novo.py
```

Por padrão, cada script lê um arquivo chamado, por exemplo, **`extrato_mercadobitcoin.csv`** e gera um **`koinly_output.csv`**. Se quiser, edite as últimas linhas do script para trocar os nomes de arquivo de entrada/saída.

---

## Layout Esperado nos Arquivos

### Extrato Antigo (Legado)

Um **exemplo** de cabeçalho e linhas:

```
Data,Categoria,Moeda,Quantidade,Saldo
2024-01-10 08:30:24,Execução de ordem,BTC,-0.002,0.520
2024-01-10 08:30:24,Execução de ordem,BRL,1000,5000
2024-01-12 10:45:17,Depósito,BRL,2000,7000
2024-01-13 12:00:55,Saque/Retirada,ETH,0.05,0.100
2024-01-13 12:00:55,Taxa de Saque/Retirada,ETH,0.001,0.099
```

### Extrato Novo

Um **exemplo** de cabeçalho e linhas:

```
MERCADO BITCOIN SERVIÇOS DIGITAIS LTDA,,,,CPF/CNPJ: 18.213.434/0001-35,,,
Ativo,Operação Tipo,Operação Data/Hora,Preço BRL,Liquido BRL,Bruto BRL,Liquido Cripto,Bruto Cripto
REN,TRADING-OUT,20/03/2025 22:32:43,"0,067","14,52967275","14,5515","217,1865672","217,1865672"
MENGOFT,TRADING-IN,20/03/2025 21:47:24,"0,69","3832,95","3832,95","5535,5575",5555
XDC,WALLET-OUT,20/03/2025 21:35:49,"0,42","645,253965","645,253965","1536,318964","1536,318964"
BRL,CASH-OUT,20/03/2025 06:01:14,1,3000,3000,3000,3000
...
```

Observando que, às vezes, há linhas extras iniciais (sobre CNPJ, endereço etc.). Os scripts mais novos fazem a **leitura automática** para localizar a linha do cabeçalho verdadeiro.

---

## Observações e Dicas

1. **Importar no Koinly**: depois de gerado o `koinly_output.csv`, vá no Koinly, selecione a exchange ou clique em *Import from file*, e aponte para esse CSV.  
2. **Formatação**: se você for abrir o CSV final no Excel/LibreOffice e ver colunas desorganizadas, certifique-se de usar `,` (vírgula) como delimitador, pois o Koinly segue o padrão CSV “internacional”.  
3. **Taxas**: o script “antigo” tenta agrupar taxa de saque coincidindo com a data/hora (dentro de 2 segundos). O script “novo” deduz a taxa da diferença (Bruto – Líquido).  
4. **Tipos de Moeda**: se o Koinly não reconhecer a ticker “MENGOFT”, por exemplo, você pode precisar renomear ou mapear manualmente, dependendo de como o Koinly rotula o ativo.  
5. **Fallback numérico**: em versões recentes, o script “novo” possui fallback para lidar com milhar (`1.234,56`) ou valores sem milhar (`1234,56`). Se notar `NaN`, verifique se o CSV difere muito do esperado.  

---

## Licença

Este projeto é disponibilizado “**como está**”, sem garantias. Fique à vontade para **modificar**, **abrir Issues** ou enviar PRs com melhorias. 

---


**Esperamos que estes scripts facilitem sua apuração e a importação no Koinly.** Em caso de dúvidas, basta contatar os mantenedores ou abrir uma [Issue](https://github.com/seu-usuario/seu-repositorio/issues) no repositório. Boas conversões!
```
