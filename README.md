# Conversor de Extrato Mercado Bitcoin para Koinly

Este projeto contém um **script em Python** que converte o extrato CSV da **Mercado Bitcoin** para um **CSV** no padrão **Koinly**, facilitando a importação e análise de transações (compras, vendas, depósitos e saques).

---

## :rocket: **Como Usar**

### **1. Instale as dependências**

- Certifique-se de ter o **Python 3** instalado em seu sistema.

- O script utiliza a biblioteca **pandas** para manipular o CSV, então é preciso instalá-la:

```bash
pip install pandas
```

ou

```bash
pip3 install pandas
```

---

### **2. Estrutura de Arquivos**

```
📁 Projeto/
 ├── 📄 mb_to_koinly.py            (Script principal de conversão)
 ├── 📄 extrato_mercadobitcoin.csv (Arquivo CSV original do Mercado Bitcoin)
 └── 📄 koinly_output.csv          (Arquivo CSV gerado para o Koinly)
```

---

### **3. Código do Script**

Salve o código abaixo em um arquivo chamado `mb_to_koinly.py` (ou outro nome que preferir):

```python
import pandas as pd
import csv
from datetime import datetime

# Possíveis formatos de data a serem testados
POSSIBLE_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",  # Ex.: 2024-01-17 09:47:30.536247
    "%Y-%m-%d %H:%M:%S",     # Ex.: 2024-01-17 09:47:30
    "%d/%m/%Y %H:%M:%S.%f",  # Ex.: 29/10/2024 08:38:24.123456
    "%d/%m/%Y %H:%M:%S",     # Ex.: 29/10/2024 08:38:24
]

# Categorias que já estão mapeadas
CATEGORIAS_MAPEADAS = {"Execução de ordem", "Depósito", "Saque/Retirada", "Taxa de Saque/Retirada"}

def detect_datetime_format(date_str):
    """Testa cada formato em POSSIBLE_DATE_FORMATS e retorna o primeiro que encaixar."""
    for fmt in POSSIBLE_DATE_FORMATS:
        try:
            datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            continue
    return None

def auto_detect_format_for_series(series, sample_size=5):
    """Tenta detectar o formato de data para as primeiras 'sample_size' linhas não-nulas."""
    sample = series.dropna().head(sample_size)
    if len(sample) == 0:
        return None
    scores = {}
    for val in sample:
        fmt = detect_datetime_format(str(val))
        if fmt:
            scores[fmt] = scores.get(fmt, 0) + 1
    if not scores:
        return None
    best_fmt = max(scores, key=scores.get)
    return best_fmt

def convert_mb_csv_to_sent_received(input_csv, output_csv):
    """
    Converte o extrato da Mercado Bitcoin para o layout Koinly com as colunas:
    
      Date,Sent Amount,Sent Currency,Received Amount,Received Currency,
      Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,Label,Description,TxHash

    Mapeamento:
      - "Depósito": transação com Received Amount preenchido.
      - "Saque/Retirada": transação com Sent Amount preenchido; se houver
          linha "Taxa de Saque/Retirada" com timestamp e moeda próximos, associa como Fee.
      - "Execução de ordem": agrupa duas linhas (uma com quantidade < 0 e outra > 0,
          com diferença de tempo < 2 segundos) para formar uma transação (Sent vs. Received).
      - Outras categorias são ignoradas, mas suas linhas são listadas no terminal.
    """
    # Ler as primeiras linhas para identificar o cabeçalho
    df_head = pd.read_csv(input_csv, nrows=10, encoding='utf-8')
    original_cols = df_head.columns.tolist()

    # Se o cabeçalho vier em minúsculas, renomear para a forma padrão
    rename_map = {}
    if "data" in original_cols:
        rename_map["data"] = "Data"
    if "categoria" in original_cols:
        rename_map["categoria"] = "Categoria"
    if "moeda" in original_cols:
        rename_map["moeda"] = "Moeda"
    if "quantidade" in original_cols:
        rename_map["quantidade"] = "Quantidade"
    if "saldo" in original_cols:
        rename_map["saldo"] = "Saldo"

    # Ler o CSV completo
    df = pd.read_csv(input_csv, encoding='utf-8')
    df.rename(columns=rename_map, inplace=True)

    # Verificar colunas obrigatórias
    required_cols = ["Data", "Categoria", "Moeda", "Quantidade", "Saldo"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Coluna '{col}' não encontrada. Colunas lidas: {df.columns.tolist()}")

    # Auto-detecção do formato de data
    dt_fmt = auto_detect_format_for_series(df["Data"], sample_size=5)
    if not dt_fmt:
        dt_fmt = "%Y-%m-%d %H:%M:%S"  # fallback
        print(f"[Aviso] Formato de data não detectado; usando fallback: {dt_fmt}")

    # Converter a coluna "Data" para datetime
    df["Data"] = pd.to_datetime(df["Data"], format=dt_fmt, errors="coerce")
    invalid_dates = df["Data"].isna().sum()
    if invalid_dates > 0:
        print(f"[AVISO] {invalid_dates} linha(s) com data inválida foram removidas.")
        df = df.dropna(subset=["Data"])

    df.sort_values("Data", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Listar linhas com categorias não mapeadas para revisão
    df_unknown = df[~df["Categoria"].isin(CATEGORIAS_MAPEADAS)]
    if not df_unknown.empty:
        print(f"[INFO] Linhas com categorias não mapeadas ({len(df_unknown)}):")
        print(df_unknown[["Data", "Categoria", "Moeda", "Quantidade", "Saldo"]].to_string(index=False))

    # Criar um dicionário para associar taxas de saque: chave = (Data, Moeda), valor = taxa acumulada
    fee_map = {}
    df_fee = df[df["Categoria"] == "Taxa de Saque/Retirada"].copy()
    for idx, row in df_fee.iterrows():
        tstamp = row["Data"]
        coin = row["Moeda"]
        fee_val = float(row["Quantidade"])
        fee_map.setdefault((tstamp, coin), 0.0)
        fee_map[(tstamp, coin)] += fee_val

    koinly_rows = []

    # Processar Depósitos: mapeia para Received
    df_dep = df[df["Categoria"] == "Depósito"].copy()
    for idx, row in df_dep.iterrows():
        ts = row["Data"]
        coin = row["Moeda"]
        qty = abs(float(row["Quantidade"]))
        date_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        rec = {
            "Date": date_str,
            "Sent Amount": "",
            "Sent Currency": "",
            "Received Amount": qty,
            "Received Currency": coin,
            "Fee Amount": "",
            "Fee Currency": "",
            "Net Worth Amount": "",
            "Net Worth Currency": "",
            "Label": "",
            "Description": "Depósito - Mercado Bitcoin",
            "TxHash": ""
        }
        koinly_rows.append(rec)

    # Processar Saques/Retiradas: mapeia para Sent; associa taxa se encontrada
    df_wd = df[df["Categoria"] == "Saque/Retirada"].copy()
    for idx, row in df_wd.iterrows():
        ts = row["Data"]
        coin = row["Moeda"]
        qty = abs(float(row["Quantidade"]))
        date_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")
        fee_val = ""
        fee_currency = ""
        # Procurar uma taxa associada: chave exata ou com diferença inferior a 2 segundos
        for (t_fee, coin_fee), fee in fee_map.items():
            if coin_fee == coin and abs((t_fee - ts).total_seconds()) < 2.0:
                fee_val = fee
                fee_currency = coin
                break
        wd_line = {
            "Date": date_str,
            "Sent Amount": qty,
            "Sent Currency": coin,
            "Received Amount": "",
            "Received Currency": "",
            "Fee Amount": fee_val,
            "Fee Currency": fee_currency,
            "Net Worth Amount": "",
            "Net Worth Currency": "",
            "Label": "",
            "Description": "Saque/Retirada - Mercado Bitcoin",
            "TxHash": ""
        }
        koinly_rows.append(wd_line)

    # Processar Execução de ordem: agrupar pares (uma linha com quantidade < 0 e outra com > 0)
    df_exec = df[df["Categoria"] == "Execução de ordem"].copy()
    df_exec.reset_index(drop=True, inplace=True)
    used_idx = set()
    n = len(df_exec)
    i = 0
    while i < n:
        if i in used_idx:
            i += 1
            continue
        row1 = df_exec.iloc[i]
        dt1 = row1["Data"]
        coin1 = row1["Moeda"]
        amt1 = float(row1["Quantidade"])
        paired = False
        for j in range(i + 1, n):
            if j in used_idx:
                continue
            row2 = df_exec.iloc[j]
            dt2 = row2["Data"]
            coin2 = row2["Moeda"]
            amt2 = float(row2["Quantidade"])
            time_diff = abs((dt2 - dt1).total_seconds())
            if time_diff < 2.0 and (amt1 * amt2 < 0):
                date_str = min(dt1, dt2).strftime("%Y-%m-%d %H:%M:%S UTC")
                if amt1 < 0:
                    sent_coin, sent_amt = coin1, abs(amt1)
                    rec_coin, rec_amt   = coin2, abs(amt2)
                else:
                    sent_coin, sent_amt = coin2, abs(amt2)
                    rec_coin, rec_amt   = coin1, abs(amt1)
                trade_line = {
                    "Date": date_str,
                    "Sent Amount": sent_amt,
                    "Sent Currency": sent_coin,
                    "Received Amount": rec_amt,
                    "Received Currency": rec_coin,
                    "Fee Amount": "",
                    "Fee Currency": "",
                    "Net Worth Amount": "",
                    "Net Worth Currency": "",
                    "Label": "",
                    "Description": "Execução de ordem - Mercado Bitcoin",
                    "TxHash": ""
                }
                koinly_rows.append(trade_line)
                used_idx.add(i)
                used_idx.add(j)
                paired = True
                break
        i += 1

    # Montar DataFrame final com o layout exigido pelo Koinly
    df_koinly = pd.DataFrame(koinly_rows, columns=[
        "Date",
        "Sent Amount",
        "Sent Currency",
        "Received Amount",
        "Received Currency",
        "Fee Amount",
        "Fee Currency",
        "Net Worth Amount",
        "Net Worth Currency",
        "Label",
        "Description",
        "TxHash"
    ])

    df_koinly.to_csv(output_csv, index=False, quoting=csv.QUOTE_ALL)
    print(f"Conversão finalizada! Foram geradas {len(df_koinly)} transações.")
    print(f"Arquivo salvo: {output_csv}")

if __name__ == "__main__":
    input_file = "extrato_mercadobitcoin.csv"   # Substitua pelo nome do seu extrato
    output_file = "koinly_output.csv"   # Nome do arquivo de saída
    convert_mb_csv_to_sent_received(input_file, output_file)
    print("Concluído.")

```

---

### **4. Como Executar**

No terminal ou prompt de comando, vá até a pasta onde o script está salvo e rode:

```bash
python mb_to_koinly.py
```

ou

```bash
python3 mb_to_koinly.py
```

Isso vai ler o arquivo chamado **`extrato_mercadobitcoin.csv`** (que deve estar na mesma pasta) e gerar um arquivo **`koinly_output.csv`** no mesmo diretório.

---

### **5. Resultado**

- O arquivo final (`koinly_output.csv`) terá **as 12 colunas** do “**Sent/Received CSV**” aceito pelo Koinly:

  1. `Date`
  2. `Sent Amount`
  3. `Sent Currency`
  4. `Received Amount`
  5. `Received Currency`
  6. `Fee Amount`
  7. `Fee Currency`
  8. `Net Worth Amount`
  9. `Net Worth Currency`
  10. `Label`
  11. `Description`
  12. `TxHash`

- Cada transação é mapeada de acordo com a lógica:

  - **Depósito** → recebe cripto/BRL (`Received Amount`).  
  - **Saque/Retirada** → envia cripto/BRL (`Sent Amount`).  
  - **Execução de ordem** → agrupa duas linhas (uma com quantidade < 0, outra > 0) e gera “Sent” vs. “Received”.

- **Cancelamento de ordem**, **Criação de ordem** e outros eventos não entram no CSV final.

---

## :wrench: **Personalização / Observações**

- Caso haja **Taxa de Saque/Retirada** no extrato, você pode inserir uma lógica para mapear essa taxa em `Fee Amount` e `Fee Currency`.  
- Se o arquivo tiver outro delimitador (por exemplo, `;`), troque `delimiter=','` para `delimiter=';'`.  
- Se as datas vierem em outro formato (por ex., sem microssegundos), revise a lista `POSSIBLE_DATE_FORMATS`.  
- Se desejar converter a data para outro fuso-horário ou exibir “(UTC)” no final, basta trocar o `strftime`.

---

### ⚖️ **Licença e Créditos**

Este script é fornecido “como está”. Sinta-se livre para adaptá-lo.  
Em caso de dúvidas ou ajustes futuros, basta fazer [Issues](https://github.com/sua-conta/sua-repo/issues) no repositório ou entrar em contato.  

**Bom uso e boa análise no Koinly!**
