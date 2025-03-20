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
 ├── 📄 mb_csv_to_koinly.py        (Script principal de conversão)
 ├── 📄 extrato_mercadobitcoin.csv (Arquivo CSV original do Mercado Bitcoin)
 └── 📄 koinly_output.csv          (Arquivo CSV gerado para o Koinly)
```

---

### **3. Código do Script**

Salve o código abaixo em um arquivo chamado `mb_csv_to_koinly.py` (ou outro nome que preferir):

```python
import pandas as pd
from datetime import datetime
import csv

POSSIBLE_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",  # Ex.: 2024-01-17 09:47:30.536247
    "%Y-%m-%d %H:%M:%S",     # Ex.: 2024-01-17 09:47:30
    "%d/%m/%Y %H:%M:%S.%f",  # Ex.: 29/10/2024 08:38:24.123456
    "%d/%m/%Y %H:%M:%S",     # Ex.: 29/10/2024 08:38:24
]

def detect_datetime_format(date_str):
    """Testa cada formato de POSSIBLE_DATE_FORMATS e retorna o primeiro que encaixar."""
    for fmt in POSSIBLE_DATE_FORMATS:
        try:
            datetime.strptime(date_str, fmt)
            return fmt
        except ValueError:
            pass
    return None

def auto_detect_format_for_series(series, sample_size=5):
    """Tenta detectar o formato de data/hora para as primeiras 'sample_size' linhas não-nulas da coluna."""
    sample = series.dropna().head(sample_size)
    if len(sample) == 0:
        return None

    scores = {}
    for val in sample:
        possible = detect_datetime_format(str(val))
        if possible:
            scores[possible] = scores.get(possible, 0) + 1
    
    if not scores:
        return None
    # Retorna o formato com maior pontuação
    best_fmt = max(scores, key=scores.get)
    return best_fmt

def convert_mb_csv_to_sent_received(input_csv, output_csv):
    """
    Script para converter extrato da Mercado Bitcoin no layout:
    
    Date,Sent Amount,Sent Currency,Received Amount,Received Currency,
    Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,
    Label,Description,TxHash

    Lógicas de mapeamento:
      - 'Depósito' => Received (Deposit).
      - 'Saque/Retirada' => Sent (Withdrawal).
      - 'Execução de ordem' => agrupar 2 linhas (uma <0, outra >0) => (Sent, Received).
      - Demais categorias (Cancelamento, Criação etc.) => ignoradas.

    Usa parse de datas com auto-detecção do formato. 
    """

    df_head = pd.read_csv(input_csv, nrows=10, encoding='utf-8')
    original_cols = df_head.columns.tolist()

    # Renomear se vier em minúsculas
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

    # Ler CSV completo
    df = pd.read_csv(input_csv, encoding='utf-8')
    df.rename(columns=rename_map, inplace=True)

    required = ["Data","Categoria","Moeda","Quantidade","Saldo"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Coluna '{col}' não encontrada. Colunas: {df.columns.tolist()}")

    # Auto-detectar formato de data
    dt_fmt = auto_detect_format_for_series(df["Data"], sample_size=5)
    if not dt_fmt:
        dt_fmt = "%Y-%m-%d %H:%M:%S"
        print(f"[Aviso] Não detectei formato. Usando fallback: {dt_fmt}")

    # Converter a data para datetime
    df["Data"] = pd.to_datetime(df["Data"], format=dt_fmt, errors="coerce")
    invalid_count = df["Data"].isna().sum()
    if invalid_count>0:
        print(f"[AVISO] {invalid_count} linha(s) com Data inválida(s). Removidas.")
        df = df.dropna(subset=["Data"])

    df.sort_values("Data", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Montar lista final
    koinly_rows = []

    # Filtrar
    df_exec = df[df["Categoria"] == "Execução de ordem"].copy()
    df_dep = df[df["Categoria"] == "Depósito"].copy()
    df_wd = df[df["Categoria"] == "Saque/Retirada"].copy()

    # 1) Depósitos => Received
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

    # 2) Saque/Retirada => Sent
    for idx, row in df_wd.iterrows():
        ts = row["Data"]
        coin = row["Moeda"]
        qty = abs(float(row["Quantidade"]))
        date_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC")

        rec = {
            "Date": date_str,
            "Sent Amount": qty,
            "Sent Currency": coin,
            "Received Amount": "",
            "Received Currency": "",
            "Fee Amount": "",
            "Fee Currency": "",
            "Net Worth Amount": "",
            "Net Worth Currency": "",
            "Label": "",
            "Description": "Saque/Retirada - Mercado Bitcoin",
            "TxHash": ""
        }
        koinly_rows.append(rec)

    # 3) Execução de ordem => pares (quantidade<0, quantidade>0)
    df_exec.reset_index(drop=True, inplace=True)
    used_idx = set()
    n = len(df_exec)
    i = 0

    while i<n:
        if i in used_idx:
            i+=1
            continue
        row1 = df_exec.iloc[i]
        dt1 = row1["Data"]
        coin1 = row1["Moeda"]
        amt1 = float(row1["Quantidade"])

        found_pair = False
        for j in range(i+1, n):
            if j in used_idx:
                continue
            row2 = df_exec.iloc[j]
            dt2 = row2["Data"]
            coin2 = row2["Moeda"]
            amt2 = float(row2["Quantidade"])
            time_diff = abs((dt2 - dt1).total_seconds())

            if time_diff<2.0 and (amt1*amt2<0):
                # Achamos par
                date_str = min(dt1, dt2).strftime("%Y-%m-%d %H:%M:%S UTC")
                
                if amt1<0:
                    sent_coin, sent_amt = coin1, abs(amt1)
                    rec_coin, rec_amt = coin2, abs(amt2)
                else:
                    sent_coin, sent_amt = coin2, abs(amt2)
                    rec_coin, rec_amt = coin1, abs(amt1)

                koinly_trade = {
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
                koinly_rows.append(koinly_trade)

                used_idx.add(i)
                used_idx.add(j)
                found_pair = True
                break
        i+=1

    # Montar DataFrame final no layout “Sent/Received CSV”
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
    input_file = "extrato_mercadobitcoin.csv"
    output_file = "koinly_output.csv"
    convert_mb_csv_to_sent_received(input_file, output_file)
```

---

### **4. Como Executar**

No terminal ou prompt de comando, vá até a pasta onde o script está salvo e rode:

```bash
python mb_csv_to_koinly.py
```

ou

```bash
python3 mb_csv_to_koinly.py
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
