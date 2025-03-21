import pandas as pd
import csv
import re
from datetime import datetime

##############################################################################
# Possíveis formatos de data/hora
##############################################################################
POSSIBLE_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S.%f",  # 2024-01-17 09:47:30.536247
    "%Y-%m-%d %H:%M:%S",     # 2024-01-17 09:47:30
    "%d/%m/%Y %H:%M:%S.%f",  # 29/10/2024 08:38:24.123456
    "%d/%m/%Y %H:%M:%S",     # 29/10/2024 08:38:24
]

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
    """
    Tenta detectar o formato de data para as primeiras 'sample_size' linhas não-nulas.
    Retorna o formato mais provável ou None se não for possível detectar.
    """
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
    # Escolhe o formato com mais "votos" na amostra
    best_fmt = max(scores, key=scores.get)
    return best_fmt

##############################################################################
# Fallback para converter valores numéricos com ou sem pontuação de milhar
##############################################################################
def parse_number_with_fallback(value: str) -> float:
    """
    Tenta converter 'value' em float de duas maneiras:
      1) Remove todos os pontos (.) como se fossem milhar e depois troca vírgula (,) por ponto (.)
      2) Apenas troca vírgula (,) por ponto (.)
    Se ambas falharem, retorna None (NaN).
    """
    val = value.strip().replace('"', '')
    if not val:
        return None
    
    # 1) Tentar remover pontos como milhar e trocar vírgula decimal
    try_1 = re.sub(r'\.', '', val)   # remove todos os '.' (supostos milhar)
    try_1 = try_1.replace(',', '.') # troca vírgula decimal por ponto
    try:
        return float(try_1)
    except ValueError:
        pass
    
    # 2) Tentar apenas trocar a vírgula decimal por ponto
    try_2 = val.replace(',', '.')
    try:
        return float(try_2)
    except ValueError:
        pass
    
    # Falhou tudo → retorna None
    return None

##############################################################################
# Funções para localizar o cabeçalho e detectar o separador
##############################################################################
def try_split(line):
    """
    Tenta dividir a linha por ',' e ';' e retorna (split_comma, split_semicolon).
    """
    return [col.strip() for col in line.split(',')], [col.strip() for col in line.split(';')]

def find_header_line_and_sep(csv_file_path, encoding='utf-8-sig'):
    """
    Lê o arquivo como texto e localiza:
      - O índice (0-based) da linha que contenha 'Ativo' e 'Operação Tipo'.
      - Qual separador (';' ou ',') está nessa linha.
    Retorna (header_index, sep) ou (None, None) se não encontrar.
    """
    with open(csv_file_path, 'r', encoding=encoding) as f:
        lines = f.read().splitlines()

    for i, line in enumerate(lines):
        split_comma, split_semicolon = try_split(line)
        if "Ativo" in split_comma and "Operação Tipo" in split_comma:
            return i, ','
        if "Ativo" in split_semicolon and "Operação Tipo" in split_semicolon:
            return i, ';'
    return None, None

##############################################################################
# Função principal de conversão
##############################################################################
def convert_new_layout_mb_csv_to_koinly(input_csv, output_csv):
    """
    Converte o novo extrato do Mercado Bitcoin para layout Koinly (Sent/Received).
    Espera colunas (em alguma linha):
      Ativo,Operação Tipo,Operação Data/Hora,
      Preço BRL,Liquido BRL,Bruto BRL,Liquido Cripto,Bruto Cripto
    
    Gera um CSV final:
      Date,Sent Amount,Sent Currency,Received Amount,Received Currency,
      Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,Label,Description,TxHash
    """

    # 1) Encontrar a linha do cabeçalho e o separador
    header_line_index, sep = find_header_line_and_sep(input_csv, encoding='utf-8-sig')
    if header_line_index is None or sep is None:
        raise ValueError("Não foi possível encontrar a linha com 'Ativo' e 'Operação Tipo' no CSV.")

    # 2) Ler o DataFrame pulando linhas até o cabeçalho
    df = pd.read_csv(
        input_csv,
        sep=sep,
        encoding='utf-8-sig',        # remove BOM
        skiprows=header_line_index,  # pula até a linha do cabeçalho
        header=0,                    # primeira linha lida agora é o cabeçalho
        engine='python',
        dtype=str
    )

    expected_cols = [
        "Ativo", "Operação Tipo", "Operação Data/Hora",
        "Preço BRL", "Liquido BRL", "Bruto BRL",
        "Liquido Cripto", "Bruto Cripto"
    ]
    for col in expected_cols:
        if col not in df.columns:
            raise ValueError(f"Coluna '{col}' não encontrada. Colunas presentes: {df.columns.tolist()}")

    # Padronizar colunas
    df.rename(columns=lambda x: x.strip(), inplace=True)

    # Remover aspas e espaços extras
    for c in expected_cols:
        df[c] = df[c].str.strip().str.replace('"', '', regex=False)

    # 3) Detectar e converter data/hora
    dt_fmt = auto_detect_format_for_series(df["Operação Data/Hora"], sample_size=5)
    if not dt_fmt:
        dt_fmt = "%d/%m/%Y %H:%M:%S"
        print(f"[Aviso] Não foi possível detectar formatação de data. Usando fallback: {dt_fmt}")

    df["DataHora"] = pd.to_datetime(df["Operação Data/Hora"], format=dt_fmt, errors="coerce")
    invalid_dates = df["DataHora"].isna().sum()
    if invalid_dates > 0:
        print(f"[AVISO] {invalid_dates} linha(s) com data inválida foram removidas.")
        df = df.dropna(subset=["DataHora"])

    df.sort_values("DataHora", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 4) Converter colunas numéricas usando parse_number_with_fallback
    numeric_cols = ["Preço BRL", "Liquido BRL", "Bruto BRL", "Liquido Cripto", "Bruto Cripto"]
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: parse_number_with_fallback(x if x else ""))

    # >>> Debug: imprimir as primeiras linhas para conferir
    print("\n=== [DEBUG] DF após conversão numérica (head(30)) ===")
    print(df.head(30))

    # 5) Construir DataFrame final do Koinly
    koinly_rows = []

    def create_koinly_line(
        dt,
        sent_amt, sent_cur,
        received_amt, received_cur,
        fee_amt, fee_cur,
        label, description,
        net_worth_amt=None, net_worth_cur="BRL"
    ):
        date_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        return {
            "Date": date_str,
            "Sent Amount": sent_amt if sent_amt else "",
            "Sent Currency": sent_cur if sent_cur else "",
            "Received Amount": received_amt if received_amt else "",
            "Received Currency": received_cur if received_cur else "",
            "Fee Amount": fee_amt if fee_amt else "",
            "Fee Currency": fee_cur if fee_cur else "",
            "Net Worth Amount": net_worth_amt if net_worth_amt else "",
            "Net Worth Currency": net_worth_cur if net_worth_amt else "",
            "Label": label,
            "Description": description,
            "TxHash": ""
        }

    for idx, row in df.iterrows():
        ativo = (row["Ativo"] or "").strip()
        oper_type = (row["Operação Tipo"] or "").upper().strip()
        dt = row["DataHora"]

        bruto_brl = row["Bruto BRL"] or 0
        liq_brl   = row["Liquido BRL"] or 0
        bruto_cr  = row["Bruto Cripto"] or 0
        liq_cr    = row["Liquido Cripto"] or 0

        diff_brl = bruto_brl - liq_brl
        diff_cr  = bruto_cr  - liq_cr
        fee_amt = 0
        fee_cur = ""

        # Se houver diferença em BRL, assumimos taxa em BRL
        if abs(diff_brl) > 1e-8:
            fee_amt = diff_brl
            fee_cur = "BRL"
        # Caso contrário, se houver diferença em cripto
        elif abs(diff_cr) > 1e-8:
            fee_amt = diff_cr
            fee_cur = ativo

        sent_amt = 0
        sent_cur = ""
        received_amt = 0
        received_cur = ""
        label = ""
        description = ""
        net_worth_amt = ""

        if "TRADING-OUT" in oper_type:
            # Venda de cripto -> Sent cripto, Received BRL
            sent_amt = liq_cr
            sent_cur = ativo
            received_amt = liq_brl
            received_cur = "BRL"
            label = "Trade"
            description = f"Venda de {ativo} - Mercado Bitcoin"
            net_worth_amt = liq_brl

        elif "TRADING-IN" in oper_type:
            # Compra de cripto -> Sent BRL, Received cripto
            sent_amt = liq_brl
            sent_cur = "BRL"
            received_amt = liq_cr
            received_cur = ativo
            label = "Trade"
            description = f"Compra de {ativo} - Mercado Bitcoin"
            net_worth_amt = liq_brl

        elif "WALLET-OUT" in oper_type:
            # Retirada de cripto
            sent_amt = liq_cr
            sent_cur = ativo
            label = "Withdrawal"
            description = f"Retirada de {ativo} - Mercado Bitcoin"

        elif "WALLET-IN" in oper_type:
            # Depósito de cripto
            received_amt = liq_cr
            received_cur = ativo
            label = "Deposit"
            description = f"Depósito de {ativo} - Mercado Bitcoin"

        elif "CASH-OUT" in oper_type and ativo == "BRL":
            # Saída de BRL
            sent_amt = liq_brl
            sent_cur = "BRL"
            label = "Withdrawal"
            description = "Saque de BRL - Mercado Bitcoin"

        else:
            print(f"[AVISO] Operação não reconhecida: {oper_type}, Ativo={ativo} (linha={idx})")
            continue

        fee_str = fee_amt if abs(fee_amt) > 1e-8 else ""
        fee_cur_str = fee_cur if abs(fee_amt) > 1e-8 else ""

        line_dict = create_koinly_line(
            dt=dt,
            sent_amt=sent_amt,
            sent_cur=sent_cur,
            received_amt=received_amt,
            received_cur=received_cur,
            fee_amt=fee_str,
            fee_cur=fee_cur_str,
            label=label,
            description=description,
            net_worth_amt=net_worth_amt
        )
        koinly_rows.append(line_dict)

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

    print(f"\nConversão finalizada! Geramos {len(df_koinly)} transações.")
    df_koinly.to_csv(output_csv, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
    print(f"Arquivo de saída: {output_csv}")

##############################################################################
# Exemplo de uso
##############################################################################
if __name__ == "__main__":
    input_file = "extrato_mercadobitcoin.csv"   # Ajuste para o nome do seu arquivo
    output_file = "koinly_output_novo.csv"
    convert_new_layout_mb_csv_to_koinly(input_file, output_file)
    print("Concluído.")
