def create_csv(endpoint: str, name: str):

    import os
    import requests
    import pandas as pd

    # Caminho dentro do container (volume)
    csv_path = "/opt/airflow/data/landing"

    # Cria a pasta se não existir
    os.makedirs(csv_path, exist_ok=True)

    retorno = requests.get(endpoint)
    retorno.raise_for_status()  # evita salvar CSV vazio se a API falhar

    dados = retorno.json()
    df = pd.json_normalize(dados)

    file_path = os.path.join(csv_path, f"{name}.csv")

    df.to_csv(file_path, index=False)

    print(df.head())
    print(f"Arquivo salvo em: {file_path}")