def create_csv(endpoint: str, name: str):

    import os
    import requests
    import pandas as pd

    # Caminho absoluto da pasta dags
    base_path = os.path.dirname(os.path.dirname(__file__))
    csv_path = os.path.join(base_path, "csv")

    # Cria a pasta se não existir
    os.makedirs(csv_path, exist_ok=True)

    link_api = endpoint
    retorno = requests.get(link_api)
    dados = retorno.json()

    df = pd.json_normalize(dados)

    print(df.head())

    # Caminho final do arquivo
    file_path = os.path.join(csv_path, f"{name}.csv")

    df.to_csv(file_path, index=False)

    print(f"Arquivo salvo em: {file_path}")
