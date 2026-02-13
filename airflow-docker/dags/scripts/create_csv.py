
def create_csv(endpoint: str,name: str):

    import os
    import requests
    import pandas as pd

    name = name
    link_api = endpoint
    retorno = requests.get(link_api)

    #adicione o .json()
    dados = retorno.json() 

    # Agora o pandas recebe uma lista de dicionários, e não um objeto de resposta
    df = pd.json_normalize(dados)

    print(df.head())

    df.to_csv(f"{name}.csv", index=False)