from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import requests
import json

@dag(
    dag_id='mais_a_dag_v3',
    start_date=datetime(2026, 2, 13),
    schedule='30 * * * *',
    catchup=False,
    tags=['+a']
)


#ingestão da API
#criaçlão do csv
#criação da base de dados


def minha_dag_moderna():

    @task
    def captura_conta_dados():
        url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
        response = requests.get(url)
        df = pd.DataFrame(json.loads(response.content))
        return len(df.index)

    @task.branch
    def e_valida(qtd: int):
        if qtd > 1000:
            return 'valido'
        return 'nvalido'

    # Tasks de Bash permanecem como operadores, mas integrados ao fluxo
    valido = BashOperator(
        task_id='valido',
        bash_command="echo 'Quantidade OK'"
    )

    nvalido = BashOperator(
        task_id='nvalido',
        bash_command="echo 'Quantidade não OK'"
    )

    # O fluxo de dados e dependências fica muito mais limpo:
    quantidade = captura_conta_dados()
    decisao = e_valida(quantidade)
    
    decisao >> [valido, nvalido]

# Instancia a DAG
minha_dag_moderna()