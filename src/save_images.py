import os
import requests

def save_3images(df):
    # 1. Cria a pasta 'imagens_produtos' se ela não existir
    pasta = "imagens_produtos"
    if not os.path.exists(pasta):
        os.makedirs(pasta)

    # 2. Pega as 3 primeiras linhas
    # O .head(3) garante que pegaremos apenas o topo da lista
    for index, row in df.head(3).iterrows():
        url_imagem = row['image']
        id_produto = row['id']
        
        # Faz o download da imagem
        img_data = requests.get(url_imagem).content
        
        # Define o nome do arquivo (ex: imagem_1.jpg)
        nome_arquivo = f"imagem_{id_produto}.jpg"
        caminho_completo = os.path.join(pasta, nome_arquivo)
        
        # Salva o arquivo no computador
        with open(caminho_completo, 'wb') as handler:
            handler.write(img_data)
            
        print(f"Sucesso: {nome_arquivo} salvo em {pasta}!")