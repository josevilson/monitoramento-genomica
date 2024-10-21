# %%

from confluent_kafka import Producer
import requests
import json
import time

# Configurações Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Endereço do servidor Kafka
    'client.id': 'news-api-producer',       # Nome do cliente
}

# Função de callback para lidar com o retorno de mensagens
def delivery_report(err, msg):
    if err:
        print(f"Erro na entrega: {err}")
    else:
        print(f"Mensagem entregue ao tópico {msg.topic()} [{msg.partition()}]")

# Criando o produtor Kafka
producer = Producer(kafka_config)

# API Key e parâmetros da requisição
API_KEY = '5220c4535529484381b6e56b7f95b0aa'
s_tema = "futebol"
s_from = "2024-09-22"
s_to = "2024-10-17"
s_language = "pt"
s_sortBy = "publishedAt"  # "publishedAt" or "popularity" or "relevancy"

# Construindo a URL para a requisição da API
url = (
    'https://newsapi.org/v2/everything?'
    f'q={s_tema}&'
    f'from={s_from}&'
    f'to={s_to}&'
    f'language={s_language}&'
    f'sortBy={s_sortBy}&'
    f'apiKey={API_KEY}'
)

# Requisição para a API
response = requests.get(url)

# Checando se a requisição foi bem-sucedida
if response.status_code == 200:
    # Convertendo a resposta para JSON
    news_data = response.json()

    # Publicando cada artigo de notícia no Kafka
    for article in news_data.get('articles', []):
        # Preparando a mensagem como string JSON
        message = json.dumps(article)
        print(f'###{message}###')
        
        # Publicando a mensagem no tópico "news_topic"
        producer.produce('news_topic', value=message, callback=delivery_report)
        time.sleep(5)
        
        # Forçando a entrega das mensagens
        producer.poll(0)

    # Forçando a entrega de qualquer mensagem pendente
    producer.flush()

    print("Todos os artigos foram enviados para o Kafka com sucesso!")
else:
    print(f"Falha ao obter notícias. Código de status: {response.status_code}")

# %%
