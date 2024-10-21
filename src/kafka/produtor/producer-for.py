from confluent_kafka import Producer
import requests
import json
import time

# Configurações Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'news-api-producer',
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
s_from = "2024-09-22"
s_to = "2024-10-17"
s_language = "pt"
s_sortBy = "publishedAt"  # "publishedAt" or "popularity" or "relevancy"

def fetch_and_produce_news(theme):
    # Construindo a URL para a requisição da API
    url = (
        'https://newsapi.org/v2/everything?'
        f'q={theme}&'
        f'from={s_from}&'
        f'to={s_to}&'
        f'language={s_language}&'
        f'sortBy={s_sortBy}&'
        f'apiKey={API_KEY}'
    )

    
    response = requests.get(url)

    
    if response.status_code == 200:
        
        news_data = response.json()

        # Publicando cada artigo de notícia no Kafka
        for article in news_data.get('articles', []):
            
            expected_url = "https://removed.com"
            if article.get("url") != expected_url:
                message = json.dumps(article)
                print(f'###{message}###')
                
                
                
                producer.produce('ada-apresentacao', value=message, callback=delivery_report)
                time.sleep(5)  # Espera 5 segundos entre publicações
                    
                    # Forçando a entrega das mensagens
                producer.poll(0)

        # Forçando a entrega de qualquer mensagem pendente
        producer.flush()

        print(f"Todos os artigos sobre '{theme}' foram enviados para o Kafka com sucesso!")
    else:
        print(f"Falha ao obter notícias sobre '{theme}'. Código de status: {response.status_code}")

# Lista de temas para buscar notícias
temas = ["diabetes"]

# Loop para buscar notícias para cada tema
for tema in temas:
    fetch_and_produce_news(tema)
