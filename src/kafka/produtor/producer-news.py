# %%
import json
import time
from confluent_kafka import Producer
from datetime import datetime
import random
from time import sleep
# Configuração do Producer Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Endereço do servidor Kafka
    'client.id': 'news-api-producer'        # Nome do cliente
}

# Criando o Kafka Producer
producer = Producer(kafka_config)

# Função de callback para entrega
def delivery_report(err, msg):
    if err is not None:
        print(f"Mensagem falhou: {err}")
    else:
        print(f"Mensagem enviada para {msg.topic()} [{msg.partition()}]")

# Gerar dados aleatórios
def generate_random_news():
    titles = [
        "Breaking News: Market Hits Record High", 
        "Technology Advances in AI and Robotics",
        "Global Warming Threatens Coastal Cities",
        "Sports: Local Team Wins Championship",
        "Health: New Vaccine Shows Promising Results"
    ]
    
    contents = [
        "The stock market reached an all-time high today, driven by strong earnings reports.",
        "AI and robotics continue to transform industries, leading to major breakthroughs.",
        "Rising sea levels caused by global warming are threatening cities around the world.",
        "The local team celebrated their victory in a thrilling championship game.",
        "Researchers have developed a new vaccine that is showing promising results in trials."
    ]
    
    news = {
        "title": random.choice(titles),
        "content": random.choice(contents),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return news

# Enviar mensagens para o tópico Kafka
topic = "faker_news"

for _ in range(10):  # Gerar e enviar 10 notícias como exemplo
    news_data = generate_random_news()
    print(news_data)
    sleep(3)
    # Converter os dados para JSON
    news_json = json.dumps(news_data)
    
    # Imprimir a mensagem que está sendo enviada
    print(f"Enviando mensagem: {news_json}")

    # Enviar para o Kafka
    producer.produce(topic, value=news_json, callback=delivery_report)
    
    # Forçar envio
    producer.poll(1)
    
    # Pausa para simular intervalo entre as mensagens
    time.sleep(2)

# Garantir que todas as mensagens foram enviadas
producer.flush()

# %%
