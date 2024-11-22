# Databricks notebook source
# MAGIC %md
# MAGIC Cette commande installe la bibliothèque `azure-eventhub` nécessaire pour interagir avec Azure Event Hub.
# MAGIC

# COMMAND ----------

# MAGIC %pip install azure-eventhub

# COMMAND ----------

# MAGIC %md
# MAGIC ### Résumé du Code pour l'Envoi de Données à Azure Event Hub
# MAGIC 1. Ce code simule des données de transactions clients, de logs de serveur et de publications sur les réseaux sociaux, et les envoie à Azure Event Hub pour un traitement ultérieur. 
# MAGIC 2. On utilise des fonctionnalités asynchrones pour gérer l'envoi des données de manière efficace.
# MAGIC

# COMMAND ----------

import asyncio
import random
import json
import time
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import datetime

EVENT_HUB_CONNECTION_STR = "Endpoint=sb://hubcontainer.servicebus.windows.net/;SharedAccessKeyName=sharedPolicy;SharedAccessKey=wox9csTZtBxDg4qNV1D4roHfR74GiB4Bp+AEhNhoqys=;EntityPath=streamhandler"
EVENT_HUB_NAME = "streamhandler"

# Ces variables contiennent la chaîne de connexion et le nom de l'Event Hub.
async def run():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )

    # Cette fonction crée un producteur d'événements, génère les données simulées, les combine en un seul événement JSON, et envoie ces événements à Azure Event Hub par lots.
    async with producer:
        for i in range(400):  # Limite le nombre d'envois pour éviter la surcharge
            event_data_batch = await producer.create_batch()

            # Simuler une transaction client
            transaction = {
                "transactionID": random.randint(1000, 9999),
                "customerID": random.randint(1, 100),
                "amount": round(random.uniform(10.0, 500.0), 2),
                "timestamp": datetime.datetime.now().isoformat(),
                "lastTransactionDate": datetime.datetime(random.randint(2019, 2024), random.randint(1, 12), random.randint(1, 28)).date().isoformat(),
                "type": "Achat vêtements",
                "customerEmail": f"customer{random.randint(1, 100)}@{random.choice(['gmail.com', 'outlook.com', 'live.com', 'apple.com', 'hotmail.com', 'yahoo.fr'])}",
                "customerFirstName": random.choice([
                    "John", "Jane", "Alex", "Emily", "Chris", "Michael", "Sarah", "David", "Laura", "Daniel",
                    "Emma", "James", "Olivia", "Matthew", "Sophia", "Joshua", "Isabella", "Andrew", "Mia", "Joseph",
                    "Charlotte", "Benjamin", "Amelia", "Samuel", "Harper", "William", "Evelyn", "Elijah", "Abigail",
                    "Mason", "Ella", "Logan", "Avery", "Lucas", "Scarlett", "Henry", "Grace", "Alexander", "Chloe",
                    "Sebastian", "Lily", "Jack", "Aria", "Owen", "Zoe", "Gabriel", "Nora", "Carter", "Lillian"
                ]),
                "customerLastName": random.choice([
                    "Doe", "Smith", "Johnson", "Williams", "Brown", "Taylor", "Anderson", "Thomas", "Jackson", "White",
                    "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee",
                    "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott",
                    "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts",
                    "Schmidt", "Müller", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Hoffmann", "Schulz"
                ])
            }

            # Simuler un log de serveur
            log_entry = {
                "logID": random.randint(1, 1000),
                "status": random.choice(["INFO", "ERROR", "WARNING"]),
                "message": f"Processed transaction {transaction['transactionID']}",
                "logDate": transaction["lastTransactionDate"]
            }

            # Simuler un post sur les réseaux sociaux
            social_media_post = {
                "postID": random.randint(1, 5000),
                "customerEmail": transaction["customerEmail"],
                "content": random.choice([
                    "Just bought something amazing!",
                    "Not satisfied with the purchase.",
                    "Great customer service!",
                    "Will definitely buy again.",
                    "Terrible experience, not recommended."
                ]),
                "postDate": transaction["lastTransactionDate"],
                "pseudonym": f"Lolo{transaction['customerFirstName']}_{transaction['customerLastName']}Umami85"
            }

            # Combiner tous les événements en une structure JSON unique
            combined_event = {**transaction, **log_entry, **social_media_post}

            # Ajouter l'événement combiné au lot(batch)
            event_data_batch.add(EventData(json.dumps(combined_event)))

            # Envoyer le lot d'événements à Event Hub
            await producer.send_batch(event_data_batch)
            print(combined_event, end='\n\n')
            time.sleep(5) # Pause pour faciliter le traitement des événements

await run()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Redémarrer le noyau Python pour utiliser les packages mis à jour

# COMMAND ----------

# dbutils.library.restartPython()
