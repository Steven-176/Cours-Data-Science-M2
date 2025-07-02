from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import os

# Coordonnées des villes
CITIES = {
    "Paris": {"lat": 48.85, "lon": 2.35},
    "London": {"lat": 51.51, "lon": -0.13},
    "Berlin": {"lat": 52.52, "lon": 13.40}
}

# DAG programmé tous les jours à 8h UTC
@dag(
    dag_id="daily_weather_etl",
    start_date=datetime(2025, 7, 2),
    schedule="0 8 * * *",
    catchup=False,
    tags=["weather", "ETL"]
)
def weather_pipeline():

    @task
    def extract():
        results = []
        for city, coords in CITIES.items():
            url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()["current_weather"]
                data["city"] = city
                data["timestamp"] = datetime.utcnow().isoformat()
                results.append(data)
        return results

    @task
    def transform(data_list):
        df = pd.DataFrame(data_list)
        df = df[["city", "temperature", "windspeed", "weathercode", "timestamp"]]
        return df.to_dict(orient="records")  # pour le passer au task suivant

    @task
    def load(cleaned_data):
        df_new = pd.DataFrame(cleaned_data)
        filepath = "/opt/airflow/data/weather_data.csv"

        # Créer dossier s'il n'existe pas
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if os.path.exists(filepath):
            df_existing = pd.read_csv(filepath)
            df_all = pd.concat([df_existing, df_new], ignore_index=True)
            # Supprimer doublons : on considère city + timestamp comme identifiant unique
            df_all.drop_duplicates(subset=["city", "timestamp"], inplace=True)
        else:
            df_all = df_new

        df_all.to_csv(filepath, index=False)

    # Enchaînement
    load(transform(extract()))

dag = weather_pipeline()
