from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
import pandas as pd
import json
import os
import io

app = FastAPI()

# CORS
origins = [
    '*'
]

app.add_middleware(CORSMiddleware,
                   allow_origins=origins,
                   allow_credentials=True,
                   allow_methods=['*'],
                   allow_headers=['*'],
                   max_age=3600)

# Inicializar cliente de Google Cloud Storage
load_dotenv()

GOOGLE_APPLICATION_CREDENTIAL = os.getenv('GOOGLE_APPLICATION_CREDENTIAL')
BUCKET_NAME = os.getenv('BUCKET_NAME')

def get_storage_client():
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_APPLICATION_CREDENTIAL)
    return storage.Client(credentials=credentials)

client = get_storage_client()
bucket = client.bucket(BUCKET_NAME)

def download_and_classify_blobs():
    orders = []
    products = []

    blobs = bucket.list_blobs()

    for blob in blobs:
        if "orders" in blob.name.lower() and blob.name.endswith(".csv"):
            content = blob.download_as_text()
            try:
                df = pd.read_csv(io.StringIO(content), on_bad_lines='skip')
                orders.extend(df.to_dict(orient="records"))
            except pd.errors.ParserError as e:
                print(f"Error parsing {blob.name}: {e}")
                continue
        elif "products" in blob.name.lower():
            content = blob.download_as_text()
            try:
                content_json = json.loads(content)
                products.append(content_json)
            except json.JSONDecodeError:
                continue  # Ignorar blobs que no son JSON válidos

    return orders, products


@app.get("/download_all_blobs/")
async def download_all_blobs():
    orders, products = download_and_classify_blobs()

    orders_file_path = "orders.json"
    products_file_path = "products.json"

    # Guardar orders en un archivo JSON
    with open(orders_file_path, "w") as orders_file:
        json.dump(orders, orders_file, indent=4)

    # Guardar products en un archivo JSON
    with open(products_file_path, "w") as products_file:
        json.dump(products, products_file, indent=4)

    return {
        "message": "Blobs downloaded and classified successfully",
        "orders_file": orders_file_path,
        "products_file": products_file_path
    }

        

