# ...existing code...
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Orquestrador: confere bucket, cria paths (.keep) e chama os 3 ingestors

from minio import Minio
from io import BytesIO
from datetime import datetime

import dbloja_ingest
import json_ingest
import ibge_ingest

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE     = False
BUCKET_NAME      = "bronze"

DBLOJA_PREFIX = "dbloja"
IBGE_PREFIX   = "ibge"
JSON_PREFIX   = "json"

def _connect_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    client.list_buckets()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")
    else:
        print(f"🪣 Bucket '{BUCKET_NAME}' OK.")
    return client

def _ensure_dir(client: Minio, prefix: str, date_str: str):
    path = f"{prefix}/data={date_str}/"
    keep_key = f"{path}.keep"
    client.put_object(BUCKET_NAME, keep_key, BytesIO(b""), length=0)
    print(f"📁 path pronto: {path}")
    return path

def main():
    from datetime import datetime
    
    # Obter o objeto datetime atual
    now = datetime.now()

    # Formatar a data (YYYYMMDD)
    date_str = now.strftime("%Y%m%d")

    # Formatar a hora (HHMMSS)
    time_str = now.strftime("%H%M%S")

    print("Data:", date_str)
    print("Hora:", time_str)

    client = _connect_minio()
    date_str = datetime.now().strftime("%Y%m%d")

    _ensure_dir(client, DBLOJA_PREFIX, date_str)
    _ensure_dir(client, IBGE_PREFIX, date_str)
    _ensure_dir(client, JSON_PREFIX, date_str)

    print("\n▶️ Iniciando cargas…")
    dbloja_ingest.run(client, date_str)
    json_ingest.run(client, date_str)
    ibge_ingest.run(client, date_str)

    print("\n✅ Orquestração finalizada com sucesso.")

if __name__ == "__main__":
    main()