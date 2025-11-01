# ibge_ingest.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
from io import BytesIO
from datetime import datetime
from minio import Minio

BUCKET_NAME = "bronze"
IBGE_PREFIX = "ibge"
IBGE_API_URL = "https://brasilapi.com.br/api/ibge/uf/v1"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False

def run(minio_client=None, date_str: str = None):
    print("🌐 Iniciando ingest IBGE (sem Spark)...")

    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    # 1) Buscar API
    try:
        resp = requests.get(IBGE_API_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        print(f"✅ IBGE carregado: {len(data)} registros.")
    except Exception as e:
        print(f"❌ Erro ao ler API do IBGE: {e}")
        return

    # 2) Garantir cliente MinIO
    if not minio_client:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )

    # 3) Garantir bucket
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")

    # 4) Montar caminho e subir
    time_str = datetime.now().strftime("%H%M%S")
    file_name = f"ibge_uf_{date_str}_{time_str}.json"
    dest_key = f"{IBGE_PREFIX}/data={date_str}/{file_name}"

    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")

    minio_client.put_object(
        BUCKET_NAME,
        dest_key,
        BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )

    print(f"📤 IBGE enviado: {BUCKET_NAME}/{dest_key}")
    print("🔒 Ingest IBGE finalizado.")
