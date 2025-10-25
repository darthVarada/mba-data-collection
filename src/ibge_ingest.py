# ...existing code...
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# IBGE ingest: lê API (ou arquivo) e envia JSON para MinIO via Spark

import os
from io import BytesIO
from datetime import datetime
from pyspark.sql import SparkSession
from minio import Minio

BUCKET_NAME = "bronze"
IBGE_PREFIX = "ibge"
IBGE_API_URL = "https://brasilapi.com.br/api/ibge/uf/v1"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False

def run(minio_client, date_str: str):
    """Lê API IBGE com Spark e envia resultante JSON para MinIO."""
    print("🌐 Iniciando Spark para IBGE...")
    spark = (
        SparkSession.builder
        .appName("IngestIBGE")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df = spark.read.json(IBGE_API_URL)
        print(f"✅ IBGE carregado: {df.count()} registros.")
        df.show(5, truncate=False)

        time_str = datetime.now().strftime("%H%M%S")
        tmp_dir = f"/tmp/ibge_{date_str}_{time_str}"
        df.coalesce(1).write.mode("overwrite").json(tmp_dir)

        json_file = None
        for f in os.listdir(tmp_dir):
            if f.startswith("part-") and f.endswith(".json"):
                json_file = os.path.join(tmp_dir, f)
                break
        if not json_file:
            raise FileNotFoundError("Arquivo part-*.json não encontrado após write do Spark.")

        if not minio_client:
            minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )

        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)
            print(f"🪣 Bucket '{BUCKET_NAME}' criado.")

        file_name = f"ibge_uf_{date_str}_{time_str}.json"
        dest_key = f"{IBGE_PREFIX}/data={date_str}/{file_name}"
        with open(json_file, "rb") as f:
            data = f.read()

        minio_client.put_object(
            BUCKET_NAME,
            dest_key,
            BytesIO(data),
            length=len(data),
            content_type="application/json"
        )
        print(f"📤 IBGE enviado: {BUCKET_NAME}/{dest_key}")

    except Exception as e:
        print(f"❌ Erro IBGE: {e}")
    finally:
        spark.stop()
        print("🔒 Spark IBGE encerrado.")