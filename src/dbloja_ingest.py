# ...existing code...
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# dbloja_ingest.py
#
# Conecta ao PostgreSQL com Spark, exibe preview e envia o dataset para o bucket MinIO (sem transformação).

import sys
import os
from io import BytesIO
from datetime import datetime
from pyspark.sql import SparkSession
from minio import Minio

# CONFIGURAÇÕES
DB_HOST = "db"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"

BUCKET_NAME = "bronze"
TABLE_NAME = "db_loja.clientes"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE = False

def run(minio_client=None, date_str=None):
    """Extrai tabela db_loja.clientes do PostgreSQL via Spark e envia para o bucket."""
    print("🚀 Iniciando sessão Spark...")
    spark = (
        SparkSession.builder
        .appName("IngestDBLoja")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    props = {"driver": "org.postgresql.Driver", "user": DB_USER, "password": DB_PASS}

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"(SELECT * FROM {TABLE_NAME}) t",
            properties=props
        )

        print("✅ Dados lidos com sucesso:")
        df.show(10, truncate=False)

        date_str = date_str or datetime.now().strftime("%Y%m%d")
        time_str = datetime.now().strftime("%H%M%S")
        parquet_name = f"dbloja_clientes_{date_str}_{time_str}.parquet"
        parquet_local = f"/tmp/{parquet_name}"

        print(f"💾 Salvando localmente: {parquet_local}")
        df.coalesce(1).write.mode("overwrite").parquet(parquet_local)

        part_file = None
        for f in os.listdir(parquet_local):
            if f.startswith("part-") and f.endswith(".parquet"):
                part_file = os.path.join(parquet_local, f)
                break

        if not part_file:
            raise FileNotFoundError("Arquivo part-*.parquet não encontrado após write do Spark.")

        if not minio_client:
            print("🔗 Conectando ao MinIO diretamente...")
            minio_client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )

        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)
            print(f"🪣 Bucket '{BUCKET_NAME}' criado.")

        dest_key = f"dbloja/data={date_str}/{parquet_name}"

        with open(part_file, "rb") as f:
            data = f.read()
        minio_client.put_object(
            BUCKET_NAME,
            dest_key,
            BytesIO(data),
            length=len(data),
            content_type="application/octet-stream"
        )
        print(f"📤 Enviado ao MinIO: {BUCKET_NAME}/{dest_key}")

    except Exception as e:
        print(f"❌ Erro ao extrair ou enviar dados: {e}", file=sys.stderr)
    finally:
        spark.stop()
        print("🔒 Sessão Spark encerrada.")