#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dbloja_ingest.py
Ingesta TUDO do schema db_loja pro MinIO no MESMO path:
bronze/dbloja/data=YYYYMMDD/

Tabelas:
- db_loja.cliente              (full)
- db_loja.categorias_produto   (full)
- db_loja.pedido_cabecalho     (full)
- db_loja.pedido_itens         (full)
- db_loja.produto              (incremental por data_atualizacao)

Watermark de produto fica em:
bronze/dbloja/_produto_watermark.txt
"""

import os
from io import BytesIO
from datetime import datetime

from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error


# =========================
# CONFIG
# =========================
DB_HOST = "db"
DB_PORT = "5432"
DB_NAME = "mydb"
DB_USER = "myuser"
DB_PASS = "mypassword"

BUCKET_NAME = "bronze"

MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE     = False

# watermark do produto (SEM criar pasta extra)
PROD_WATERMARK_KEY = "dbloja/_produto_watermark.txt"


# =========================
# HELPERS DE MINIO
# =========================
def _get_minio(client: Minio | None = None) -> Minio:
    if client:
        return client
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"🪣 Bucket '{bucket}' criado.")
    else:
        print(f"🪣 Bucket '{bucket}' OK.")


def _upload_single_parquet(local_dir: str, client: Minio, bucket: str, dest_key: str):
    """Pega o part-*.parquet do diretório e envia com o nome que queremos."""
    part_file = None
    for f in os.listdir(local_dir):
        if f.startswith("part-") and f.endswith(".parquet"):
            part_file = os.path.join(local_dir, f)
            break

    if not part_file:
        raise FileNotFoundError("part-*.parquet não encontrado após write do Spark.")

    with open(part_file, "rb") as f:
        data = f.read()

    client.put_object(
        bucket,
        dest_key,
        BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    print(f"📤 enviado: {bucket}/{dest_key}")


def _get_last_watermark(client: Minio) -> str | None:
    try:
        resp = client.get_object(BUCKET_NAME, PROD_WATERMARK_KEY)
        content = resp.read().decode("utf-8").strip()
        resp.close(); resp.release_conn()
        if content:
            print(f"🕒 Watermark encontrado: {content}")
            return content
    except S3Error:
        print("ℹ️ Nenhum watermark de produto encontrado (carga FULL).")
    return None


def _save_watermark(client: Minio, value: str):
    payload = value.encode("utf-8")
    client.put_object(
        BUCKET_NAME,
        PROD_WATERMARK_KEY,
        BytesIO(payload),
        length=len(payload),
        content_type="text/plain"
    )
    print(f"📝 Watermark salvo: {value}")


# =========================
# INGESTORES
# =========================
def _ingest_full(
    spark: SparkSession,
    client: Minio,
    date_str: str,
    table_name: str,
    file_prefix: str,
):
    """
    Lê a tabela inteira e salva em:
    dbloja/data={date_str}/{file_prefix}_{date_str}_{time_str}.parquet
    """
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    props = {"driver": "org.postgresql.Driver", "user": DB_USER, "password": DB_PASS}

    print(f"📥 Lendo tabela (FULL): {table_name}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=props)
    df.show(10, truncate=False)

    time_str = datetime.now().strftime("%H%M%S")
    tmp_dir = f"/tmp/{file_prefix}_{date_str}_{time_str}"
    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)

    dest_key = f"dbloja/data={date_str}/{file_prefix}_{date_str}_{time_str}.parquet"
    _upload_single_parquet(tmp_dir, client, BUCKET_NAME, dest_key)


def _ingest_produto_incremental(
    spark: SparkSession,
    client: Minio,
    date_str: str,
):
    """
    Incremental de db_loja.produto baseado em data_atualizacao.
    """
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    props = {"driver": "org.postgresql.Driver", "user": DB_USER, "password": DB_PASS}

    last_wm = _get_last_watermark(client)
    if last_wm:
        query = f"(SELECT * FROM db_loja.produto WHERE data_atualizacao > '{last_wm}') t"
    else:
        query = f"(SELECT * FROM db_loja.produto) t"

    print("📥 Lendo db_loja.produto (incremental)...")
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
    count = df.count()
    print(f"📦 Linhas desta carga: {count}")

    if count == 0:
        print("ℹ️ Nenhum produto novo/atualizado. Saindo sem escrever parquet.")
        return

    df.show(10, truncate=False)

    # pega maior data_atualizacao
    max_row = df.agg({"data_atualizacao": "max"}).collect()[0]
    new_wm_dt = max_row[0]
    new_wm = None
    if new_wm_dt:
        new_wm = new_wm_dt.strftime("%Y-%m-%d %H:%M:%S")

    time_str = datetime.now().strftime("%H%M%S")
    tmp_dir = f"/tmp/dbloja_produto_{date_str}_{time_str}"
    df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)

    dest_key = f"dbloja/data={date_str}/dbloja_produto_{date_str}_{time_str}.parquet"
    _upload_single_parquet(tmp_dir, client, BUCKET_NAME, dest_key)

    if new_wm:
        _save_watermark(client, new_wm)


# =========================
# ENTRYPOINT
# =========================
def run(minio_client: Minio | None = None, date_str: str | None = None):
    print("🚀 Iniciando ingestão db_loja (único arquivo, mesmo diretório)...")
    date_str = date_str or datetime.now().strftime("%Y%m%d")

    spark = (
        SparkSession.builder
        .appName("IngestDBLojaAllSameDir")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    client = _get_minio(minio_client)
    _ensure_bucket(client, BUCKET_NAME)

    try:
        # FULLS
        _ingest_full(spark, client, date_str, "db_loja.cliente", "dbloja_clientes")
        _ingest_full(spark, client, date_str, "db_loja.categorias_produto", "dbloja_categorias")
        _ingest_full(spark, client, date_str, "db_loja.pedido_cabecalho", "dbloja_pedido_cabecalho")
        _ingest_full(spark, client, date_str, "db_loja.pedido_itens", "dbloja_pedido_itens")

        # INCREMENTAL
        _ingest_produto_incremental(spark, client, date_str)

        print("✅ Ingestão db_loja concluída.")
    except Exception as e:
        print(f"❌ Erro na ingestão db_loja: {e}")
    finally:
        spark.stop()
        print("🔒 Spark encerrado.")


if __name__ == "__main__":
    run()
