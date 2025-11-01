#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ===========================================================
# ORQUESTRADOR GERAL
# Confere o bucket, cria paths (.keep) e chama os ingestors:
# - dbloja_ingest (todas tabelas e incremental de produto)
# - json_ingest
# - ibge_ingest
# ===========================================================

from minio import Minio
from io import BytesIO
from datetime import datetime

import dbloja_ingest
import json_ingest
import ibge_ingest

# ==============================
# CONFIGURAÇÕES GERAIS
# ==============================
MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_SECURE     = False
BUCKET_NAME      = "bronze"

DBLOJA_PREFIX = "dbloja"
IBGE_PREFIX   = "ibge"
JSON_PREFIX   = "json"


# ==============================
# FUNÇÕES AUXILIARES
# ==============================
def _connect_minio():
    """Conecta ao MinIO e garante a existência do bucket principal."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    # Teste rápido de conexão e criação do bucket, se necessário
    client.list_buckets()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")
    else:
        print(f"🪣 Bucket '{BUCKET_NAME}' OK.")
    return client


def _ensure_dir(client: Minio, prefix: str, date_str: str):
    """Cria o path base no bucket (com .keep) para garantir estrutura Hive-like."""
    path = f"{prefix}/data={date_str}/"
    keep_key = f"{path}.keep"

    client.put_object(BUCKET_NAME, keep_key, BytesIO(b""), length=0)
    print(f"📁 path pronto: {path}")
    return path


# ==============================
# FUNÇÃO PRINCIPAL
# ==============================
def main():
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")

    print("============================================")
    print("🚀 ORQUESTRADOR INICIADO")
    print(f"📅 Data: {date_str}")
    print(f"⏰ Hora: {time_str}")
    print("============================================")

    # Conecta ao MinIO
    client = _connect_minio()

    # Garante que diretórios base existem
    _ensure_dir(client, DBLOJA_PREFIX, date_str)
    _ensure_dir(client, IBGE_PREFIX, date_str)
    _ensure_dir(client, JSON_PREFIX, date_str)

    print("\n▶️ Iniciando cargas...")

    # 1️⃣ Ingestão de todas as tabelas do schema db_loja
    print("\n============================================")
    print("📦 [1/3] Iniciando ingestão DBLOJA...")
    print("============================================")
    dbloja_ingest.run(client, date_str)

    # 2️⃣ Ingestão dos arquivos JSON locais (mock de APIs)
    print("\n============================================")
    print("🧾 [2/3] Iniciando ingestão JSON...")
    print("============================================")
    json_ingest.run(client, date_str)

    # 3️⃣ Ingestão IBGE (API BrasilAPI)
    print("\n============================================")
    print("🌎 [3/3] Iniciando ingestão IBGE...")
    print("============================================")
    ibge_ingest.run(client, date_str)

    print("\n============================================")
    print("✅ ORQUESTRAÇÃO FINALIZADA COM SUCESSO!")
    print("============================================")


# ==============================
# EXECUÇÃO DIRETA
# ==============================
if __name__ == "__main__":
    main()
