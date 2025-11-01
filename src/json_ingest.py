# ...existing code...
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# JSON local → envia todos os arquivos *.json da pasta para MinIO (sem transformação)

import os
from io import BytesIO
from datetime import datetime

# Obter o objeto datetime atual
now = datetime.now()

# Formatar a data (YYYYMMDD)
date_str = now.strftime("%Y%m%d")

# Formatar a hora (HHMMSS)
time_str = now.strftime("%H%M%S")

print("Data:", date_str)
print("Hora:", time_str)
BUCKET_NAME = "bronze"
JSON_PREFIX = "json"
JSON_PATH   = "/workspace/json"

def run(minio_client, date_str: str):
    if not os.path.isdir(JSON_PATH):
        print(f"⚠️ Pasta JSON não encontrada: {JSON_PATH}")
        return

    files = [f for f in os.listdir(JSON_PATH) if f.lower().endswith(".json")]
    if not files:
        print(f"⚠️ Nenhum .json encontrado em {JSON_PATH}")
        return

    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"🪣 Bucket '{BUCKET_NAME}' criado.")

    for fname in files:
        src = os.path.join(JSON_PATH, fname)
        import re  # (certifique-se de ter isso no topo)
        base_name = re.sub(r'^(dados_)?(.*)\.json$', r'\2', fname, flags=re.IGNORECASE)
        dest_key = f"{JSON_PREFIX}/data={date_str}/{base_name}_{date_str}_{time_str}.json"
        with open(src, "rb") as f:
            blob = f.read()

        minio_client.put_object(
            BUCKET_NAME,
            dest_key,
            BytesIO(blob),
            length=len(blob),
            content_type="application/json"
        )
        print(f"🧾 JSON enviado: {BUCKET_NAME}/{dest_key}")