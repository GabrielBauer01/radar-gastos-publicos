# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingestão de deputados (bronze)
# MAGIC
# MAGIC **Lê:** API de Dados Abertos da Câmara (`/deputados`)
# MAGIC **Escreve:** JSON cru em `/Volumes/radar_gastos_publicos/bronze/raw/deputados/` e a tabela `bronze.deputados_raw`
# MAGIC
# MAGIC A lista é um retrato atual (deputados em exercício), por isso a tabela é sobrescrita a cada execução.
# MAGIC Cada execução grava um arquivo novo no Volume (histórico da fonte), mas a tabela lê **apenas o arquivo
# MAGIC desta execução**, para que rodar duas vezes não duplique registros.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F

CATALOG = "radar_gastos_publicos"
BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
RAW_DIR = f"/Volumes/{CATALOG}/bronze/raw/deputados"
TABLE = f"{CATALOG}.bronze.deputados_raw"

os.makedirs(RAW_DIR, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração: percorre todas as páginas da API

# COMMAND ----------

def fetch_all(path, params=None):
    """Percorre a paginação da API da Câmara e devolve todos os registros."""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    url = f"{BASE_URL}/{path}"
    params = {**(params or {}), "itens": 100, "pagina": 1}
    registros = []

    while True:
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        corpo = r.json()
        registros.extend(corpo["dados"])
        tem_proxima = any(l["rel"] == "next" for l in corpo.get("links", []))
        if not tem_proxima:
            break
        params["pagina"] += 1

    return registros

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cópia fiel da fonte: JSON cru no Volume

# COMMAND ----------

deputados = fetch_all("deputados")
print(f"{len(deputados)} deputados baixados")
assert len(deputados) > 0, "a API devolveu uma lista vazia; abortando antes de sobrescrever a bronze"

ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
arquivo = f"{RAW_DIR}/deputados_{ts}.json"

with open(arquivo, "w", encoding="utf-8") as f:
    for d in deputados:
        f.write(json.dumps(d, ensure_ascii=False) + "\n")

print("Salvo em:", arquivo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: tabela Delta com colunas de auditoria

# COMMAND ----------

df = (
    spark.read.json(arquivo)  # só o arquivo desta execução, não a pasta inteira
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)

n = spark.table(TABLE).count()
print(n, "linhas na", TABLE)
assert n == len(deputados), f"tabela ({n}) diferente do baixado ({len(deputados)})"
