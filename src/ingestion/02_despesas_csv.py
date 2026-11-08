# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingestão de despesas da cota parlamentar (bronze)
# MAGIC
# MAGIC **Lê:** arquivos anuais da Câmara (`https://www.camara.leg.br/cotas/Ano-{ano}.csv.zip`)
# MAGIC **Escreve:** CSV descompactado em `/Volumes/radar_gastos_publicos/bronze/raw/despesas_csv/` e a tabela `bronze.despesas_raw`
# MAGIC
# MAGIC **Por que arquivo e não API:** o endpoint `/deputados/{id}/despesas` devolveu lista vazia em todos os testes
# MAGIC (15 deputados × 5 filtros de período), então a fonte passou a ser o arquivo anual oficial.
# MAGIC
# MAGIC **Regras:**
# MAGIC - Bronze é cópia fiel: tudo como `string`, sem correções.
# MAGIC - Anos encerrados são baixados uma vez só; o **ano corrente é sempre baixado de novo**, porque a Câmara o atualiza.
# MAGIC - A carga é por ano e idempotente: recarregar um ano apaga o anterior e insere o novo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

import io
import os
import zipfile
from datetime import date

import requests
from pyspark.sql import functions as F

CATALOG = "radar_gastos_publicos"
RAW_DIR = f"/Volumes/{CATALOG}/bronze/raw/despesas_csv"
TABLE = f"{CATALOG}.bronze.despesas_raw"
URL_ANO = "https://www.camara.leg.br/cotas/Ano-{ano}.csv.zip"
ANOS = [2023, 2024, 2025, 2026]

os.makedirs(RAW_DIR, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download: baixa e descompacta um ano no Volume

# COMMAND ----------

def baixar_ano(ano):
    destino = f"{RAW_DIR}/ano={ano}/despesas.csv"
    ano_encerrado = ano < date.today().year

    if os.path.exists(destino) and ano_encerrado:
        print(f"{ano}: já baixado, pulando")
        return destino

    r = requests.get(URL_ANO.format(ano=ano), timeout=120)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        nomes = z.namelist()
        assert len(nomes) == 1, f"esperava 1 arquivo no zip, veio: {nomes}"
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        with z.open(nomes[0]) as src, open(destino, "wb") as dst:
            dst.write(src.read())

    print(f"{ano}: salvo em {destino} ({os.path.getsize(destino) / 1e6:.1f} MB)")
    return destino

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: CSV para Delta, com colunas de auditoria

# COMMAND ----------

def carregar_bronze(ano):
    caminho = f"{RAW_DIR}/ano={ano}/despesas.csv"
    df = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("multiLine", False)
        .csv(caminho)  # tudo como string: bronze é cópia fiel
    )
    # remove BOM que às vezes gruda no nome da primeira coluna
    df = df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])

    df = (
        df.withColumn("_ano_arquivo", F.lit(ano))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    if spark.catalog.tableExists(TABLE):
        spark.sql(f"DELETE FROM {TABLE} WHERE _ano_arquivo = {ano}")
        df.write.mode("append").saveAsTable(TABLE)
    else:
        df.write.saveAsTable(TABLE)

    n = spark.table(TABLE).filter(f"_ano_arquivo = {ano}").count()
    assert n > 0, f"{ano}: nenhuma linha carregada"
    print(f"{ano}: {n} linhas na bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução

# COMMAND ----------

for ano in ANOS:
    baixar_ano(ano)
    carregar_bronze(ano)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT _ano_arquivo AS ano, COUNT(*) AS linhas
        FROM {TABLE}
        GROUP BY 1
        ORDER BY 1
    """)
)
