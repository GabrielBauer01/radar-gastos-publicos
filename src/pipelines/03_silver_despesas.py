# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: despesas tipadas, validadas e sinalizadas
# MAGIC
# MAGIC **Lê:** `bronze.despesas_raw`
# MAGIC **Escreve:** `silver.despesas` (linhas válidas) e `silver.despesas_quarentena` (linhas rejeitadas, com o motivo)
# MAGIC
# MAGIC **Decisões (detalhes em `docs/decisoes-tecnicas.md`):**
# MAGIC - `try_cast` em vez de `cast`: valor ilegível vira nulo e a linha vai para a quarentena, sem derrubar o pipeline.
# MAGIC - O grão é a linha de despesa, **não** o documento: `ideDocumento` repete em notas divididas entre categorias.
# MAGIC   Por isso a chave é `id_linha` (hash das colunas de negócio) e não há `dropDuplicates`.
# MAGIC - `ideDocumento = 0` significa "sem documento digitalizado" (telefonia): vira nulo + flag `sem_documento`, e **não** vai para a quarentena.
# MAGIC - Estornos (`valor_liquido < 0`) ficam na silver com a flag `estorno`.
# MAGIC - `tipo_beneficiario` é decidido pela existência de `id_cadastro`, não por lista de nomes (lideranças e partidos não têm).
# MAGIC - Valores monetários em `decimal(15,2)`, nunca `double`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "radar_gastos_publicos"
SRC = f"{CATALOG}.bronze.despesas_raw"
SILVER = f"{CATALOG}.silver.despesas"
QUAR = f"{CATALOG}.silver.despesas_quarentena"

b = spark.table(SRC)
colunas_negocio = [c for c in b.columns if not c.startswith("_")]


def nulo_se_vazio(c):
    return F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))


# chave da linha: hash de todas as colunas de negócio (coalesce evita colisão por nulo)
id_linha = F.sha2(
    F.concat_ws("\u001f", *[F.coalesce(F.col(c), F.lit("")) for c in colunas_negocio]), 256
)

t = (
    b.withColumn("_id_doc_raw", nulo_se_vazio("ideDocumento"))
     .withColumn("_data_txt", nulo_se_vazio("datEmissao"))
     .select(
        id_linha.alias("id_linha"),
        F.when(F.col("_id_doc_raw") == "0", None).otherwise(F.col("_id_doc_raw")).alias("id_documento"),
        F.coalesce(F.col("_id_doc_raw") == "0", F.lit(False)).alias("sem_documento"),
        F.expr("try_cast(ideCadastro as int)").alias("id_cadastro"),
        nulo_se_vazio("txNomeParlamentar").alias("nome_parlamentar"),
        nulo_se_vazio("sgPartido").alias("partido"),
        nulo_se_vazio("sgUF").alias("uf"),
        F.expr("try_cast(nuLegislatura as int)").alias("legislatura"),
        F.expr("try_cast(numSubCota as int)").alias("id_categoria"),
        nulo_se_vazio("txtDescricao").alias("categoria"),
        nulo_se_vazio("txtDescricaoEspecificacao").alias("especificacao"),
        nulo_se_vazio("txtFornecedor").alias("fornecedor"),
        F.regexp_replace(F.col("txtCNPJCPF"), r"[^0-9]", "").alias("doc_fornecedor"),
        nulo_se_vazio("txtNumero").alias("numero_documento"),
        F.expr("try_cast(datEmissao as timestamp)").alias("data_emissao"),
        F.expr("try_cast(vlrDocumento as decimal(15,2))").alias("valor_documento"),
        F.expr("try_cast(vlrGlosa as decimal(15,2))").alias("valor_glosa"),
        F.expr("try_cast(vlrLiquido as decimal(15,2))").alias("valor_liquido"),
        F.expr("try_cast(numParcela as int)").alias("num_parcela"),
        nulo_se_vazio("numLote").alias("num_lote"),
        F.expr("try_cast(numMes as int)").alias("mes"),
        F.expr("try_cast(numAno as int)").alias("ano"),
        nulo_se_vazio("urlDocumento").alias("url_documento"),
        F.col("_id_doc_raw").isNull().alias("_id_ausente"),
        (F.col("_data_txt").isNotNull() & F.expr("try_cast(datEmissao as timestamp) is null")).alias("_data_quebrada"),
        F.col("_source_file"),
        F.col("_ingested_at"),
     )
     .withColumn("estorno", F.col("valor_liquido") < 0)
     .withColumn("tipo_beneficiario",
                 F.when(F.col("id_cadastro").isNotNull(), "parlamentar").otherwise("lideranca_partido"))
     .withColumn("tipo_doc_fornecedor",
                 F.when(F.length("doc_fornecedor") == 14, "cnpj")
                  .when(F.length("doc_fornecedor") == 11, "cpf"))
)

# Regras de validação: cada uma gera um motivo de rejeição.
# F.filter remove os nulos do array (F.array_remove(..., None) devolveria NULL e perderia todas as linhas).
motivos = F.filter(
    F.array(
        F.when(F.col("_id_ausente"), F.lit("sem_id_documento")),
        F.when(F.col("valor_liquido").isNull(), F.lit("valor_liquido_invalido")),
        F.when(F.col("mes").isNull() | ~F.col("mes").between(1, 12), F.lit("mes_invalido")),
        F.when(F.col("ano").isNull(), F.lit("ano_invalido")),
        F.when(F.col("_data_quebrada"), F.lit("data_emissao_invalida")),
    ),
    lambda x: x.isNotNull(),
)

marcado = t.withColumn("motivos_rejeicao", motivos)
aux = ["_id_ausente", "_data_quebrada"]

validos = marcado.filter(F.size("motivos_rejeicao") == 0).drop("motivos_rejeicao", *aux)
rejeitados = marcado.filter(F.size("motivos_rejeicao") > 0).drop(*aux)

validos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER)
rejeitados.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(QUAR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testes de qualidade

# COMMAND ----------

n_b = b.count()
n_s = spark.table(SILVER).count()
n_q = spark.table(QUAR).count()
n_ids = spark.table(SILVER).select("id_linha").distinct().count()

print(f"bronze: {n_b} | silver: {n_s} | quarentena: {n_q}")
print(f"id_linha únicos: {n_ids}")
assert n_s + n_q == n_b, "linhas sumiram ou foram duplicadas"
assert n_ids == n_s, "id_linha não é único"

display(spark.table(SILVER).groupBy("tipo_beneficiario", "sem_documento", "estorno").count())
