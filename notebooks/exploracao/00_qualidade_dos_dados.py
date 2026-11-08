# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Exploração: qualidade dos dados de despesas
# MAGIC
# MAGIC Notebook de investigação (não faz parte do pipeline). Ele registra o que foi descoberto na bronze
# MAGIC **antes** de definir as regras da silver. Cada seção traz a pergunta, o código e a conclusão.
# MAGIC
# MAGIC **Lê:** `bronze.despesas_raw` e `bronze.deputados_raw`. Não escreve nada.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "radar_gastos_publicos"
b = spark.table(f"{CATALOG}.bronze.despesas_raw")
dep = spark.table(f"{CATALOG}.bronze.deputados_raw").select(F.col("id").cast("string").alias("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Quem aparece sem `ideCadastro`?
# MAGIC
# MAGIC **Conclusão:** lideranças e partidos (ex.: "LIDERANÇA DO PT", "PSD"), não pessoas.
# MAGIC Na silver, `tipo_beneficiario` é decidido pela existência de `id_cadastro`, e não por lista de nomes.

# COMMAND ----------

total = b.count()
sem_id = b.filter("ideCadastro is null or trim(ideCadastro) = ''").count()
print(f"total: {total} | sem ideCadastro: {sem_id} ({sem_id / total:.1%})")

display(
    b.filter("ideCadastro is null or trim(ideCadastro) = ''")
     .groupBy("txNomeParlamentar").count().orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `ideCadastro` é a chave dos deputados da API?
# MAGIC
# MAGIC **Conclusão:** os 513 ids da API aparecem no arquivo, que tem 859 ids distintos (inclui quem já saiu da Câmara).
# MAGIC Por isso a `dim_deputado` nasce do arquivo de despesas e a API só enriquece.

# COMMAND ----------

ids_arq = b.filter("ideCadastro is not null and trim(ideCadastro) <> ''").select("ideCadastro").distinct()
print("ids distintos no arquivo:", ids_arq.count())
print("ids que batem com a API:", ids_arq.join(dep, ids_arq.ideCadastro == dep.id).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Valores, datas e meses que não convertem

# COMMAND ----------

print("vlrLiquido inválido:", b.filter("try_cast(vlrLiquido as decimal(15,2)) is null").count())
print("datEmissao inválida:", b.filter("try_cast(datEmissao as timestamp) is null").count())
print("numMes fora de 1-12:", b.filter("try_cast(numMes as int) is null or try_cast(numMes as int) not between 1 and 12").count())
print("linhas 100% duplicadas:", total - b.dropDuplicates().count())
print("vlrLiquido negativo (estornos):", b.filter("try_cast(vlrLiquido as decimal(15,2)) < 0").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `ideDocumento` repetido
# MAGIC
# MAGIC **Conclusão:** um único valor, `0`, concentra quase todas as repetições.

# COMMAND ----------

display(b.groupBy("ideDocumento").count().orderBy("count", ascending=False).limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. O que é `ideDocumento = 0`?
# MAGIC
# MAGIC **Conclusão:** todas as linhas são da categoria **TELEFONIA**, com `urlDocumento` vazio e sem data de emissão.
# MAGIC Todas as datas inválidas do arquivo estão nesse grupo. A leitura é que `0` significa "sem documento digitalizado".
# MAGIC Na silver: `id_documento` nulo + flag `sem_documento`, sem ir para a quarentena.

# COMMAND ----------

zero = b.filter("ideDocumento = '0'")
print("linhas com id 0:", zero.count())
display(zero.groupBy("numAno").count().orderBy("numAno"))
display(zero.groupBy("txtDescricao").count().orderBy("count", ascending=False).limit(10))

inv = b.filter("try_cast(datEmissao as timestamp) is null")
print("datas inválidas:", inv.count())
print("...das quais com id 0:", inv.filter("ideDocumento = '0'").count())
print("...com id 0 e data vazia:", zero.filter("datEmissao is null or trim(datEmissao) = ''").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Os documentos que aparecem 2 vezes (id diferente de 0)
# MAGIC
# MAGIC **Conclusão:** não são duplicidade. É a mesma nota dividida entre categorias (ou meses), e os `vlrLiquido` das linhas
# MAGIC somam o `vlrDocumento`. Consequência: o grão é a linha de despesa, não o documento, e **somar `valor_documento`
# MAGIC contaria o gasto em dobro**. Toda análise usa `valor_liquido`.

# COMMAND ----------

dup2 = (
    b.groupBy("ideDocumento").count()
     .filter("count = 2 and ideDocumento <> '0'")
     .select("ideDocumento")
)
display(
    b.join(dup2, "ideDocumento").orderBy("ideDocumento")
     .select("ideDocumento", "txNomeParlamentar", "numSubCota", "txtFornecedor",
             "vlrDocumento", "vlrLiquido", "numParcela", "numLote", "numMes", "numAno")
     .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Telefonia: quantas linhas por deputado e mês?
# MAGIC
# MAGIC Em aberto: a maioria tem 1 linha por deputado por mês, mas há casos com 2 a 8. O que as separa não foi investigado.

# COMMAND ----------

g = zero.groupBy("ideCadastro", "numAno", "numMes").count()
display(g.groupBy("count").count().orderBy("count"))
