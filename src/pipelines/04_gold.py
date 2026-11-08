# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: modelo dimensional
# MAGIC
# MAGIC **Lê:** `silver.despesas` e `bronze.deputados_raw`
# MAGIC **Escreve:** `gold.dim_deputado`, `gold.dim_categoria`, `gold.dim_tempo`, `gold.dim_fornecedor`, `gold.fato_despesa`
# MAGIC
# MAGIC **Decisões (detalhes em `docs/decisoes-tecnicas.md`):**
# MAGIC - `dim_deputado` nasce do arquivo de despesas (inclui quem já saiu da Câmara); a API só enriquece (foto, e-mail, `em_exercicio`).
# MAGIC - `partido_na_despesa` e `uf_na_despesa` ficam na fato (o partido da época do gasto); a versão atual fica na dimensão.
# MAGIC - Documento de fornecedor **válido** (dígito verificador) é a chave do fornecedor. Documento **inválido** (placeholders de zeros)
# MAGIC   gera chave por documento + nome, para não colapsar centenas de fornecedores diferentes em um só.
# MAGIC - `dim_tempo` tem grão mensal (competência), pois parte das despesas (telefonia sem documento) não tem
# MAGIC   data de emissão, e ano/mês existem em todas as linhas.
# MAGIC - **Somar `valor_liquido`, nunca `valor_documento`**: o total da nota se repete nas linhas divididas entre categorias.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "radar_gastos_publicos"
S = f"{CATALOG}.silver.despesas"
G = f"{CATALOG}.gold"

s = spark.table(S)
api = spark.table(f"{CATALOG}.bronze.deputados_raw")


def salvar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{nome}")
    print(f"{nome}: {spark.table(f'{G}.{nome}').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_deputado

# COMMAND ----------

w = Window.partitionBy("id_cadastro").orderBy(F.col("ano").desc(), F.col("mes").desc())
ultimo = (
    s.filter("id_cadastro is not null")
     .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
     .select("id_cadastro", "nome_parlamentar", "partido", "uf")
)

api_enr = api.select(
    F.col("id").cast("int").alias("id_cadastro"),
    F.col("siglaPartido").alias("partido_atual"),
    F.col("siglaUf").alias("uf_atual"),
    F.col("urlFoto").alias("url_foto"),
    F.col("email"),
    F.lit(True).alias("em_exercicio"),
)

dim_deputado = (
    ultimo.join(api_enr, "id_cadastro", "left")
          .withColumn("em_exercicio", F.coalesce("em_exercicio", F.lit(False)))
)
salvar(dim_deputado, "dim_deputado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_categoria e dim_tempo

# COMMAND ----------

chk = s.groupBy("id_categoria").agg(F.countDistinct("categoria").alias("n"))
print("categorias com mais de um nome:", chk.filter("n > 1").count())

dim_categoria = (
    s.groupBy("id_categoria")
     .agg(F.first("categoria", ignorenulls=True).alias("categoria"))
)
salvar(dim_categoria, "dim_categoria")

dim_tempo = (
    s.select("ano", "mes").distinct()
     .withColumn("ano_mes", F.make_date("ano", "mes", F.lit(1)))
     .withColumn("trimestre", F.quarter("ano_mes"))
)
salvar(dim_tempo, "dim_tempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação de CNPJ/CPF (dígito verificador)
# MAGIC
# MAGIC É uma **informação**, não um filtro: nenhum fornecedor é removido por ter documento inválido, só marcado.

# COMMAND ----------

def valida_cnpj(d):
    if not d or len(d) != 14 or d == d[0] * 14:
        return False

    def dv(base, pesos):
        r = sum(int(x) * p for x, p in zip(base, pesos)) % 11
        return 0 if r < 2 else 11 - r

    p1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    d1 = dv(d[:12], p1)
    d2 = dv(d[:12] + str(d1), [6] + p1)
    return d[12:] == f"{d1}{d2}"


def valida_cpf(d):
    if not d or len(d) != 11 or d == d[0] * 11:
        return False
    r = sum(int(d[i]) * (10 - i) for i in range(9)) % 11
    d1 = 0 if r < 2 else 11 - r
    r = sum(int(d[i]) * (11 - i) for i in range(10)) % 11
    d2 = 0 if r < 2 else 11 - r
    return d[9:] == f"{d1}{d2}"


@F.udf("boolean")
def doc_valido(doc, tipo):
    if tipo == "cnpj":
        return valida_cnpj(doc)
    if tipo == "cpf":
        return valida_cpf(doc)
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_fornecedor

# COMMAND ----------

docs = (
    s.select("doc_fornecedor", "tipo_doc_fornecedor").distinct()
     .withColumn("doc_valido", doc_valido("doc_fornecedor", "tipo_doc_fornecedor"))
)
s2 = s.join(docs.select("doc_fornecedor", "doc_valido"), "doc_fornecedor", "left")

inv = s2.filter(~F.col("doc_valido"))
print(
    "linhas com doc inválido:", inv.count(),
    "| nomes distintos:", inv.select("fornecedor").distinct().count(),
    "| valor_liquido:", inv.agg(F.sum("valor_liquido")).first()[0],
)

# chave: doc válido -> o próprio doc; inválido -> hash de doc + nome
s2 = s2.withColumn(
    "id_fornecedor",
    F.when(F.col("doc_valido"), F.col("doc_fornecedor"))
     .otherwise(F.sha2(F.concat_ws("|", F.col("doc_fornecedor"), F.upper(F.trim("fornecedor"))), 256)),
)

wn = Window.partitionBy("id_fornecedor").orderBy(F.col("n").desc())
nomes = (
    s2.groupBy("id_fornecedor", "doc_fornecedor", "tipo_doc_fornecedor", "doc_valido", "fornecedor")
      .agg(F.count("*").alias("n"))
      .withColumn("_rn", F.row_number().over(wn)).filter("_rn = 1")
)
qtd = s2.groupBy("id_fornecedor").agg(F.countDistinct("fornecedor").alias("qtd_nomes"))

dim_fornecedor = (
    nomes.join(qtd, "id_fornecedor")
         .select(
             "id_fornecedor", "doc_fornecedor",
             F.col("tipo_doc_fornecedor").alias("tipo_doc"),
             F.col("fornecedor").alias("nome_fornecedor"),
             "qtd_nomes", "doc_valido",
         )
)
salvar(dim_fornecedor, "dim_fornecedor")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_despesa

# COMMAND ----------

fato = s2.select(
    "id_linha", "id_documento", "sem_documento", "estorno",
    "tipo_beneficiario", "id_cadastro",
    F.col("nome_parlamentar").alias("beneficiario"),
    F.col("partido").alias("partido_na_despesa"),
    F.col("uf").alias("uf_na_despesa"),
    "id_categoria", "id_fornecedor",
    "ano", "mes", "data_emissao",
    "valor_documento", "valor_glosa", "valor_liquido",
    "url_documento",
)
salvar(fato, "fato_despesa")

spark.sql(f"""
    COMMENT ON COLUMN {G}.fato_despesa.valor_documento IS
    'Total da nota. Repete em linhas divididas: NAO somar. Use valor_liquido.'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testes de qualidade

# COMMAND ----------

f = spark.table(f"{G}.fato_despesa")

assert f.count() == s.count(), "fato diferente da silver"

soma_s = s.agg(F.sum("valor_liquido")).first()[0]
soma_f = f.agg(F.sum("valor_liquido")).first()[0]
assert soma_s == soma_f, f"soma diverge: {soma_s} vs {soma_f}"

sem_dep = (
    f.filter("id_cadastro is not null")
     .join(spark.table(f"{G}.dim_deputado"), "id_cadastro", "left_anti").count()
)
sem_cat = f.join(spark.table(f"{G}.dim_categoria"), "id_categoria", "left_anti").count()
sem_forn = f.join(spark.table(f"{G}.dim_fornecedor"), "id_fornecedor", "left_anti").count()
assert sem_dep == 0 and sem_cat == 0 and sem_forn == 0, (
    f"chaves órfãs: deputado={sem_dep}, categoria={sem_cat}, fornecedor={sem_forn}"
)

print("testes ok | soma valor_liquido:", soma_f)
print("fornecedores:", spark.table(f"{G}.dim_fornecedor").count())

display(
    spark.table(f"{G}.dim_fornecedor")
         .filter("not doc_valido")
         .orderBy(F.desc("qtd_nomes"))
         .limit(10)
)
