-- Databricks notebook source
-- Setup do ambiente: catálogo, schemas e volume do projeto Radar de Gastos Públicos.
-- Rode uma vez (é idempotente: pode rodar de novo sem efeitos colaterais).

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS radar_gastos_publicos
COMMENT 'Projeto Radar de Gastos Públicos: lakehouse com dados abertos da Câmara dos Deputados';

-- COMMAND ----------

USE CATALOG radar_gastos_publicos;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Dados brutos, como vêm da fonte';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Dados limpos, validados e tipados';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Modelo dimensional pronto para consumo';

CREATE VOLUME IF NOT EXISTS bronze.raw COMMENT 'Arquivos brutos antes de virarem tabela';

-- COMMAND ----------

SHOW SCHEMAS IN radar_gastos_publicos;
