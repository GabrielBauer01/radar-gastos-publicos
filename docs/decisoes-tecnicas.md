# Decisões técnicas

Registro das decisões de engenharia do **Radar de Gastos Públicos**, com o motivo de cada uma e o que foi observado nos dados que a justifica. Os números vêm da carga feita em novembro de 2026 (2023 a 2026, sendo 2026 parcial).

Quando algo é **interpretação** e não um fato documentado pela fonte, está marcado como tal. Quando uma conclusão se apoia em uma amostra, e não na base inteira, o texto diz isso.

---

## 1. Fonte: arquivo anual de despesas, e não a API

**Decisão.** As despesas vêm dos arquivos anuais da Câmara (`https://www.camara.leg.br/cotas/Ano-{ano}.csv.zip`). A API de Dados Abertos é usada só para a lista de deputados.

**Motivo.** O plano inicial era ingerir despesas pela API (`/deputados/{id}/despesas`). O endpoint respondeu `200` com `"dados": []` em todos os testes: 15 deputados sorteados × 5 filtros (sem filtro, 2023, 2024, 2025 e 2026). O endpoint de detalhe do deputado funcionava normalmente, então o problema era específico das despesas.

**Consequências.**
- A carga histórica passou de milhares de chamadas paginadas para 4 downloads.
- O código de retry e paralelismo escrito para a API foi descartado.
- A causa do endpoint vazio é desconhecida (pode ser instabilidade temporária). Se voltar a funcionar, a API pode ser reavaliada para carga incremental.

---

## 2. Bronze: cópia fiel da fonte

**Decisão.** O arquivo cru é guardado no Volume (`bronze/raw`) e carregado em Delta com **todas as colunas como `string`**, mais `_ingested_at`, `_source_file` e `_ano_arquivo`.

**Motivo.** A bronze não corrige nem interpreta nada. Se uma regra da silver estiver errada, o reprocessamento parte da fonte original, sem baixar de novo. As colunas de auditoria dão rastreabilidade (de que arquivo e quando cada linha veio).

---

## 3. Reprocessamento seguro (idempotência)

**Decisão.**
- Despesas: a carga é por ano. Recarregar um ano apaga as linhas daquele ano e insere as novas.
- Anos encerrados são baixados uma vez só. O **ano corrente é baixado de novo a cada execução**, partindo da premissa de que a Câmara atualiza o arquivo do ano em andamento (essa premissa não foi verificada nesta carga).
- Deputados: a tabela é sobrescrita, lendo apenas o arquivo da execução atual.

**Motivo.** Rodar o pipeline duas vezes não pode duplicar dados. Dois defeitos desse tipo foram encontrados na revisão e corrigidos: a bronze de deputados lia a pasta inteira (duplicaria na segunda execução) e o download pulava o arquivo de 2026 se ele já existisse (ficaria desatualizado).

---

## 4. O grão da silver é a linha de despesa, não o documento

**Decisão.** A chave é `id_linha`, um hash SHA-256 de todas as colunas de negócio. Não há `dropDuplicates` por `ideDocumento`.

**Observado.** Havia 639.320 linhas e 609.255 `ideDocumento` distintos. Das repetições:
- 29.972 linhas têm `ideDocumento = 0` (ver decisão 5).
- 94 documentos aparecem duas vezes. Em uma amostra de 20 linhas inspecionadas, o mesmo documento aparece dividido entre categorias da cota (ou entre meses), e os `vlrLiquido` das linhas somam o `vlrDocumento` (ex.: 296,62 + 351,44 = 648,06). A conferência nos 94 documentos inteiros não foi registrada aqui.
- Não há nenhuma linha 100% idêntica.

**Motivo.** Deduplicar por documento apagaria gasto legítimo, pelo menos nos casos inspecionados.

**Consequência importante.** `valor_documento` repete o total da nota em cada linha da divisão. **Somá-lo conta o gasto em dobro.** Toda agregação usa `valor_liquido`. A coluna `valor_documento` na fato tem um comentário no catálogo avisando isso.

O `id_linha` é verificado por teste (`assert`) a cada execução: precisa ser único.

---

## 5. `ideDocumento = 0` significa "sem documento digitalizado"

**Decisão.** Nessas linhas, `id_documento` vira nulo e a coluna `sem_documento` fica `true`. Elas **não** vão para a quarentena.

**Observado.**
- São 29.972 linhas (4,7% da base), todas da categoria **TELEFONIA**, distribuídas pelos quatro anos e por vários deputados.
- Dessas, **25.745 não têm data de emissão**. As outras 4.227 têm data.
- Todas as 25.745 datas de emissão que não convertiam estão dentro desse grupo.
- Nas linhas inspecionadas, não há `urlDocumento`. Isso não foi conferido para o grupo inteiro.

**Interpretação.** Os dados apontam que `0` é a forma da fonte de dizer "sem documento digitalizado". Uma resposta pública da Câmara a um pedido de acesso à informação afirma que as contas de telefonia não são digitalizadas pelo gabinete ([resposta LAI](https://achadosepedidos.org.br/uploads/pedidos/RespostaLAI230328000139278342_1.pdf)). A fonte de dados, porém, não documenta o significado do `0`.

**Motivo da decisão.** É dinheiro real. Descartar subestimaria o gasto com telefonia.

---

## 6. Lideranças e partidos não são parlamentares

**Decisão.** `tipo_beneficiario` é `parlamentar` quando existe `id_cadastro` e `lideranca_partido` caso contrário.

**Observado.** 3.257 linhas (0,5%) não têm `ideCadastro`. Os nomes são lideranças e siglas de partido ("LIDERANÇA DO PT", "PSD", "PODE"...), não pessoas.

**Motivo.** Um ranking de "quem mais gasta" não pode comparar um deputado com uma liderança inteira. A regra é pela **ausência do id**, e não por uma lista de nomes, que quebraria quando surgisse uma liderança nova. Os registros não são descartados, só separados.

---

## 7. Quarentena: existe, mas está vazia

**Decisão.** A silver tem `silver.despesas_quarentena`, com a coluna `motivos_rejeicao`. As regras: `valor_liquido` ilegível, mês fora de 1-12, ano ilegível, id de documento ausente e data preenchida porém inválida.

**Observado.** Nesta carga a quarentena ficou com **0 linhas**. O diagnóstico já mostrava 0 valores inválidos e 0 meses fora do intervalo, e as datas problemáticas eram todas de linhas sem documento (ver decisão 5).

**Motivo de manter a tabela.** Ela protege cargas futuras. A qualidade desta base aparece nas **flags** (`sem_documento`, `estorno`, `tipo_beneficiario`) e nas regras de fornecedor (decisão 9), e não em rejeições.

**Estornos.** 23 linhas têm `valor_liquido` negativo. Ficam na silver com `estorno = true`, porque ignorá-las ou somá-las sem distinção mudaria o total.

---

## 8. Tipos e dinheiro

**Decisão.** `try_cast` em vez de `cast`, e valores monetários em `decimal(15,2)`.

**Motivo.** Dependendo da configuração do Spark, um `cast` com valor ilegível pode interromper o processamento inteiro. Com `try_cast`, a linha fica nula e é tratada pelas regras de qualidade. Dinheiro em ponto flutuante acumula erro de arredondamento nas somas.

---

## 9. Fornecedor: CNPJ válido é chave, CNPJ inválido não

**Decisão.** O documento do fornecedor é validado pelo **dígito verificador** (CNPJ e CPF).
- Documento válido: é a chave (`id_fornecedor`).
- Documento inválido: a chave é um hash de documento + nome normalizado.

**Observado.**
- 45.836 CNPJs e 406 CPFs distintos passam na validação. 5 CNPJs não passam, e todos são placeholders de zeros (`00000000000000`, `...01`, `...02`, `...06`, `...10`).
- O placeholder `00000000000010` estava associado a **671 nomes de fornecedores diferentes**. Na primeira versão da `dim_fornecedor`, todos colapsavam em um só. O nome mais frequente nele é "Adobe Systems Software Ireland Ltd".
- Linhas com documento inválido: 32.471 (5,1% da base), somando cerca de R$ 5,35 milhões (aproximadamente 0,7% do valor líquido total).
- Depois da correção, a dimensão tem 46.811 fornecedores: 46.242 com documento válido e, por diferença, 569 sem.

**Interpretação.** O padrão sugere que o `...10` é usado para fornecedores estrangeiros, mas a fonte não documenta isso.

**Motivo.** Usar o CNPJ como chave sem validar atribuiria os gastos de centenas de fornecedores distintos a uma entidade só, o que distorceria qualquer ranking ou modelo por fornecedor. A validação é uma **informação** (`doc_valido`), e nenhum fornecedor é removido por causa dela.

---

## 10. Modelo dimensional

| Tabela | Grão | Observação |
|---|---|---|
| `fato_despesa` | uma linha de despesa | 639.320 linhas |
| `dim_deputado` | um parlamentar | 859 |
| `dim_categoria` | uma categoria da cota | 19 |
| `dim_fornecedor` | um fornecedor | 46.811 |
| `dim_tempo` | um mês (competência) | 47 |

**`dim_deputado` nasce do arquivo de despesas, não da API.** A API só lista quem está em exercício (513). O arquivo tem 859 ids distintos, incluindo suplentes e ex-deputados desde 2023. Todos os 513 da API aparecem no arquivo. A API entra só para enriquecer (foto, e-mail, `em_exercicio`).

**Partido e UF ficam na fato** (`partido_na_despesa`, `uf_na_despesa`), e a versão atual fica na dimensão. O parlamentar pode trocar de partido, e uma análise por partido deve usar o da época do gasto.

**`dim_tempo` tem grão mensal.** Parte das despesas (telefonia sem documento) não tem data de emissão, e a competência (ano e mês) existe em todas as linhas.

---

## 11. Testes que rodam junto com o pipeline

Cada notebook termina com `assert`, e uma falha interrompe a execução:
- **Silver:** `silver + quarentena = bronze` e `id_linha` único.
- **Gold:** fato com o mesmo número de linhas da silver, soma de `valor_liquido` idêntica e nenhuma chave órfã (deputado, categoria, fornecedor).
- **Ingestão:** cada ano carregado tem pelo menos uma linha.

---

## 12. Erros encontrados e o que ensinaram

- **Silver vazia por causa de `array_remove`.** A primeira versão montava a lista de motivos de rejeição com `array_remove(..., None)`, que devolve `NULL` no Spark. Resultado: 0 linhas na silver e 0 na quarentena. Quem pegou foi o `assert` de contagem. Corrigido com `F.filter(..., lambda x: x.isNotNull())`.
- **Fornecedores colapsados.** A validação de integridade (nenhuma linha órfã) passou, mas o modelo estava errado, porque a soma total não muda quando fornecedores são agrupados errado. Só a inspeção dos 5 CNPJs inválidos revelou o problema. Teste de contagem e de soma não substituem olhar os dados.
- **Plano que mudou.** A API de despesas foi investigada antes de ser descartada, em vez de seguir adiante com dado vazio.

---

## 13. Limitações conhecidas

- **2026 é parcial** e muda a cada carga. Toda comparação anual deve levar isso em conta.
- **O projeto cobre 2023 em diante.** É uma escolha de escopo, e não há histórico anterior carregado.
- **Telefonia com várias linhas por deputado e mês.** A maioria tem 1, mas há casos com 2 a 8. O que as separa não foi investigado.
- **Causa do endpoint de despesas vazio:** desconhecida.
- **Gasto atípico não é irregularidade.** Qualquer resultado de detecção de anomalias indica apenas que o valor foge do padrão da categoria, e pode ter explicação legítima.

---

## 14. Planejado (ainda não implementado)

1. Detecção de gasto atípico (referência estatística por categoria e ano, mais um modelo), com experimentos rastreados no MLflow.
2. Dashboard no Databricks SQL.
3. Espaço Genie sobre a gold, se estiver disponível no workspace.
4. README com diagrama de arquitetura e resultados.
