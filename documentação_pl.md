# Documentação Técnica: Pipeline de Dados Medallion (TechChallenge)

## 1. Visão Geral da Arquitetura

O sistema consiste em uma solução de engenharia de dados estruturada no padrão **Medallion Architecture** (Bronze, Silver e Gold). O objetivo principal do pipeline é a ingestão, padronização e modelagem analítica de dados educacionais relacionados a índices de alfabetização. O armazenamento físico dos dados utiliza o formato **Parquet**, gerenciado de forma abstrata por uma classe utilitária de persistência conectada ao Cloud Storage (`S3Client`).

---

## 2. Estrutura e Fluxo do Pipeline

### Camada Bronze (`BronzePipeline`)

Esta camada realiza a ingestão de dados brutos (*raw data*) a partir de fontes externas sem aplicar transformações estruturais ou de negócio complexas.

* **Processo de Ingestão:** Percorre ciclicamente a coleção configurada em `BRONZE_TABLES`. Para cada tabela, invoca o componente `ExtractionService` para extração dos registros.
* **Volumetria e Cargas:** O método `run` aceita um argumento opcional `limit`, viabilizando execuções parciais para fins de validação, depuração ou cargas iniciais reduzidas.
* **Persistência:** Os conjuntos de dados extraídos são salvos no diretório raiz da camada `bronze`.

### Camada Silver (`SilverPipeline`)

Responsável pela consolidação, limpeza e enriquecimento das estruturas brutas vindas da camada anterior.

* **Processamento Híbrido (Batch + Streaming):**
* Para a entidade `alunos`, o método `load_bronze_table` executa uma busca adaptativa. Caso existam arquivos incrementais pendentes na partição de *streaming* (`bronze/streaming/alunos`), o pipeline carrega e concatena estes micro-lotes ao DataFrame histórico da camada Bronze.
* As demais tabelas seguem fluxo estrito de processamento em lote (*batch*).


* **Arquivamento e Governança:** Concluída a unificação da entidade `alunos`, os arquivos incrementais consumidos são movidos por meio de `archive_streaming_files` para um diretório de histórico processado (`bronze/streaming/processed`), mitigando riscos de reprocessamento e duplicidade.
* **Padronização:** Invoca o módulo externo `bronze_to_silver` para aplicação de tipagem, regras sanitárias e esquemas padronizados, salvando o resultado final na camada `silver`.

### Camada Gold (`GoldPipeline`)

Focada em transformar os dados consolidados da camada Silver em estruturas altamente otimizadas para consumo analítico, modelagem de Business Intelligence (BI) e Data Analytics.

* **Enriquecimento de Metas Temporais:** Carrega referências externas de metas de alfabetização segmentadas por esferas (Município, UF e Brasil). O método interno `add_meta` avalia o ano de referência de cada registro do DataFrame e mapeia dinamicamente o valor correspondente contido nas colunas sazonais (ex: `meta_alfabetizacao_2024` a `meta_alfabetizacao_2030`), eliminando as colunas sobressalentes logo em seguida.
* **Modelagem Dimensional:** Estrutura os dados brutos em um esquema estrela (*Star Schema*), separando os contextos de negócio em tabelas de Dimensão e Fato.

---

## 3. Especificação do Modelo Dimensional (Camada Gold)

### Tabelas de Dimensão (`gold/dimensions`)

Todas as tabelas de dimensão sofrem expurgo de registros duplicados (`drop_duplicates`) e ordenação lógica antes da escrita final em formato Parquet.

| Identificador da Tabela | Atributos/Colunas | Regra de Negócio / Origem |
| --- | --- | --- |
| **`dim_municipio`** | `id_municipio`, `Municipio`, `UF` | Entidades municipais únicas ordenadas pelo nome do município. |
| **`dim_rede`** | `ID_Rede`, `Rede` | Segmentações institucionais (Redes de Ensino) ordenadas por ID. |
| **`dim_tempo`** | `ano` | Linha do tempo contendo os anos fiscais/letivos mapeados na base. |
| **`dim_uf`** | `UF` | Unidades Federativas de ocorrência dos dados. |

### Tabelas Fato (`gold/facts`)

As métricas calculadas em tempo de execução pela camada Gold seguem formulações matemáticas padronizadas pela função `calculate_metrics`, com arredondamento fixado em duas casas decimais:

* **Taxa de Alfabetização:**
  
$$\text{taxa}_{\text{alfabetizacao}} = \left( \frac{\text{alunos}_{\text{alfabetizados}}}{\text{alunos}_{\text{avaliados}}} \right) \times 100$$


* **Taxa de Presença:** 

$$\text{taxa}_{\text{presenca}} = \left( \frac{\text{alunos}_{\text{presentes}}}{\text{alunos}_{\text{avaliados}}} \right) \times 100$$

* **Taxa de Preenchimento:**

$$\text{taxa}_{\text{preenchimento}} = \left( \frac{\text{provas}_{\text{preenchidas}}}{\text{alunos}_{\text{avaliados}}} \right) \times 100$$



#### 1. `fato_alfabetizacao_municipio`

* **Granularidade:** Ano, Município, Unidade Federativa e Rede de Ensino.
* **Agregadores Aplicados:** Contagem absoluta de alunos avaliados e somatórios condicionais baseados em regras categóricas textuais (ex: contagem de strings equivalentes a `"Aluno alfabetizado"`, `"Presente"` e `"Prova preenchida"`).
* **Campos:** `ano`, `id_municipio`, `ID_Rede`, `alunos_avaliados`, `alunos_alfabetizados`, `alunos_presentes`, `provas_preenchidas`, `taxa_alfabetizacao`, `taxa_presenca`, `taxa_preenchimento`, `meta`.

#### 2. `fato_alfabetizacao_uf`

* **Granularidade:** Ano, Unidade Federativa e Rede de Ensino.
* **Agregadores Aplicados:** Agrupamento e consolidação via soma dos valores inteiros da tabela `fato_alfabetizacao_municipio`. As taxas percentuais e o mapeamento de metas estaduais são recalculados pós-agregação.
* **Campos:** `ano`, `UF`, `ID_Rede`, `alunos_avaliados`, `alunos_alfabetizados`, `alunos_presentes`, `provas_preenchidas`, `taxa_alfabetizacao`, `taxa_presenca`, `taxa_preenchimento`, `meta`.

#### 3. `fato_alfabetizacao_brasil`

* **Granularidade:** Ano macro e Rede de Ensino.
* **Agregadores Aplicados:** Consolidação em nível nacional com base nos dados sumarizados de `fato_alfabetizacao_uf`. As taxas globais do país e suas respectivas metas nacionais são processadas após o agrupamento final.
* **Campos:** `ano`, `ID_Rede`, `alunos_avaliados`, `alunos_alfabetizados`, `alunos_presentes`, `provas_preenchidas`, `taxa_alfabetizacao`, `taxa_presenca`, `taxa_preenchimento`, `meta`.

---

## 4. Sistema de Observabilidade, Telemetria e Resiliência

O ecossistema dispõe de mecanismos integrados para monitoramento operacional de volumetria e controle de falhas em tempo de execução:

* **Isolamento de Erros:** A execução dos loops de tabela é isolada em blocos de tratamento de exceções (`try-except`). Falhas de leitura, processamento ou validação de esquema em uma tabela específica são capturadas pelo módulo `logging.exception()`, permitindo que tabelas subsequentes continuem seu fluxo regular sem derrubar a execução global da aplicação.
* **Monitoramento de Performance:** Cada camada gerencia instâncias de `PipelineMonitor` que delimitam o ciclo de vida operacional (métodos `start()` e `finish()`). O encerramento do monitoramento consolida as seguintes métricas técnicas:
* Quantidade de linhas de entrada carregadas (*input rows*).
* Quantidade de linhas geradas após transformações (*output rows*).
* Largura do esquema final (número de colunas).
* Tamanho físico ocupado em bytes nas respectivas camadas de armazenamento.


* **Sumarização Executiva:** Ao término de cada pipeline, a classe `PipelineSummary` agrupa e consolida as métricas capturadas por tabela, exibindo relatórios de auditoria técnica no console através do método `print()`.
