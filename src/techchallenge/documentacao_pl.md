# Documentação Técnica: Pipeline de Dados Educacionais (Arquitetura Medalhão)

Esta documentação descreve a arquitetura técnica e o funcionamento das pipelines de dados desenvolvidas para o processamento de indicadores de alfabetização nacional. O sistema adota a arquitetura medalhão (**Bronze**, **Silver** e **Gold**) estruturada na AWS via cliente S3 customizado, utilizando processamento distribuído em lote (*batch*) e em formato de arquivos altamente otimizado (Parquet).

---

## 1. Arquitetura Geral do Pipeline

O fluxo de dados é dividido em três etapas sequenciais executadas de forma isolada, garantindo robustez, auditabilidade e separação de conceitos.

```
[Fontes de Dados] -> (ExtractionService) 
                          │
                          ▼
            ┌───────────────────────────┐
            │       Camada Bronze       │  <-- Dados brutos em formato Parquet
            └─────────────┬─────────────┘
                          │  + Streaming de Alunos
                          ▼
            ┌───────────────────────────┐
            │       Camada Silver       │  <-- Dados limpos e padronizados
            └─────────────┬─────────────┘
                          │  
                          ▼
            ┌───────────────────────────┐
            │        Camada Gold        │  <-- Modelagem Star Schema (Fatos e Dimensões)
            └───────────────────────────┘

```

---

## 2. Descrição Detalhada das Camadas

### Camada Bronze (`BronzePipeline`)

A classe `BronzePipeline` é responsável pela ingestão de dados brutos a partir dos sistemas de origem.

* **Objetivo:** Garantir a extração íntegra das tabelas configuradas em `BRONZE_TABLES` sem realizar transformações de negócio complexas.
* **Mecanismo de Escrita:** Salva os dados brutos no bucket S3 na camada `bronze/` em formato Parquet para ganho de performance e custo de armazenamento.
* **Monitoramento:** Utiliza a classe `PipelineMonitor` para registrar o tempo de início, fim, quantidade de linhas, colunas e tamanho do arquivo salvo.

---

### Camada Silver (`SilverPipeline`)

A classe `SilverPipeline` executa a higienização, padronização, enriquecimento dos dados e a unificação com fontes incrementais (*streaming*).

#### Processamento Incremental de Alunos (*Streaming*)

A tabela de alunos possui um comportamento diferenciado devido à necessidade de ingestão contínua:

1. **Leitura Base:** Carrega o dado histórico consolidado da camada Bronze (`bronze/alunos.parquet`).
2. **Consolidação Incremental:** Verifica o diretório `bronze/streaming/alunos`. Caso existam arquivos pendentes, realiza a leitura de todos, concatena-os em um dataframe único utilizando Pandas e realiza a junção com os dados históricos.
3. **Transformação:** Aplica as regras de negócio declaradas na função externa `bronze_to_silver`.
4. **Arquivamento Seguro:** Após salvar a tabela processada na camada Silver, move os arquivos brutos de streaming para a pasta `bronze/streaming/processed` para evitar reprocessamento duplicado.

---

### Camada Gold (`GoldPipeline`)

A classe `GoldPipeline` transforma os dados limpos da camada Silver em tabelas prontas para consumo por ferramentas de Business Intelligence e modelos preditivos de Inteligência Artificial, utilizando a modelagem multidimensional (**Star Schema**).

#### Modelagem Dimensional Criada:

##### Tabelas de Dimensão:

* **`dim_municipio`**: Contém chaves e nomes descritivos de municípios e estados (UF).
* **`dim_rede`**: Cadastra os tipos de redes de ensino (`ID_Rede`, `Rede`).
* **`dim_tempo`**: Mapeia o histórico temporal baseado no ano letivo.
* **`dim_uf`**: Consolidação geográfica a nível de estado.

##### Tabelas Fato:

A pipeline calcula e consolida indicadores de desempenho agrupados em três níveis de agregação geográfica:

1. **`fato_alfabetizacao_municipio`**
2. **`fato_uf`**
3. **`fato_brasil`**

#### Fórmulas de Indicadores Calculados:

As seguintes métricas estatísticas de desempenho são calculadas de forma dinâmica no método `calculate_metrics`:

* **Taxa de Alfabetização ($T_{alfa}$):**

$$T_{alfa} = \left( \frac{\text{Alunos Alfabetizados}}{\text{Alunos Avaliados}} \right) \times 100$$


* **Taxa de Presença ($T_{presenca}$):**

$$T_{presenca} = \left( \frac{\text{Alunos Presentes}}{\text{Alunos Avaliados}} \right) \times 100$$


* **Taxa de Preenchimento de Prova ($T_{preenchimento}$):**

$$T_{preenchimento} = \left( \frac{\text{Provas Preenchidas}}{\text{Alunos Avaliados}} \right) \times 100$$



#### Enriquecimento e Alinhamento de Metas Nacionais:

Através do método `add_meta`, o pipeline cruza dinamicamente os resultados obtidos com os planos de metas nacionais anualizadas (`2024` a `2030`), unificando e gerando uma coluna de target (`meta`) baseada no ano letivo em execução. Isso simplifica análises de desvio e previsibilidade temporal.

---

## 3. Tratamento de Erros, Telemetria e Monitoramento

Toda a arquitetura é projetada sob o princípio da resiliência operacional:

* **Tratamento de Exceções:** Cada processamento de tabela dentro dos pipelines roda sob blocos individuais de `try-except`. Se o processamento de uma tabela falhar, o pipeline loga o erro via `logging.exception` e avança para a próxima tabela sem interromper a execução do fluxo principal.
* **Métricas de Infraestrutura:** Ao final de cada execução de camada, são calculados o tamanho das bases na origem e no destino em bytes (`get_layer_size`), permitindo auditoria direta do fator de compressão do formato Parquet.
* **Sumarização:** Ao fim da execução de cada camada do pipeline, o objeto `PipelineSummary` exibe um consolidado com a volumetria e estatísticas de processamento de dados.
