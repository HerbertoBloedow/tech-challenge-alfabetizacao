# Documentação Técnica: Pipelines de Dados (`pipeline.py`)

**Projeto:** Tech Challenge - Alfabetização

**Arquitetura de Dados:** Arquitetura Medallion (Bronze, Silver e Gold)

**Tecnologias Core:** Python 3, Pandas, AWS S3 Client (`S3Client`), Apache Parquet

---

## 1. Visão Geral da Solução

O módulo centraliza a orquestração de dados do projeto de alfabetização através de três pipelines sequenciais (`BronzePipeline`, `SilverPipeline` e `GoldPipeline`). O objetivo final é extrair dados operacionais brutos, limpá-los, consolidar arquivos vindos por streaming e estruturar uma modelagem dimensional (Tabelas Fato e Dimensão) pronta para consumo em ferramentas de Business Intelligence (BI) e dashboards.

---

## 2. Arquitetura dos Pipelines

O fluxo de dados segue a transformação em três estágios lógicos armazenados isoladamente em camadas no S3:

### 2.1. Camada Bronze (`BronzePipeline`)

* **Propósito:** Ingestão de dados brutos (*as-is*).
* **Funcionamento:** Itera sobre a lista de tabelas mapeadas em `BRONZE_TABLES`, extrai os dados via `ExtractionService` e os persiste diretamente no S3 no formato binário de alto desempenho **Parquet**.
* **Métricas Coletadas:** Quantidade de linhas inseridas, colunas e tamanho do arquivo gerado no storage.

### 2.2. Camada Silver (`SilverPipeline`)

* **Propósito:** Limpeza, padronização e consolidação.
* **Diferencial (Tratamento de Streaming):** Possui uma regra de negócio exclusiva para a tabela `alunos`. Se houver arquivos oriundos de ingestão contínua em `bronze/streaming/alunos`, o pipeline carrega todos, faz um append (`pd.concat`) com a carga histórica da Bronze e, após o processamento bem-sucedido, move esses arquivos de streaming para uma pasta de histórico (`bronze/streaming/processed`).
* **Transformação:** Invoca o método externo `bronze_to_silver` para aplicar regras de qualidade nos DataFrames antes de salvá-los de volta na camada Silver.

### 2.3. Camada Gold (`GoldPipeline`)

* **Propósito:** Modelagem Dimensional (Star Schema) focada em indicadores analíticos de alfabetização.
* **Processamento de Metas:** Consolida dinamicamente dados históricos com metas futuras de alfabetização (prospectadas de 2024 até 2030) a nível municipal, estadual (UF) e nacional (Brasil), aplicando um mapeamento dinâmico em relação ao ano vigente do registro (`add_meta`).

---

## 3. Mapeamento do Modelo Dimensional (Camada Gold)

Abaixo está a estrutura de tabelas geradas e disponibilizadas para os analistas de negócio:

### Tabelas Dimensão (`gold/dimensions`)

Organizam os eixos de análise e evitam repetição textual nas tabelas analíticas.

| Nome da Tabela | Colunas Contidas | Origem / Filtro |
| --- | --- | --- |
| **`dim_municipio`** | `id_municipio`, `Municipio`, `UF` | Removidos duplicados, ordenada por nome do Município. |
| **`dim_rede`** | `ID_Rede`, `Rede` | Redes de ensino (Ex: Municipal, Estadual, Privada). |
| **`dim_tempo`** | `ano` | Eixo cronológico das avaliações. |
| **`dim_uf`** | `UF` | Unidades Federativas avaliadas. |

### Tabelas Fato (`gold/facts`)

Consolidam as métricas volumétricas agregadas e as taxas percentuais de performance educacional.

* **`fato_alfabetizacao_municipio`**: Granalidade por Município, Ano e Rede.
* **`fato_alfabetizacao_uf`**: Agrupamento consolidado por Estado, Ano e Rede.
* **`fato_alfabetizacao_brasil`**: Agrupamento macro a nível nacional por Ano e Rede.

---

## 4. Métricas e Fórmulas Calculadas na Camada Gold

As tabelas fato passam por um enriquecimento analítico no método `calculate_metrics`. Abaixo estão as fórmulas matemáticas aplicadas de forma vetorizada via Pandas:

* **Taxa de Alfabetização:** Percentual de alunos avaliados considerados alfabetizados.

$$\text{Taxa de Alfabetização} = \left( \frac{\text{Alunos Alfabetizados}}{\text{Alunos Avaliados}} \right) \times 100$$


* **Taxa de Presença:** Percentual de comparecimento dos alunos no dia da aplicação da avaliação.

$$\text{Taxa de Presença} = \left( \frac{\text{Alunos Presentes}}{\text{Alunos Avaliados}} \right) \times 100$$


* **Taxa de Preenchimento:** Validação de qualidade de dados (auditoria de cadernos de prova preenchidos).

$$\text{Taxa de Preenchimento} = \left( \frac{\text{Provas Preenchidas}}{\text{Alunos Avaliados}} \right) \times 100$$


---

## 5. Resiliência, Monitoramento e Logs

Cada camada do pipeline implementa um ciclo robusto de segurança:

1. **Monitoramento Ativo:** O objeto `PipelineMonitor` captura o estado de execução no início (`monitor.start()`) e calcula o delta de tempo e volumetria ao finalizar (`monitor.finish()`).
2. **Tratamento de Exceções:** Blocos `try/except` individuais por tabela garantem que a falha no processamento de uma tabela específica não quebre a execução do pipeline inteiro. O erro é capturado detalhadamente pelo `logging.exception`.
3. **Sumário de Execução:** Ao final de cada execução de classe (`run`), o objeto `PipelineSummary` imprime na console um relatório consolidado com o status de todas as tabelas e volumetria processada.
