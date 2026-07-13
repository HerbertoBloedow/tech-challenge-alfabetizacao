---

# Pipeline Híbrido para Análise da Alfabetização no Brasil

Este repositório contém a solução desenvolvida para o **Tech Challenge - Fase 2**, que consiste em um pipeline híbrido de dados (Batch e Streaming) estruturado sob a Arquitetura Medalhão para integrar e analisar dados públicos sobre a alfabetização infantil no Brasil.

---

## 📋 Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico de um país. Com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental, foi instituído o **Compromisso Nacional Criança Alfabetizada**, uma política pública que mobiliza a União, estados, Distrito Federal e municípios.

Para apoiar e subsidiar essa iniciativa com parâmetros nacionais, o INEP realizou a **Pesquisa Alfabetiza Brasil** em 2023. O grande desafio para gestores públicos e cientistas de dados é que compreender e mitigar as desigualdades educacionais exige ir além da análise isolada de indicadores. É fundamental cruzar dados macro de metas nacionais com realidades territoriais e microdados educacionais altamente heterogêneos para viabilizar políticas públicas baseadas em evidências.

---

## 🎓 Desafio Educacional e Indicador de Alfabetização

A partir da Pesquisa Alfabetiza Brasil, determinou-se um ponto de corte técnico de **743 pontos** na escala de proficiência do Saeb, estabelecendo o nível mínimo a partir do qual uma criança é considerada formalmente alfabetizada. 

Com base nesse corte, instituiu-se o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar de proficiência. O desafio educacional deste projeto centra-se em consolidar e monitorar este indicador frente às metas estabelecidas, as quais preveem que 100% das crianças atinjam a meta até 2030. A engenharia de dados atua aqui como o motor que viabiliza o cruzamento do indicador de alfabetização com dados de UF, municípios e dados de alunos para identificar gargalos geográficos e operacionais.

---

## 🏛️ Arquitetura Proposta

A arquitetura foi projetada em ambiente de nuvem (**[AWS]**) seguindo o paradigma híbrido (*Lambda Architecture*) para lidar eficientemente com duas velocidades de dados:

1. **Camada Batch (Lote):** Processamento periódico focado em dados históricos e volumosos (Metas nacionais, estaduais e municipais, além de dados territoriais fornecidos pela plataforma *Base dos Dados*).
2. **Camada Streaming (Tempo Real):** Simulação de eventos em tempo quase real para capturar atualizações dinâmicas de indicadores, novas medições de desempenho escolar e alterações imediatas de resultados.

Para garantir a qualidade e governança, os dados progridem através da **Arquitetura Medalhão** (camadas Bronze, Silver e Gold) antes de estarem prontos para o consumo.

---

## ⚙️ Descrição da Arquitetura da Solução

O ecossistema de dados está dividido em quatro etapas funcionais de processamento:

* **Ingestão:** Os dados históricos da *Base dos Dados* são extraídos via rotinas Batch. Paralelamente, eventos contínuos alimentam uma fila de mensagens em tempo real (Streaming).
* **Camada Bronze (Raw Data):** Armazenamento de persistência de dados brutos exatamente como vieram da fonte. O histórico completo é preservado em formato imutável.
* **Camada Silver (Dados Tratados):** Camada onde ocorrem as transformações pesadas: limpeza de dados, tratamento de valores ausentes, padronização de esquemas/tipos, validação de chaves e a unificação/enriquecimento de dados de diferentes tabelas heterogêneas.
* **Camada Gold (Camada Analítica):** Tabelas agregadas e otimizadas em formato colunar altamente performático. Esta camada expõe visões prontas como a *evolução temporal do indicador de alfabetização por município* e a *comparação direta entre metas esperadas e resultados atingidos*.

---

## 🗺️ Diagrama da Pipeline

O fluxo visual abaixo exemplifica como os dados transitam entre as tecnologias e as camadas da arquitetura medalhão:


---

# Arquitetura

```
                +----------------+
                | Banco SQL      |
                +-------+--------+
                        |
                        v
                Bronze (Batch)
                        |
                        |
                        |
 Streaming Producer      |
(simulador_streaming)    |
        |               |
        v               |
temp_streaming_input    |
        |               |
        v               |
 Streaming Consumer      |
        |               |
        v               |
 Bronze (Streaming) -----+
                        |
                        v
                     Silver
                        |
                        v
                      Gold
```

---

# Estrutura do Projeto

```
src/
└── techchallenge/
    ├── batch.py
    ├── streaming.py
    ├── pipeline.py
    ├── batch_orchestrator.py
    ├── streaming_orchestrator.py
    │
    ├── extract/
    ├── transform/
    ├── load/
    ├── monitoring/
    ├── config/
    │
    └── streaming/
        ├── simulador_streaming.py
        └── streaming_consumer.py

data/
├── bronze/
│   ├── batch/
│   └── streaming/
├── silver/
├── gold/
├── temp_streaming_input/
└── temp_streaming_processed/
```

---

## 🔄 Fluxo de Dados

O ciclo de vida do dado dentro da pipeline cumpre o seguinte fluxo lógico:

1. **Coleta e Origem:** Ingestão dos datasets de UF, Municípios, Dados de Alunos e tabelas de Metas a partir do repositório público.
2. **Carga Inicial (Bronze):** Os arquivos são gravados no *Storage* mantendo sua estrutura nativa (ex: JSON ou CSV).
3. **Qualidade e Integração (Silver):** É executado um pipeline que valida regras estritas de qualidade (deduplicação, checagem de nulos e integridade referencial entre os códigos de municípios). O resultado é persistido em formato colunar (ex: **Parquet**) otimizado para consultas.
4. **Consumo Analítico (Gold):** Cruzamento final gerando métricas agregadas por ano, região e rede de ensino, prontas para alimentar dashboards e ferramentas de decisão governamental.
5. **Armazenamento em nuvem:** A implementação utiliza a biblioteca **Boto3**, que é o SDK oficial da AWS para Python, permitindo realizar operações como envio, leitura e gerenciamento de arquivos armazenados em um bucket S3.

---

## 🛠️ Tecnologias Utilizadas e Decisões Arquiteturais

### Ferramentas Escolhidas
* **Provedor de Nuvem:** `[AWS]` devido à maturidade de infraestrutura elástica e ferramentas nativas de Big Data.
* **Processamento Spark / Engine:**
   * Pipeline PySpark
   * Python
   * Pandas
   * SQLAlchemy
   * Parquet
   * PyArrow
* **Armazenamento:** `[Amazon S3]`
  
### Trade-offs Considerados
* **Batch vs. Streaming:** Optou-se por uma pipeline híbrida porque os dados agregados do Saeb mudam anualmente (Batch), mas as atualizações das medições locais de desempenho nas escolas exigem respostas em tempo quase real (Streaming) para intervenções pedagógicas rápidas.
* **Data Lake vs. Data Warehouse:** A utilização da arquitetura Lakehouse (armazenamento em arquivos estruturados como Parquet) foi escolhida em vez de um DW tradicional para manter os custos operacionais baixos, preservando a capacidade de escalar para análise massiva de dados não estruturados.

---

## 📉 Monitoramento e FinOps (Otimização de Custos)

* **Observabilidade:** O pipeline conta com mecanismos de monitoramento para rastrear falhas de ingestão, latência de ponta a ponta e volume de dados processados através de alertas e métricas centralizadas.
  * Cada etapa do pipeline registra:
   
     * Tempo de execução;
     * Quantidade de registros de entrada;
     * Quantidade de registros de saída;
     * Quantidade de colunas processadas;
     * Arquivos produzidos.

  * Ao final da execução é apresentado um resumo da pipeline.

* **Práticas de FinOps:** * Uso mandatório de arquivos compactados em formato **Parquet** com estratégias de particionamento por Ano e UF, reduzindo drasticamente o volume de dados varridos por query.
    * Políticas de ciclo de vida (*Lifecycle*) configuradas para mover dados históricos frios da camada Bronze para classes de armazenamento mais baratas.

---

## 🤖 Aplicação em Inteligência Artificial (IA)

A base estruturada na camada **Gold** serve como insumo ideal e confiável para iniciativas avançadas de Ciência de Dados e IA:
* **Modelos de Predição de Alfabetização:** Criação de modelos preditivos (como regressões ou árvores de decisão) para antecipar quais municípios correm risco de não atingirem a meta de 2030.
* **Análise de Desigualdade e Vulnerabilidade:** Enriquecimento futuro utilizando dados socioeconômicos do IBGE e do Censo Escolar para clusterizar municípios com perfis de vulnerabilidade educacional semelhantes, permitindo a distribuição otimizada de recursos públicos.

```
