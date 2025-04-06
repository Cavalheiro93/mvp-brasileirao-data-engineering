# ⚽ MVP - Engenharia de Dados com Databricks, AWS e Redshift

Este projeto foi desenvolvido como parte da Pós-Graduação em Data Science e Analytics da PUC-Rio.  
O objetivo é construir um pipeline completo de Engenharia de Dados usando ferramentas do mercado, com foco em dados reais do Campeonato Brasileiro Série A 2024.


<br></br>
## 📚 Sumário

- [🚀 Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [📁 Estrutura de Pastas](#-estrutura-de-pastas)
- [📒 Notebooks do Projeto](#-notebooks-do-projeto)
- [🧭 Etapas do Projeto](#-etapas-do-projeto)


<br></br>
## 🚀 Tecnologias Utilizadas

- **Databricks Community Edition**: para ingestão, transformação e tratamento dos dados com PySpark (ETL)
- **AWS S3**: armazenamento em Data Lake (Bronze, Silver, Gold)
- **Amazon Redshift Serverless**: utilizado como Data Warehouse para armazenar a camada Gold do projeto, estruturada em Data Marts temáticos focados em clubes, jogadores e métricas estatísticas.
- **PySpark + SQL**: para manipulação, modelagem e criação de data marts
- **GitHub**: versionamento e documentação do projeto


<br></br>
## 📁 Estrutura de Pastas

```plaintext
├── 📁 data/                → Contém os dados divididos por camadas do Data Lake
│   ├── 📂 bronze/          → Dados brutos, exatamente como foram recebidos (raw)
│   ├── 📂 silver/          → Dados tratados, limpos e padronizados
│   └── 📂 gold/            → Dados modelados prontos para análise e consumo (Data Marts)

├── 📁 notebooks/           → Notebooks com cada etapa do pipeline de dados
│   └── 00-Configuracao     → Notebook de configuração de ambiente e credenciais

├── 📁 images/              → Diagramas, capturas de tela e elementos visuais do projeto
├── README.md              → Documentação geral do projeto
└── placeholder.txt        → Arquivo temporário para inicializar o repositório
```


<br></br>
## 📒 Notebooks do Projeto

| Ordem | Nome do Notebook         | Descrição                                                       | Link
|-------|--------------------------|-----------------------------------------------------------------|-------|
| 00    | `00-Configuracao.ipynb`  | Leitura das credenciais, configuração do S3 e testes de conexão |[🔗](notebooks/00-Configuracao.ipynb)


### 📌 Observações
> 🔐 **Credenciais AWS** não estão incluídas no repositório por segurança.  
> O arquivo `aws_credentials.json` foi usado localmente para leitura via Spark no notebook `00-Configuracao`.

---

<br></br>
# 🧭 Etapas do Projeto


## 1. Configuração Inicial 🛠️
O projeto foi construído com base na integração entre `AWS` e `Databricks`, com o objetivo de aprofundar o uso dessas duas tecnologias.
<br>
Durante o processo de conexão do Databricks ao AWS S3, percebi que não seria seguro deixar as chaves de acesso expostas no código, já que qualquer usuário poderia visualizá-las.
<br>
Pensando em boas práticas e segurança, esta etapa inicial foi dedicada à configuração do ambiente, incluindo a criação de um arquivo
`aws_credentials.json`, que permite a conexão com a AWS de forma segura e controlada.


### 1.A - Configuração das chaves de acesso | [00-Configuracao 📎](notebooks/00-Configuracao.ipynb)
- Criação da conta na AWS
- Criação do bucket S3 com as camadas bronze, silver e gold
- Criação de credenciais IAM e geração das chaves de acesso
- Armazenamento seguro das credenciais em um arquivo `.json`
- Configuração do acesso ao S3 no Databricks usando `spark.conf`

<br></br>

### 2. 🥉 Catálogo da Camada Bronze | [01-Catalogo de Dados no Metastore do Databricks Bronze📎](notebooks/01-Catalogo%20de%20Dados%20no%20Metastore%20do%20Databricks%20Bronze.ipynb)  

Esta etapa teve como objetivo principal visualizar e documentar todas as colunas disponíveis nos arquivos brutos, a fim de entender a estrutura dos dados recebidos e decidir quais colunas seriam aproveitadas nas próximas etapas do pipeline.

- Criação do catálogo no Metastore do Databricks com os dados brutos (raw) armazenados na camada Bronze
- Registro do nome dos campos, tipo e respectivas descrições de cada um deles
- Adição de atributos informativos: valores mínimos e máximos, total de registros, registros nulos e registros distintos
- Detalhamento da fonte dos dados: link da origem, nome do arquivo original e nome utilizado no Databricks/S3

<br></br>

### 3. 📙 Limpeza e Tratamento dos Dados


#### 3A. Ingestão Bronze - Todas as Partidas | [02A-Ingestao-Bronze-TodasPartidas📎](notebooks/02A-Ingestao-Bronze-TodasPartidas.ipynb)  
Este notebook realiza a leitura dos dados de partidas diretamente da camada Bronze, aplicando diversas transformações para prepará-los para a camada Silver.
- Leitura do arquivo `BrasilSerieA_2024_TodasPartidas.csv` na Bronze
- Remoção de colunas irrelevantes (ex: colunas com odds de apostas)
- Filtro para considerar apenas partidas da temporada **2024**
- Remoção de linhas duplicadas e com dados ausentes críticos
- Criação das colunas:
  - `Match_ID`: identificador único da partida
  - `Trimestre`: com base na data da partida
  - `Turno`: para indicar se é 1º ou 2º turno do campeonato
- Salvamento do DataFrame tratado na **camada Silver** no formato Parquet


#### 3B. Ingestão Bronze - Classificação Final | [02B-Ingestao-Bronze-Classificacao📎](notebooks/02B-Ingestao-Bronze-Classificacao.ipynb)

Este notebook realiza o tratamento da tabela de classificação dos clubes, transformando os dados brutos da Bronze em um formato estruturado para análise na camada Silver.

- Leitura do arquivo `BrasilSerieA_2024_ClassificacaoFinal.csv` na Bronze
- Renomeação de colunas para padronização
- Criação de um dicionário de clubes, para padronizar os nomes
- Salvamento do DataFrame tratado na **camada Silver** no formato Parquet


#### 3C. Ingestão Bronze - Estatísticas por Jogador e Partida | [02C-Ingestao-Bronze-EstatisticaJogadorPorPartida📎](notebooks/02C-Ingestao-Bronze-EstatisticaJogadorPorPartida.ipynb)

Este notebook trata os dados estatísticos dos jogadores por partida, realizando ajustes essenciais antes de armazená-los na camada Silver.

- Leitura do arquivo `BrasilSerieA_2024_EstatisticaJogadorPorPartida.csv` na Bronze
- Remoção de colunas irrelevantes ou redundantes
- Renomeação de colunas para padronização
- Criação de um dicionário de clubes, para padronizar os nomes
- Correção no Tipo de Dado de algumas colunas
- ⚠️ Ajustes na base de dados por falta de informação
- Salvamento do DataFrame tratado na **camada Silver** no formato Parquet


#### 3D. Correção de Datas e Partidas Ausentes | [02D-Correcao-Datas-e-Partidas-Ausentes📎](notebooks/02D-Correcao-Datas-e-Partidas-Ausentes.ipynb)

Este notebook realiza correções importantes nos dados da tabela de estatísticas dos jogadores, garantindo integridade e consistência antes de seguirmos para análises na camada Silver.

- Identificação de divergências entre datas de partidas nos arquivos de estatísticas e todas as partidas
- Encontrado um padrão, onde as datas diferentes das partidas são de -1 dia
- Correção das datas incorretas com base no dataset validado (fbref.com)
- Salvamento do DataFrame final corrigido na camada Silver no formato Parquet

<br></br>

### 4. 🗂️ Catálogo da Camada Silver | [03-Catalogo de Dados no Metastore do Databricks Silver📎](notebooks/03-Catalogo%de%Dados.ipynb)

Este notebook é responsável por registrar no Metastore as tabelas já tratadas da camada Silver, possibilitando o consumo via SQL e outras ferramentas.

- Criação do Database específico para os dados tratados (camada Silver)
- Conversão dos arquivos tratados de .parquet para o formato Delta
- Registro das tabelas da camada Silver no Metastore com o caminho no S3
- Tabelas disponíveis para consulta direta com spark.sql("SELECT * FROM ...")


### 5. 🥇 Transformação para a Camada Gold - Mart de Clubes | 04A-Transformacao-Gold-Mart-Clubes

Nessa etapa damos início à construção do **Data Warehouse** do projeto, criando nossas tabelas da **camada Gold**.
O foco aqui é consolidar os dados tratados na camada Silver em uma estrutura otimizada para consumo analítico, utilizando métricas agregadas, normalizações e criação de scores personalizados.

Principais etapas desenvolvidas:
...

