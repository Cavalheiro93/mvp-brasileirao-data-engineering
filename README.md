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
|
├── 📁 notebooks/           → Notebooks com cada etapa do pipeline de dados
|
├── 📁 images/              → Diagramas, capturas de tela e elementos visuais do projeto
│   ├── 📂 icons/           → Icones personalizados para o README do GitHub
│   ├── 📂 AWS-S3/          → Imagens referente ao S3, para auxiliar no entendimento do processo
|
├── README.md              → Documentação geral do projeto
└── placeholder.txt        → Arquivo temporário para inicializar o repositório
```


<br></br>
## 📒 Notebooks do Projeto

| Ordem | Nome do Notebook                                | Descrição                                                                                      | Link |
|-------|------------------------------------------       |------------------------------------------------------------------------------------------------|------|
| 00    | `00-Configuracao.ipynb`                                 | Leitura das credenciais, configuração do S3 e testes de conexão                                | [🔗](notebooks/00-Configuracao.ipynb) |
| 01    | `01-Catalogo-Bronze.ipynb`                              | Criação do catálogo no Metastore com os dados brutos da camada Bronze                          | [🔗](notebooks/01-Catalogo%20de%20Dados%20no%20Metastore%20do%20Databricks%20Bronze.ipynb) |
| 02A   | `02A-Ingestao-Bronze-TodasPartidas.ipynb`               | Leitura e tratamento inicial do arquivo de partidas (ajuste de datas, nomes e tipos)           | [🔗](notebooks/02A-Ingestao-Bronze-TodasPartidas.ipynb) |
| 02C   | `02B-Ingestao-Bronze-Classificacao.ipynb`               | Leitura da classificação final dos clubes, ajustes e criação da tabela na camada Silver        | [🔗](notebooks/02B-Ingestao-Bronze-Classificacao.ipynb) |
| 02B   | `02C-Ingestao-Bronze-EstatisticaJogadorPorPartida.ipynb`| Leitura e tratamento das estatísticas por jogador; limpeza e padronização de colunas           | [🔗](notebooks/02C-Ingestao-Bronze-EstatisticaJogadorPorPartida.ipynb) |
| 02D   | `02D-Correcao-Datas-e-Partidas-Ausentes.ipynb`          | Correções manuais em datas e partidas ausentes nas estatísticas do campeonato                  | [🔗](notebooks/02D-Correcao-Datas-e-Partidas-Ausentes.ipynb) |
| 03    | `03-Catalogo-Silver.ipynb`                              | Registro das tabelas tratadas da camada Silver no Metastore (com caminho S3)                   | [🔗](notebooks/03-Catalogo-Silver.ipynb) |
| 04A   | `04A-Transformacao-Gold-Mart-Clubes.ipynb`              | Agregações, normalizações e criação do Mart com desempenho dos clubes na camada Gold           | [🔗](notebooks/04A-Transformacao-Gold-Mart-Clubes.ipynb) |
| 04B   | `04B-Transformacao-Gold-Mart-Jogadores.ipynb`           | Criação do Mart com desempenho individual dos jogadores e informações complementares           | [🔗](notebooks/04B-Transformacao-Gold-Mart-Jogadores.ipynb) |
| 05    | `05-Carga-Redshift.ipynb`                               | Criação do Namespace e Workgroup no Redshift Serverless, além da carga das tabelas Gold        | [🔗](notebooks/05-Carga-Redshift.ipynb) |



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

#### 1.A - Configuração das chaves de acesso | [00-Configuracao 📎](notebooks/00-Configuracao.ipynb)
- Criação da conta na AWS
- Criação do bucket S3 com as camadas bronze, silver e gold
- Criação de credenciais IAM e geração das chaves de acesso
- Armazenamento seguro das credenciais em um arquivo `.json`
- Configuração do acesso ao S3 no Databricks usando `spark.conf`


<br></br>


## 2. Extração dos Dados e Catalogação Inicial da camada Bronze 📦🥉
Nesta etapa, realizamos a extração dos dados brutos (raw) da fonte original (`Kaggle`), com armazenamento direto na camada Bronze do nosso Data Lake no S3 (`mvp-brasileirao-2024`).
<br>
Em seguida, iniciamos o processo de catalogação para entender a estrutura dos dados recebidos e decidir quais colunas seriam aproveitadas nas próximas etapas do pipeline.

#### 2.A - Extração dos Dados e armazenamento no AWS S3 |  [Arquivos da camada Bronze](https://github.com/Cavalheiro93/mvp-brasileirao-data-engineering/tree/main/data/bronze)
![Visualização da Camada Bronze no S3](images/AWS-S3/bucket-s3-camada-bronze-arquivos-raw.jpg)

#### 2.B  Catálogo da Camada Bronze | [01-Catalogo de Dados no Metastore do Databricks Bronze📎](notebooks/01-Catalogo%20de%20Dados%20no%20Metastore%20do%20Databricks%20Bronze.ipynb)  
- Criação do catálogo no Metastore do Databricks com os dados brutos (raw) armazenados na camada Bronze
- Registro do nome dos campos, tipo e respectivas descrições de cada um deles
- Adição de atributos informativos: valores mínimos e máximos, total de registros, registros nulos e registros distintos
- Detalhamento da fonte dos dados: link da origem, nome do arquivo original e nome utilizado no Databricks/S3


<br></br>


## 3. Processamento da Camada Silver: Limpeza, Transformação e Catalogação dos Dados 🧹🥈
Após a extração e armazenamento dos dados brutos na camada Bronze, esta etapa é dedicada à preparação dos dados para consumo analítico.
<br>
Realizamos a limpeza, padronização e transformação de cada uma das tabelas originais, tratando problemas como colunas irrelevantes, formatações inconsistentes e divergências nas datas das partidas.
<br>
Ao final do processo, os dados tratados são armazenados na camada Silver do Data Lake no formato Delta e Parquet e, em seguida, registrados no Metastore do Databricks com o caminho S3, possibilitando consultas diretas via SQL.


#### 3.A - Dados Transformados e armazenados em Parquet e Delta na camada Silver no AWS S3 | [Arquivos da camada Silver](https://github.com/Cavalheiro93/mvp-brasileirao-data-engineering/tree/main/data/silver)
![Visualização da Camada Bronze no S3](images/AWS-S3/bucket-s3-camada-silver-pastas-parquet-delta.jpg)


#### 3B. Ingestão Bronze - Todas as Partidas | [02A-Ingestao-Bronze-TodasPartidas📎](notebooks/02A-Ingestao-Bronze-TodasPartidas.ipynb)  
- Leitura do arquivo `BrasilSerieA_2024_TodasPartidas.csv` na Bronze
- Remoção de colunas irrelevantes (ex: colunas com odds de apostas)
- Filtragem da temporada **2024** apenas.
- Remoção de linhas duplicadas e com dados ausentes críticos
- Criação das colunas:
  - `Match_ID`: identificador único da partida
  - `Trimestre`: com base na data da partida
  - `Turno`: para indicar se é 1º ou 2º turno do campeonato
- Armazenamento do DataFrame tratado na **camada Silver** no formato Parquet


#### 3C. Ingestão Bronze - Classificação Final | [02B-Ingestao-Bronze-Classificacao📎](notebooks/02B-Ingestao-Bronze-Classificacao.ipynb)
- Leitura do arquivo `BrasilSerieA_2024_ClassificacaoFinal.csv` na Bronze
- Renomeação de colunas para padronização
- Criação de um dicionário de clubes, para padronizar os nomes
- Armazenamento do DataFrame tratado na **camada Silver** no formato Parquet


#### 3D. Ingestão Bronze - Estatísticas por Jogador e Partida | [02C-Ingestao-Bronze-EstatisticaJogadorPorPartida📎](notebooks/02C-Ingestao-Bronze-EstatisticaJogadorPorPartida.ipynb)
- Leitura do arquivo `BrasilSerieA_2024_EstatisticaJogadorPorPartida.csv` na Bronze
- Remoção de colunas irrelevantes ou redundantes
- Renomeação de colunas para padronização
- Criação de um dicionário de clubes, para padronizar os nomes
- Correção no Tipo de Dado de algumas colunas
- ⚠️ Ajuste na base de dados por falta de informação
- Armazenamento do DataFrame tratado na **camada Silver** no formato Parquet


#### 3E. Correção de Datas e Partidas Ausentes | [02D-Correcao-Datas-e-Partidas-Ausentes📎](notebooks/02D-Correcao-Datas-e-Partidas-Ausentes.ipynb)
- Identificação de divergências entre datas de partidas nos arquivos de estatísticas e todas as partidas
- Identificação do padrão onde as datas diferentes das partidas são de -1 dia
- Correção das datas incorretas com base no dataset validado (fbref.com)
- Armazenamento do DataFrame final corrigido na camada Silver no formato Parquet


#### 3F. Catálogo da Camada Silver | [03-Catalogo de Dados no Metastore do Databricks Silver📎](notebooks/03-Catalogo%20de%20Dados%20no%20Metastore%20do%20Databricks%20Silver.ipynb)
- Criação do Database específico para os dados tratados (camada Silver)
- Conversão dos arquivos tratados de .parquet para o formato Delta
- Registro das tabelas da camada Silver no Metastore com o caminho no S3


<br></br>


## 4. Transformações Analíticas para Camada Gold e Catálogo de Dados 🧪🥇
Nesta etapa, consolidamos os dados tratados da camada Silver e criamos as primeiras tabelas analíticas do nosso **Data Warehouse**.

A Camada Gold será responsável por organizar os dados de forma otimizada para consumo analítico, através de **Data Marts** modelados por clube e por jogador, além de outras tabelas complementares.

O Catálogo de dados da Camada Gold foram registrados no **Metastore do Databricks** com caminho no S3, permitindo consultas SQL e integração com ferramentas analíticas.
![Visualização da Camada Bronze no S3](images/AWS-S3/bucket-s3-camada-gold-pastas-arquivos-finais.jpg)

#### 4A. Transformação por Clube | [04A-Transformacao-Gold-Mart-Clubes](notebooks/04A-Transformacao-Gold-Mart-Clubes.ipynb)  
- Leitura dos Arquivos no DBFS (Para diminuir o custo da AWS S3)
- Criação de Views Temporárias
- Criação de métricas de normalização
- Criando pesos para cada uma dessas métricas
- Criando um Score beseado na soma dessas métricas de normalização, para o resultado final do desempenho do clube


#### 4B. Transformação por Jogador | [04B-Transformacao-Gold-Mart-Jogadores](notebooks/04B-Transformacao-Gold-Mart-Jogadores.ipynb)
- Leitura dos Arquivos no DBFS (Para diminuir o custo da AWS S3)
- Criação de Views Temporárias
- Criação de métricas de normalização
- Criando pesos para cada uma dessas métricas
- Criando um Score beseado na soma dessas métricas de normalização, para o resultado final do desempenho do Jogador


#### 4C. Informações Complementares dos Jogadores | [04C-Transformacao-Gold-Mart_Info_Jogadores.ipynb](notebooks/04C-Transformacao-Gold-Mart_Info_Jogadores.ipynb)
- Leitura dos Arquivos no DBFS (Para diminuir o custo da AWS S3)
- Mapeamento das posições dos Jogadores em cada partida
- Ranqueamento das duas posições mais jogadas por cada jogador
- Criação de colunas de Posição Principal e Improvisação

#### 4D. Catálogo da Camada Gold | [05-Catalogo de Dados no Metastore do Databricks Gold](notebooks/05-Catalogo%20de%20Dados%20no%20Metastore%20do%20Databricks%20Gold.ipynb)
- Criação do Database específico para os dados tratados (camada Gold)
- Registro das tabelas da camada Gold no Metastore com o caminho no S3
- Adição de atributos informativos: valores mínimos e máximos, total de registros, registros nulos e registros distintos
- Detalhamento da fonte dos dados: link da origem, nome do arquivo original e nome utilizado no Databricks/S3
