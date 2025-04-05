# ⚽ MVP - Engenharia de Dados com Databricks, AWS e Redshift

Este projeto foi desenvolvido como parte da Pós-Graduação em Data Science e Analytics da PUC-Rio.  
O objetivo é construir um pipeline completo de Engenharia de Dados usando ferramentas do mercado, com foco em dados reais do Campeonato Brasileiro Série A 2024.

---
## 📚 Sumário

- [🚀 Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [📁 Estrutura de Pastas](#-estrutura-de-pastas)
- [📒 Notebooks do Projeto](#-notebooks-do-projeto)
- [📌 Observações](#-observações)
- [00 - Configuração Inicial](notebooks/00-Configuracao.ipynb)
- <img src="images/notebook.png" width="18"/> [00 - Configuração Inicial](notebooks/00-Configuracao.ipynb)

---

## 🚀 Tecnologias Utilizadas

- **Databricks Community Edition**: para ingestão, transformação e tratamento dos dados com PySpark (ETL)
- **AWS S3**: armazenamento em Data Lake (Bronze, Silver, Gold)
- **Amazon Redshift Serverless**: utilizado como Data Warehouse para armazenar a camada Gold do projeto, estruturada em Data Marts temáticos focados em clubes, jogadores e métricas estatísticas.
- **PySpark + SQL**: para manipulação, modelagem e criação de data marts
- **GitHub**: versionamento e documentação do projeto

---

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

---

## 📒 Notebooks do Projeto

| Ordem | Nome do Notebook         | Descrição                                                       | Link
|-------|--------------------------|-----------------------------------------------------------------|-------|
| 00    | `00-Configuracao.ipynb`  | Leitura das credenciais, configuração do S3 e testes de conexão |[🔗](notebooks/00-Configuracao.ipynb)


### 📌 Observações
> 🔐 **Credenciais AWS** não estão incluídas no repositório por segurança.  
> O arquivo `aws_credentials.json` foi usado localmente para leitura via Spark no notebook `00-Configuracao`.

---

## 🧭 Etapas do Projeto
O projeto foi construído de forma sequencial, seguindo boas práticas de engenharia de dados. Abaixo, descrevemos cada uma das etapas realizadas:


### 1. 📘 Configuração Inicial | [00-Configuracao📎](notebooks/00-Configuracao.ipynb)
- Criação da conta na AWS
- Criação do bucket S3 com as camadas bronze, silver e gold
- Criação de credenciais IAM e geração das chaves de acesso
- Armazenamento seguro das credenciais em um arquivo `.json`
- Configuração do acesso ao S3 no Databricks usando `spark.conf`

### 2. 📗 Catálogo da Camada Bronze | [01-Catalogo de Dados📎](notebooks/01-Catalogo%20de%20Dados%20no%20Metastore%20do%20Databricks%20Bronze.ipynb)  
Esta etapa teve como objetivo principal visualizar e documentar todas as colunas disponíveis nos arquivos brutos, a fim de entender a estrutura dos dados recebidos e decidir quais colunas seriam aproveitadas nas próximas etapas do pipeline.

- Criação do catálogo no Metastore do Databricks com os dados brutos (raw) armazenados na camada Bronze
- Registro do nome dos campos, tipo e respectivas descrições de cada um deles
- Adição de atributos informativos: valores mínimos e máximos, total de registros, registros nulos e registros distintos
- Detalhamento da fonte dos dados: link da origem, nome do arquivo original e nome utilizado no Databricks/S3


