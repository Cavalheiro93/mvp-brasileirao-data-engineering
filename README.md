# ⚽ MVP - Engenharia de Dados com Databricks, AWS e Redshift

Este projeto foi desenvolvido como parte da Pós-Graduação em Data Science e Analytics da PUC-Rio.  
O objetivo é construir um pipeline completo de Engenharia de Dados usando ferramentas do mercado, com foco em dados reais do Campeonato Brasileiro Série A 2024.

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



## 📒 Notebooks do Projeto

| Ordem | Nome do Notebook        | Descrição                                          |
|-------|--------------------------|-----------------------------------------------------|
| 00    | `00-Configuracao.ipynb`  | Leitura das credenciais, configuração do S3 e testes de conexão |


## 📌 Observações

> 🔐 **Credenciais AWS** não estão incluídas no repositório por segurança.  
> O arquivo `aws_credentials.json` foi usado localmente para leitura via Spark no notebook `00-Configuracao`.

---
