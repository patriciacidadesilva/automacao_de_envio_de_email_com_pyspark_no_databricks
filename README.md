## 📧 Alertas Operacionais Automatizados com PySpark no Databricks

### 🎯 Visão Geral

Este projeto demonstra uma **automação de envio de e-mails operacionais**, construída com **PySpark no Databricks**, cujo objetivo é identificar **documentos pendentes de processamento** acima de um SLA definido e **notificar automaticamente os responsáveis**.

Todas as tabelas, colunas, domínios e regras de negócio utilizadas neste repositório são **fictícias** e existem **exclusivamente para fins de demonstração técnica e portfólio**.

> **Resumo executivo:**  
> dados distribuídos → regras de negócio → exceção → notificação automática

---

### 🧠 Contexto de Negócio (Fictício)

**Empresa:** Acme Corp  
**Área:** Operações Financeiras e Administrativas  

**Desafio:**  
Documentos operacionais pendentes por longos períodos geram gargalos, retrabalho e impactos em processos downstream.
Antes desta solução, o acompanhamento dessas pendências dependia de verificações manuais e comunicações reativas.

---

### 🚀 Solução

Um **pipeline híbrido**, com responsabilidades bem definidas:

- **PySpark** para processamento distribuído e aplicação das regras de negócio  
- **Python** para orquestração, geração de Excel e envio de e-mails  
- **Databricks** como plataforma de execução, segurança e agendamento  

A solução:
- identifica documentos pendentes acima do SLA
- resolve dinamicamente o e-mail do responsável
- gera um relatório estruturado em Excel
- envia notificações automáticas com evidência anexada

---

### 🏗️ Arquitetura (Alto Nível)

1. Leitura das tabelas fato e dimensão via Spark  
2. Normalização de usuários e datas  
3. Enriquecimento com informações de e-mail  
4. Aplicação de filtros e regras de exclusão  
5. Coleta controlada de dados para o driver  
6. Geração do arquivo Excel  
7. Envio de e-mail via SMTP autenticado  

---

### 🧩 Tecnologias Utilizadas

- Databricks  
- Apache Spark / PySpark  
- Python 3  
- Pandas  
- xlsxwriter  
- SMTP (Office 365)  
- Databricks Secrets  

---

### 📂 Fontes de Dados (Fictícias)

| Tipo | Tabela |
|---|---|
| Fato | `analytics.ops_core.fact_documents_backlog` |
| Dimensão | `analytics.ops_core.dim_users` |

---

### 🧱 Modelo de Dados (Simplificado)

#### Fato: `fact_documents_backlog`

| Coluna | Descrição |
|---|---|
| document_id | Identificador único do documento |
| document_number | Número do documento |
| document_key | Chave única |
| document_amount | Valor total |
| issue_date | Data de emissão |
| due_date | Data de vencimento |
| client_tax_id | Identificador do cliente |
| client_name | Nome do cliente |
| supplier_tax_id | Identificador do fornecedor |
| supplier_name | Nome do fornecedor |
| processing_status | Status de processamento |
| processing_days | Dias em pendência |
| document_link | Link de referência |
| document_category | Categoria do documento |
| resolution_type | Tipo de conclusão |
| responsible_area | Área responsável |
| request_owner | Dono do documento |
| task_name | Tarefa atual |
| processing_flag | Indicador de processamento |
| business_unit | Unidade de negócio |
| cost_center | Centro de custo |

---

#### Dimensão: `dim_users`

| Coluna | Descrição |
|---|---|
| username | Identificador do usuário |
| email | E-mail do usuário |

---

### 📐 Regras de Negócio (Fictícias)

- Considera apenas documentos:
  - com `processing_flag = 'Pending'`
  - com data de emissão válida
  - pendentes há mais de **15 dias**
  - pertencentes a um centro de custo específico (`D010`)
- Exclui tarefas técnicas ou automatizadas
- Resolução de e-mail:
  - prioritariamente via join com a dimensão de usuários
  - fallback por regras baseadas na área responsável

---

### 📊 Saídas (Output)

- **Relatório Excel (.xlsx)** com os documentos pendentes
- **E-mail automático** contendo:
  - assunto padronizado
  - corpo explicativo orientado à ação
  - relatório anexado

---

### 🔐 Segurança e Governança

- Credenciais armazenadas via **Databricks Secrets**
- Remetente alinhado ao usuário autenticado (anti-spoofing)
- Limite explícito de linhas antes do `toPandas()` para proteção do driver
- Separação clara entre processamento distribuído e execução local

---

### ⚙️ Configuração

Parâmetros ajustáveis no código:

```python
MAX_LINHAS_EXCEL = 2000
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
``` 

---

### Secrets necessários no Databricks:

- USER
- PASSWORD

---

### 🕒 Execução

O notebook pode ser:
- executado manualmente para testes
- agendado como Databricks Job
- integrado a um fluxo maior de monitoramento operacional

---

### 📈 Benefícios

- Monitoramento proativo de pendências
- Redução de trabalho manual
- Comunicação padronizada e auditável
- Escalabilidade com Spark
- Código limpo, governável e pronto para produção

---

### 📁 Estrutura do Repositório

```text
.
├── notebooks/
│   └── alertas_operacionais_pyspark_databricks.ipynb
├── README.md
└── .gitignore
```

---

### 👩‍💻 Autoria

Projeto desenvolvido com foco em **Engenharia de Dados**, **Analytics Engineering** e **DataOps**, utilizando PySpark no Databricks como base tecnológica.
Este projeto utiliza nomes, tabelas e regras de negócio fictícias.
Não representa sistemas ou dados reais de nenhuma organização.

---

### 🧭 Próximos Passos (Roadmap)

- Parametrização do SLA e centro de custo
- Geração do relatório em storage (em vez de anexo)
- Integração com ferramentas de alerta (Teams / Slack)
- Observabilidade e logs estruturados
- Transformação em Databricks Job com SLA e alertas de falha
