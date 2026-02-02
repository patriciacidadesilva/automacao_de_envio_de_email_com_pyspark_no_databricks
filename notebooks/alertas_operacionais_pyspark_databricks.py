# Databricks notebook source
# MAGIC %md
# MAGIC ## 📧 Alertas Operacionais Automatizados com PySpark no Databricks
# MAGIC
# MAGIC **Objetivo (demo/fictício):** identificar documentos pendentes acima do SLA e notificar responsáveis via e-mail, anexando evidência em Excel.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 0) Setup e Dependências

# COMMAND ----------

from datetime import datetime
import os
import re

import pandas as pd
from pyspark.sql import SparkSession, functions as F

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# Dependência para escrita do Excel
try:
    import xlsxwriter  # noqa: F401
except ModuleNotFoundError:
    # Databricks: instala no cluster/notebook atual
    get_ipython().run_line_magic("pip", "install xlsxwriter")
    import xlsxwriter  # noqa: F401

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1) Parâmetros e Configuração (Fictícios)

# COMMAND ----------

# 1.1) Fonte de dados (fictícia)
CATALOG_FACT = "analytics"
SCHEMA_FACT  = "ops_core"
TABLE_FACT   = "fact_documents_backlog"

CATALOG_DIM  = "analytics"
SCHEMA_DIM   = "ops_core"
TABLE_DIM    = "dim_users"

# 1.2) Regras de negócio (fictícias)
SLA_DIAS = 15
COST_CENTER_FILTRO = "D010"

# 1.3) Segurança operacional (protege o driver ao coletar para Pandas)
MAX_LINHAS_EXCEL = 2000

# 1.4) Config SMTP (ajustável por variável de ambiente)
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
SMTP_DEBUG  = False  # True para debug do handshake

# 1.5) Destinatários (ajustáveis por variável de ambiente)
EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "alerts@example.com")
EMAIL_CC = os.getenv("ALERT_EMAIL_CC", "ops@example.com")

# 1.6) Função utilitária para ler credencial via Secrets ou env
def get_secret_or_env(secret_scope: str, secret_key: str, env_key: str) -> str:
    """
    Recupera credencial via Databricks Secrets ou variável de ambiente.
    """
    try:
        return dbutils.secrets.get(scope=secret_scope, key=secret_key)  # type: ignore[name-defined]
    except Exception:
        v = os.getenv(env_key)
        if not v:
            raise ValueError(
                f"Credencial ausente: configure Databricks Secret ({secret_scope}:{secret_key}) "
                f"ou variável de ambiente ({env_key})."
            )
        return v

# 1.7) Credenciais SMTP (não ficam hardcoded no código)
SMTP_USER = get_secret_or_env("SVC_EMAIL", "USER", "SMTP_USER")
SMTP_PASSWORD = get_secret_or_env("SVC_EMAIL", "PASSWORD", "SMTP_PASSWORD")

# 1.8) Anti-spoof (remetente igual ao usuário autenticado no SMTP)
EMAIL_FROM = SMTP_USER

# 1.9) Data para nome do arquivo
DATA_HOJE = datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2) Sessão Spark

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3) Leitura e Normalização (PySpark)
# MAGIC
# MAGIC Etapas:
# MAGIC - Lê tabela fato e dimensão
# MAGIC - Normaliza chaves (upper/trim)
# MAGIC - Converte datas para DATE

# COMMAND ----------

# 3.1) Leitura das tabelas (Spark)
df_fact_raw = spark.table(f"{CATALOG_FACT}.{SCHEMA_FACT}.{TABLE_FACT}")
df_users_raw = spark.table(f"{CATALOG_DIM}.{SCHEMA_DIM}.{TABLE_DIM}")

print(f">>> RAW fact : {df_fact_raw.count()}")
print(f">>> RAW users: {df_users_raw.count()}")

# 3.2) Normalização da fato (ex.: dono do documento e data de emissão)
df_fact = (
    df_fact_raw
    .withColumn("request_owner_norm", F.upper(F.trim(F.col("request_owner").cast("string"))))
    .withColumn("issue_date_dt", F.to_date(F.col("issue_date")))
)

# 3.3) Normalização da dimensão (ex.: username e e-mail)
df_users = (
    df_users_raw
    .withColumn("username_norm", F.upper(F.trim(F.col("username").cast("string"))))
    .select(
        "username_norm",
        F.col("email").alias("user_email")
    )
)

# 3.4) Join para resolver e-mail do dono do documento
df_base = (
    df_fact.alias("f")
    .join(
        df_users.alias("u"),
        F.col("f.request_owner_norm") == F.col("u.username_norm"),
        "left"
    )
    .withColumn("notification_email", F.col("u.user_email"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4) Regras de Negócio e Filtros (PySpark)
# MAGIC
# MAGIC Etapas:
# MAGIC - Aplica fallback de e-mail quando não encontrado na dimensão
# MAGIC - Filtra por pendências acima do SLA e por centro de custo
# MAGIC - Exclui tarefas que não interessam no contexto do alerta

# COMMAND ----------

# 4.1) Tarefas a excluir (fictícias)
tarefas_excluir = [
    "Auto - Sync Metadata",
    "Auto - Retry Integration",
    "Auto - Generate Reference Doc",
    "End - Finalized"
]

# 4.2) Identifica e-mail vazio
email_vazio = (
    F.col("notification_email").isNull() |
    (F.length(F.trim(F.col("notification_email"))) == 0)
)

# 4.3) Normaliza área responsável (para fallback)
resp_norm = F.upper(F.trim(F.col("responsible_area").cast("string")))

# 4.4) Fallback de e-mail (quando não encontrou na dimensão)
df_base = (
    df_base
    .withColumn(
        "notification_email",
        F.when(~email_vazio, F.col("notification_email"))
         .when(resp_norm == F.lit("WAREHOUSE"), F.lit("warehouse.team@example.com"))
         .when(resp_norm == F.lit("CUSTOMER_SUPPORT"), F.lit("support.team@example.com"))
         .when(resp_norm.isin("FINANCE_OPS", "FINANCE"), F.lit("finance.ops@example.com"))
         .otherwise(F.lit(None).cast("string"))
    )
)

# 4.5) Filtro principal (SLA + centro + status)
df_filtrado = (
    df_base
    .where(~F.col("task_name").isin(tarefas_excluir))
    .where(F.col("processing_flag") == "Pending")
    .where(F.col("cost_center") == COST_CENTER_FILTRO)
    .where(F.col("issue_date_dt").isNotNull())
    .where(F.datediff(F.current_date(), F.col("issue_date_dt")) > SLA_DIAS)
)

qtd = df_filtrado.count()
print(f">>> REGISTROS FILTRADOS: {qtd}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5) Preparação do Excel (limit + toPandas)
# MAGIC
# MAGIC Etapas:
# MAGIC - Seleciona colunas na ordem desejada
# MAGIC - Limita volume para evitar sobrecarga no driver
# MAGIC - Converte para Pandas e escreve Excel em /tmp

# COMMAND ----------

if qtd == 0:
    print("Nenhuma pendência encontrada com as regras aplicadas. Nenhum e-mail enviado.")
else:
    # 5.1) Seleção e ordenação de colunas para o relatório
    df_excel = (
        df_filtrado.select(
            F.col("document_id"),
            F.col("document_number"),
            F.col("document_key"),
            F.col("document_amount"),
            F.col("issue_date_dt").alias("issue_date"),
            F.col("due_date"),
            F.col("client_tax_id"),
            F.col("client_name"),
            F.col("supplier_tax_id"),
            F.col("supplier_name"),
            F.col("processing_status"),
            F.col("processing_days"),
            F.col("document_link"),
            F.col("document_category"),
            F.col("resolution_type"),
            F.col("responsible_area"),
            F.col("request_owner"),
            F.col("task_name"),
            F.col("processing_flag"),
            F.col("business_unit"),
            F.col("cost_center"),
            F.col("notification_email"),
        )
        .limit(MAX_LINHAS_EXCEL)
    )

    # 5.2) Coleta para Pandas (driver)
    df_pandas = df_excel.toPandas()
    print(f">>> Linhas no Excel (limit {MAX_LINHAS_EXCEL}): {len(df_pandas)}")

    if df_pandas.empty:
        print("DataFrame final vazio após seleção. Nenhum e-mail enviado.")
    else:
        # 5.3) Renomeia colunas para layout do Excel
        column_mapping = {
            "document_id": "Document ID",
            "document_number": "Document Number",
            "document_key": "Document Key",
            "document_amount": "Amount",
            "issue_date": "Issue Date",
            "due_date": "Due Date",
            "client_tax_id": "Client Tax ID",
            "client_name": "Client Name",
            "supplier_tax_id": "Supplier Tax ID",
            "supplier_name": "Supplier Name",
            "processing_status": "Processing Status",
            "processing_days": "Days Pending",
            "document_link": "Document Link",
            "document_category": "Category",
            "resolution_type": "Resolution Type",
            "responsible_area": "Responsible Area",
            "request_owner": "Request Owner",
            "task_name": "Task",
            "processing_flag": "Processing Flag",
            "business_unit": "Business Unit",
            "cost_center": "Cost Center",
            "notification_email": "Notification Email",
        }
        df_pandas.rename(columns=column_mapping, inplace=True)

        # 5.4) Gera Excel em /tmp
        file_name = f"operational_alerts_{COST_CENTER_FILTRO}_{DATA_HOJE}.xlsx"
        file_path = f"/tmp/{file_name}"
        os.makedirs("/tmp", exist_ok=True)

        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            df_pandas.to_excel(writer, index=False, sheet_name="Pending Items")

        print(">>> anexo existe?", os.path.exists(file_path), "bytes:", os.path.getsize(file_path))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6) Corpo do E-mail

# COMMAND ----------

def sanitize_subject(text: str) -> str:
    """Remove quebras de linha do subject e normaliza espaços."""
    return re.sub(r"[\r\n]+", " ", text).strip()

ASSUNTO = sanitize_subject(
    f"[ACME] Ação necessária — Pendências > {SLA_DIAS} dias — Centro {COST_CENTER_FILTRO}"
)

CORPO_EMAIL = f"""
Prezados,

Identificamos documentos operacionais com pendência de processamento há mais de {SLA_DIAS} dias, vinculados ao centro {COST_CENTER_FILTRO}.
Para evitar impactos em prazos e rotinas de conciliação, solicitamos a priorização do tratamento.

Orientações:
• Verificar dependências que estejam bloqueando o processamento;
• Reencaminhar para a área correta, se aplicável;
• Atualizar o status dos itens assim que regularizados.

Segue relatório em anexo com a evidência consolidada.

Atenciosamente,
Acme Corp — Operações
""".strip()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 7) Envio de E-mail (SMTP)
# MAGIC
# MAGIC Etapas:
# MAGIC - Monta mensagem com anexo
# MAGIC - Autentica no SMTP e envia para TO/CC

# COMMAND ----------

if qtd == 0:
    print("Sem envio: nenhuma pendência.")
else:
    # 7.1) Monta mensagem
    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Cc"] = EMAIL_CC
    msg["Subject"] = ASSUNTO
    msg.attach(MIMEText(CORPO_EMAIL, "plain"))

    # 7.2) Anexa o Excel gerado
    with open(file_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={file_name}")
        msg.attach(part)

    # 7.3) Lista de destinatários (TO + CC)
    recipients = [EMAIL_TO] + ([EMAIL_CC] if EMAIL_CC else [])

    # 7.4) Envia via SMTP
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        if SMTP_DEBUG:
            server.set_debuglevel(1)

        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, recipients, msg.as_string())
        server.quit()

        print(f"✅ E-mail enviado! FROM={EMAIL_FROM} TO={EMAIL_TO} CC={EMAIL_CC}")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {str(e)}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ### 8) Observabilidade (Métricas)
# MAGIC
# MAGIC Saída simples para auditoria em jobs agendados.

# COMMAND ----------

if qtd == 0:
    metrics = {
        "status": "no_action",
        "filtered_records": 0,
        "cost_center": COST_CENTER_FILTRO,
        "sla_days": SLA_DIAS,
        "date": DATA_HOJE,
    }
else:
    metrics = {
        "status": "email_sent",
        "filtered_records": int(qtd),
        "excel_rows": int(len(df_pandas)),
        "cost_center": COST_CENTER_FILTRO,
        "sla_days": SLA_DIAS,
        "date": DATA_HOJE,
        "excel_path": file_path,
    }

print("METRICS:", metrics)
