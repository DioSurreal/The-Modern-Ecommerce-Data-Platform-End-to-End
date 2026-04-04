import logging
import os
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.sql import SqlSensor
from airflow.utils.email import send_email
from airflow.hooks.base import BaseHook

try:
    from airflow.providers.snowflake.sensors.snowflake import SnowflakeSensor
    HAS_SNOWFLAKE_PROVIDER = True
except ModuleNotFoundError:
    SnowflakeSensor = None
    HAS_SNOWFLAKE_PROVIDER = False

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
EVENT_MAKER_DIR = "/opt/airflow/event_maker"

S3_BUCKET = os.getenv("F3_S3_BUCKET") or os.getenv("AWS_S3_BUCKET") or "my-ecommerce-project-landing-zone"
S3_PREFIX = (os.getenv("F3_S3_PREFIX") or "landing").strip("/")
AWS_CONN_ID = "aws_default" 

MY_EMAIL = os.getenv("MY_EMAIL", "").strip()
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_MAIL_FROM = os.getenv("SMTP_MAIL_FROM", "").strip()
ALERT_EMAILS = [MY_EMAIL] if (MY_EMAIL and SMTP_HOST and SMTP_MAIL_FROM) else []

THAI_DATE = "{{ logical_date.in_timezone('Asia/Bangkok').strftime('%Y-%m-%d') }}"
THAI_HOUR = "{{ logical_date.in_timezone('Asia/Bangkok').strftime('%H') }}"


def _q(value: str) -> str:
    return value.replace("'", "''")


def prepare_dbt_profiles():
    conn = BaseHook.get_connection("snowflake_default")
    extra = conn.extra_dejson or {}

    account = conn.host or extra.get("account")
    user = conn.login
    password = conn.password
    warehouse = extra.get("warehouse")
    role = extra.get("role")
    database = extra.get("database")
    schema = extra.get("schema") or conn.schema

    missing = [
        key
        for key, val in {
            "account": account,
            "user": user,
            "password": password,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
        }.items()
        if not val
    ]
    if missing:
        raise ValueError(
            f"snowflake_default is missing required fields for dbt profile: {', '.join(missing)}"
        )

    role_line = f"\n      role: '{_q(str(role))}'" if role else ""
    profile_text = (
        "omni_ecommerce_dbt:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        f"      account: '{_q(str(account))}'\n"
        f"      user: '{_q(str(user))}'\n"
        f"      password: '{_q(str(password))}'\n"
        f"      role: '{_q(str(role))}'\n" if role else
        "omni_ecommerce_dbt:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: snowflake\n"
        f"      account: '{_q(str(account))}'\n"
        f"      user: '{_q(str(user))}'\n"
        f"      password: '{_q(str(password))}'\n"
    )

    profile_text += (
        f"      warehouse: '{_q(str(warehouse))}'\n"
        f"      database: '{_q(str(database))}'\n"
        f"      schema: '{_q(str(schema))}'\n"
        "      threads: 4\n"
    )

    profiles_path = Path(DBT_PROJECT_DIR) / "profiles.yml"
    profiles_path.write_text(profile_text, encoding="utf-8")


def send_failure_email(context):
    if not ALERT_EMAILS:
        return

    task_instance = context["task_instance"]
    dag_id = context["dag"].dag_id
    task_id = task_instance.task_id
    log_url = task_instance.log_url

    subject = f"Airflow Alert: {dag_id}.{task_id} failed"
    html_content = (
        f"<p>Task <b>{task_id}</b> in DAG <b>{dag_id}</b> failed.</p>"
        f"<p>Execution date: {context.get('ds')}</p>"
        f"<p><a href='{log_url}'>Open task log</a></p>"
    )
    try:
        send_email(to=ALERT_EMAILS, subject=subject, html_content=html_content)
    except Exception as exc:
        logging.exception("Skipping failure email because SMTP is not ready: %s", exc)


default_args = {
    "owner": "senior_de",
    "depends_on_past": False,
    "email_on_failure": bool(ALERT_EMAILS),
    "email": ALERT_EMAILS,
    "on_failure_callback": send_failure_email,
    "retries": 0,
}

with DAG(
    dag_id="f3_robust_logistics_pipeline_v3",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2026, 4, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["production", "logistics", "monitoring"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_and_upload_to_s3",
        bash_command=(
            "export AWS_ACCESS_KEY_ID=${F3_AWS_ACCESS_KEY_ID:-$AWS_ACCESS_KEY_ID}; "
            "export AWS_SECRET_ACCESS_KEY=${F3_AWS_SECRET_ACCESS_KEY:-$AWS_SECRET_ACCESS_KEY}; "
            "export AWS_REGION=${F3_AWS_REGION:-${AWS_REGION:-ap-southeast-1}}; "
            "export AWS_DEFAULT_REGION=$AWS_REGION; "
            f"python {EVENT_MAKER_DIR}/generate_hourly_data_f3.py "
            f"--date {THAI_DATE} --hour {THAI_HOUR} --upload-s3"
        ),
    )

    wait_for_order_items = S3KeySensor(
        task_id="wait_for_order_items_file",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}/{THAI_DATE}/{THAI_HOUR}/order_items{THAI_HOUR}.csv",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=10,
        timeout=120,
        mode="reschedule",
    )

    wait_for_inventory_items = S3KeySensor(
        task_id="wait_for_inventory_items_file",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}/{THAI_DATE}/{THAI_HOUR}/inventory_items{THAI_HOUR}.csv",
        aws_conn_id=AWS_CONN_ID,
        poke_interval=10,
        timeout=120,
        mode="reschedule",
    )

    snowpipe_sql = """
        SELECT COUNT(*)
        FROM TABLE(ECOMMERCE_RAW.information_schema.copy_history(
           table_name=>'ECOMMERCE_RAW.BRONZE.RAW_ORDER_ITEMS',
           start_time=> DATEADD(hours, -24, CURRENT_TIMESTAMP())
        ))
        WHERE file_name ILIKE '%order_items23.csv'
          AND status ILIKE 'LOADED';
    """

    if HAS_SNOWFLAKE_PROVIDER:
        wait_for_snowpipe = SnowflakeSensor(
            task_id="wait_for_snowpipe_ingestion",
            snowflake_conn_id="snowflake_default",
            sql=snowpipe_sql,
            poke_interval=30,
            timeout=1200,
            mode="reschedule",
        )
    else:
        wait_for_snowpipe = SqlSensor(
            task_id="wait_for_snowpipe_ingestion",
            conn_id="snowflake_default",
            sql=snowpipe_sql,
            poke_interval=30,
            timeout=1200,
            mode="reschedule",
        )

    prepare_dbt_profile = PythonOperator(
        task_id="prepare_dbt_profile",
        python_callable=prepare_dbt_profiles,
    )

    run_dbt = BashOperator(
        task_id="dbt_run_logistics",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export PATH=\"$HOME/.local/bin:$PATH\" && "
            "dbt --version >/dev/null 2>&1 && "
            "dbt run --select +fct_logistics_optimization "
            f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} "
            f"--vars '{{\"run_date\": \"{THAI_DATE}\"}}'"
        ),
    )

    test_dbt = BashOperator(
        task_id="dbt_test_logistics",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "export PATH=\"$HOME/.local/bin:$PATH\" && "
            "dbt test --select +fct_logistics_optimization "
            f"--profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} "
            ""
        ),
    )

    generate_data >> [wait_for_order_items, wait_for_inventory_items] >> wait_for_snowpipe >> prepare_dbt_profile >> run_dbt >> test_dbt
