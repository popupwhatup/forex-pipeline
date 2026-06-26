from airflow.sdk import dag, task, get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.utils.trigger_rule import TriggerRule

import os
import random
import pendulum
import psycopg2

def get_postgres_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

def get_run_date():
    context = get_current_context()

    dt = context.get("data_interval_start")

    if dt is None:
        dag_run = context.get("dag_run")
        dt = context.get("logical_date") or dag_run.run_after
    
    return dt.date()


@dag(
    dag_id="trade_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 20, tz="UTC"),
    catchup=True,
)

def trade_pipeline():
    #1
    @task
    def create_tables():
        conn = None
        cursor = None

        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    trade_date DATE NOT NULL,
                    instrument TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    profit NUMERIC NOT NULL    
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_summary (
                    summary_date DATE PRIMARY KEY,
                    total_profit NUMERIC NOT NULL,
                    trade_count INTEGER NOT NULL,
                    win_count INTEGER NOT NULL         
                );
            """)

            conn.commit()
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    #2
    @task
    def generate_trades():
        trade_date = get_run_date()
        trade_date_str = trade_date.isoformat()

        # deterministic seed : วันเดิม = seed เดิม = trade ชุดเดิม
        seed = int(trade_date_str.replace("-", ""))
        locked_seed_random = random.Random(seed)

        instruments = ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY"]
        directions = ["BUY", "SELL"]

        trade_count = locked_seed_random.randint(2, 5)

        trades = []

        for i in range(trade_count):
            trade = {
                "trade_id": f"{trade_date_str}-{i + 1:03d}", #ทำให้เป็นตัวเลขสามหลัก เช่น 001, 002, 003
                "trade_date": trade_date_str,
                "instrument": locked_seed_random.choice(instruments),
                "direction": locked_seed_random.choice(directions),
                "profit": locked_seed_random.randint(-500, 1500), #randint = random integer สุ่มตัวเลข
            }
            trades.append(trade)

        print(f"Generated trades for {trade_date_str}: {trades}")

        return trades
    
    #3
    @task.branch
    def check_market_day():
        trade_date = get_run_date()
        weekday = trade_date.weekday()

        #Python weekday(): Monday = 0, Sunday = 6
        if weekday >= 5:
            return "market_closed"
        return "load_to_postgres"
    
    #4
    @task
    def load_to_postgres(trades):
        if not trades:
            raise AirflowSkipException("No trades to load.")
        
        conn = None
        cursor = None

        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()

            for trade in trades:
                cursor.execute("""
                    INSERT INTO trades (
                        trade_id,
                        trade_date,
                        instrument,
                        direction,
                        profit           
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (trade_id)
                    DO UPDATE SET
                        trade_date = EXCLUDED.trade_date,
                        instrument = EXCLUDED.instrument,
                        direction = EXCLUDED.direction,
                        profit = EXCLUDED.profit;
                """, (
                    trade["trade_id"],
                    trade["trade_date"],
                    trade["instrument"],
                    trade["direction"],
                    trade["profit"],
                ))
            
            conn.commit()
            print(f"Loaded {len(trades)} trades to Postgres.")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        
    #5
    @task
    def market_closed():
        raise AirflowSkipException("Market is closed on weekends.")
    

    #6
    @task
    def daily_summary():
        summary_date = get_run_date().isoformat()

        conn = None
        cursor = None

        try:
            conn = get_postgres_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    trade_date,
                    SUM(profit) AS total_profit,
                    COUNT(*) AS trade_count,
                    COUNT(*) FILTER (WHERE profit > 0) AS win_count
                FROM trades
                WHERE trade_date = %s
                GROUP BY trade_date
            """, (summary_date,))

            row = cursor.fetchone()
            if row is None:
                raise AirflowSkipException(f"No trades found for {summary_date}.")
            
            trade_date, total_profit, trade_count, win_count = row

            cursor.execute("""
                INSERT INTO daily_summary (
                    summary_date,
                    total_profit,
                    trade_count,
                    win_count           
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (summary_date)
                DO UPDATE SET
                    total_profit = EXCLUDED.total_profit,
                    trade_count = EXCLUDED.trade_count,
                    win_count = EXCLUDED.win_count;
            """, (
                trade_date,
                total_profit,
                trade_count,
                win_count,
            ))

            conn.commit()

            result = {
                "summary_date": str(trade_date),
                "total_profit": float(total_profit),
                "trade_count": trade_count,
                "win_count": win_count,
            }
            print(f"Daily summary: {result}")

            return result
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    
    #7
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def notify():
        context = get_current_context()
        ti = context["ti"]

        summary = ti.xcom_pull(task_ids="daily_summary")

        if not summary:
            raise AirflowSkipException("No summary to notify.")


        print("===== DAILY TRADING SUMMARY =====")
        print(f"Date: {summary['summary_date']}")
        print(f"Total profit: {summary['total_profit']}")
        print(f"Trade count: {summary['trade_count']}")
        print(f"Win count: {summary['win_count']}")
        print("=================================")

    

    create_tables = create_tables()
    trades = generate_trades()
    branch = check_market_day()

    loaded = load_to_postgres(trades)
    closed = market_closed()
    summary = daily_summary()

    latest_only = LatestOnlyOperator(
        task_id="latest_only",
    )

    notified = notify()

    create_tables >> trades >> branch

    branch >> loaded >> summary >> latest_only >> notified
    branch >> closed


trade_pipeline()