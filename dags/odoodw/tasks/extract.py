from airflow.providers.postgres.hooks.postgres import PostgresHook


from psycopg2.extras import execute_values
from odoodw.utils.dw_setup import DWSetup


def extract_to_stage():
    print("Begin extraction")

    # Fetch stage load settings
    dw_setup = PostgresHook(postgres_conn_id="ODOODW_SETUP")
    dw_setup_conn = dw_setup.get_conn()

    odoo_src = PostgresHook(postgres_conn_id="ODOO_SOURCE")
    odoo_src_conn = odoo_src.get_conn()

    stage_load_setup = DWSetup(dw_setup_conn, odoo_src_conn).get()

    ## Setup done, close setup connection
    dw_setup_conn.close()

    dw_stage = PostgresHook(postgres_conn_id="ODOODW_STAGE")
    stage_conn = dw_stage.get_conn()
    stage_cur = stage_conn.cursor()

    for config in stage_load_setup:

        print(f"Begin processing table {config['src_table']}")
        # Run pre_execute_script

        # Create if not exists stage table
        stage_cur.execute(config["create_script"])
        stage_conn.commit()

        # Truncate staging table
        stage_cur.execute(config["pre_execute_script"])
        stage_conn.commit()

        # Copy to stage
        # TODO: How does this handle large tables? (Over 1M rows)

        odoo_src_cur = odoo_src_conn.cursor()
        odoo_src_cur.execute(config["src_query"])
        res = odoo_src_cur.fetchall()

        execute_values(stage_cur, config["insert_script"], res)
        stage_conn.commit()
        stage_cur.execute(config["src_query"])

        print("Insert count: ", len(stage_cur.fetchall()))

    odoo_src_conn.commit()
    odoo_src_cur.close()
    odoo_src_conn.close()

    stage_cur.close()
    stage_conn.close()
