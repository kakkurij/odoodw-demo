import psycopg2 as pg


conn = pg.connect(
    "host=192.168.38.110 dbname=odoo_dw_setup user=airflow password=airflow"
)
cur = conn.cursor()
cur.execute(
    """
    SELECT
        src_query,
        dst_table,
        pre_execute_script
	FROM public.odoo_stage_table_copy_setup
    where row_enabled = 1"""
)

print(cur.fetchall())
