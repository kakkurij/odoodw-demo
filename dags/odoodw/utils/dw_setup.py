class DWSetup:
    def __init__(self, setup_conn, src_conn):
        """
        Requires variable setup, that is a list of instructions for setting up stage load

        Columns that are necessary are:
        0:  src_query (sql query from the source)
        1: dst_table (destination table name)
        2: pre_execute_scripts (If you want to perform some action before inserting to destination table (truncate etc.))
        3: row_enabled (optional)


        NOTE: This class will NOT close the connection. It just utilizes the connection


        returns a list of configurations:

        [
            {
                'src_query': 'select * from xx',
                'src_schema': 'public',
                'src_table': 'table',
                'pre_execute_script': 'truncate/delete xxx',
                'row_enabled': 1/0,
                'create_script': 'create table xxx' (setup_conn must be provided)

            }

        ]
        """

        self.src_conn = src_conn

        self.setup = []

        self.conn = setup_conn
        cur = self.conn.cursor()
        cur.execute(
            """
        SELECT
            src_query,
            src_schema,
            src_table,
            pre_execute_script
        FROM public.stage_table_copy_setup
        where row_enabled = 1"""
        )

        setup_list = cur.fetchall()
        cur.close()

        for row in setup_list:
            setup_row = {}
            if len(row) >= 4:
                setup_row["src_query"] = row[0]
                setup_row["src_schema"] = row[1]
                setup_row["src_table"] = row[2]
                setup_row["pre_execute_script"] = row[3]
                setup_row["row_enabled"] = 1

            if len(row) > 4:
                setup_row["row_enabled"] = row[4]

            ## Add create script

            # Fetch columns used in the create script
            cols = (
                setup_row["src_query"]
                .lower()
                .split("select")[1]
                .split("from")[0]
                .strip()
            )

            cols = list(map(lambda x: f"'{x.strip()}'", cols.split(",")))
            insert_cols = list(map(lambda x: x.replace("'", ""), cols))

            create_script = self._create_create_script(
                setup_row["src_schema"], setup_row["src_table"], cols
            )

            insert_script = self._create_bulk_insert_script(
                setup_row["src_schema"], setup_row["src_table"], insert_cols
            )

            setup_row["create_script"] = create_script
            setup_row["insert_script"] = insert_script

            self.setup.append(setup_row)

    def get(self):
        return self.setup

    def _create_create_script(self, schema, table, columns):
        """Get source table schema and construct create script for it"""

        query = f"""select column_name, data_type from information_schema.columns where table_schema='{schema}' and table_name='{table}' and column_name in ({ ",".join(columns) })"""

        cur = self.src_conn.cursor()
        cur.execute(query)

        cols = cur.fetchall()

        # Construct create statement

        # Turn information schema (columns, datatype) to create table script

        ## [ (id, integer), (name, character varying)] et...
        ## -> create table schema.table (id integer, name character varying)
        sql = f"create table if not exists {schema}.{table} ("
        query = ",".join(list(map(lambda x: f"{x[0]} {x[1]}", cols)))

        sql += query + ")"

        cur.close()
        return sql

    def _create_bulk_insert_script(self, schema, table, columns):
        """Creates insert script suitable for bulk insert (psycopg2.extras.execute_values())"""
        query = f"""insert into {schema}.{table} ({",".join(columns)}) values %s"""
        return query
