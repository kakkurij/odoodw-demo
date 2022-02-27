# odoodw-demo

Testing dw solution for odoo. Utilizing Apache airflow &amp; data vault.

To run this program you need:

1. Docker-compose
2. Odoo running in a different machine than local (or it has to be available through public ip) as a source database
3. Setup database
4. Staging database
5. DW database

Setup &amp; staging &amp; dw can be in a same server, but its recommended to put source and dw to separate servers.

# NOTE: Currently only postgres is supported as setup/staging/dw! You cannot use snowflake etc.
