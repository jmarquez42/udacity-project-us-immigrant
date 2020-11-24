from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insertTable = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.insertTable = insertTable

    def execute(self, context):
        table = self.table
        insertTable = self.insertTable
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Write table {}".format(table))
        sql = f"INSERT INTO {table} {insertTable}"
        self.log.info("SQL LoadFactOperator \n {}".format(sql))
        redshift.run(sql)
