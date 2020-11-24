from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insertTable = "",
                 taskId = "",
                 deleteLoad = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.deleteLoad = deleteLoad
        self.insertTable = insertTable
        self.task_ids = taskId

    def execute(self, context):
        self.log.info("Context {}".format(context))
        table = self.table
        task_id = self.task_ids
        deleteTable = self.deleteLoad
        data = context['task_instance'].xcom_pull(task_ids=task_id)['data']
        insertTable = self.insertTable
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Write table {}".format(table))
        insertTable = self.insertTable
        redshift = PostgresHook(postgres_conn_id="redshift")
        if deleteTable:
            sql = f"DELETE FROM {table}"
            redshift.run(sql)
            self.log.info("Clean the table: {}".format(table))

        with redshift.get_conn() as conn:
            with conn.cursor() as cur:
                for row in data:
                    try:
                        cur.execute(insertTable, list(row))
                        conn.commit()
                    except Exception as e:
                        logging.error(e)
                        conn.rollback()