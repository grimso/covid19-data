from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 sql_query="",
                 truncate_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.truncate_table=truncate_table

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        if self.truncate_table:
            db_hook.run(f"TRUNCATE {self.truncate_table}")
            self.log.info(f'Truncated table {self.truncate_table} before insert')
        db_hook.run(self.sql_query)
