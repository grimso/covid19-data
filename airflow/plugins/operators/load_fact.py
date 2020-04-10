from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    @apply_defaults
    def __init__(self, conn_id="redshift", sql_query="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        db_hook.run(self.sql_query)
        # self.log.info('LoadFactOperator not implemented yet')
