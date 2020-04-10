from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"
    check_query = "SELECT COUNT(*) FROM {}"

    @apply_defaults
    def __init__(self, conn_id="redshift", tables_to_check=[], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables_to_check = tables_to_check

    def execute(self, context):
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)
        for table in self.tables_to_check:
            records = db_hook.get_records(DataQualityOperator.check_query.format(table))
            if records is None or len(records[0]) < 1:
                raise ValueError(f"No records present in table {table}")
            nr_of_rows = records[0][0]
            self.log.info(f"Table `{table}` has {records[0][0]} rows")
            if nr_of_rows < 1:
                raise ValueError(f"Table {table} is empty")

        self.log.info("Tables passed data quality check")
