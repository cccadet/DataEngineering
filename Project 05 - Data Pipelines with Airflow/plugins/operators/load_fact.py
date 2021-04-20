from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = 'INSERT INTO {} {}'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name                  
                 redshift_conn_id = '',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info('Start LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Delete Redshift table')
        redshift.run('DELETE FROM {}'.format(self.table))

        insert_sql_formatted = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql
        )
        #self.log.info(f'Run {insert_sql_formatted}')
        redshift.run(insert_sql_formatted)
