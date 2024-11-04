from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator

class dbtCommandOperator(BaseOperator):
    def __init__(self, dbt_command, dbt_profile, full_refresh, **kwargs):
        super().__init__(**kwargs)
        self.dbt_command = dbt_command
        self.dbt_profile = dbt_profile
        self.full_refresh = full_refresh
        self.project_dir = '/opt/airflow/dbt_scripts'

    def execute(self, context):
        list_of_commands = [
            f'cd {self.project_dir} && {self.dbt_command} -t {self.dbt_profile} --project-dir {self.project_dir}'
        ]

        for command in list_of_commands:
            if self.full_refresh:
                command = command + ' --full-refresh'
            BashOperator(
                task_id='run_dbt_command',
                bash_command=command,
                env={'DBT_PROFILES_DIR': self.project_dir}
            ).execute()
        return
