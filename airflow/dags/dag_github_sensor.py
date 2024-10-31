from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.github.sensors.github import GithubSensor
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime
from typing import Any
import logging

task_logger = logging.getLogger("airflow.task")

YOUR_GITHUB_REPO_NAME = Variable.get(
    "my_github_repo", "apache/airflow"
)
YOUR_COMMIT_MESSAGE = "STOPREPO"  # Replace with your commit message


def commit_message_checker(repo: Any, trigger_message: str) -> bool | None:
    """Check the last 10 commits to a repository for a specific message.
    Args:
        repo (Any): The GitHub repository object.
        trigger_message (str): The commit message to look for.
    """

    task_logger.info(
        f"Checking for commit message: {trigger_message} in 10 latest commits to the repository {repo}."
    )

    result = None
    try:
        if repo is not None and trigger_message is not None:
            commits = repo.get_commits().get_page(0)[:10]
            for commit in commits:
                if trigger_message in commit.commit.message:
                    result = True
                    break
            else:
                result = False

    except Exception as e:
        raise AirflowException(f"GitHub operator error: {e}")
    return result


default_args = {
    "owner": "Pablo Ramos",
    "start_date": datetime(2024, 10, 27),
    "email_on_failure": True,
    "email_on_retry": False
}

dag = DAG(
    "github_commit_sensor",
    default_args=default_args,
    description="DAG for example",
    schedule_interval="@daily",
    catchup=False,
    tags=['FORMACION']
)

github_sensor = GithubSensor(
    task_id="example_sensor",
    github_conn_id="my_github_conn",
    method_name="get_repo",
    method_params={"full_name_or_id": YOUR_GITHUB_REPO_NAME},
    result_processor=lambda repo: commit_message_checker(repo, YOUR_COMMIT_MESSAGE),
    timeout=60 * 60,
    poke_interval=30,
    dag=dag
)

start= EmptyOperator(task_id= 'start', dag=dag)

end= EmptyOperator(task_id= 'end', dag=dag)

# Set up task dependencies
start >> github_sensor >> end