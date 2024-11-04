from __future__ import annotations

import datetime
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param, ParamsDict
from airflow.utils.trigger_rule import TriggerRule


with DAG(
    'Example_Params_Trigger_UI',
    schedule=None,
    start_date=datetime.datetime(2022, 3, 4),
    catchup=False,
    tags=["FORMACION"],
    params={
        "names": Param(
            ["Linda", "Martha", "Thomas"],
            type="array",
            description="Define the list of names for which greetings should be generated in the logs."
            " Please have one name per line.",
            title="Names to greet",
        ),
        "english": Param(True, type="boolean", title="English"),
        "german": Param(True, type="boolean", title="German (Formal)"),
        "french": Param(True, type="boolean", title="French"),
    },
) as dag:

    @task(task_id="get_names", task_display_name="Get names")
    def get_names(**kwargs) -> list[str]:
        params: ParamsDict = kwargs["params"]
        if "names" not in params:
            print("Uuups, no names given, was no UI used to trigger?")
            return []
        return params["names"]

    @task.branch(task_id="select_languages", task_display_name="Select languages")
    def select_languages(**kwargs) -> list[str]:
        params: ParamsDict = kwargs["params"]
        selected_languages = []
        for lang in ["english", "german", "french"]:
            if params[lang]:
                selected_languages.append(f"generate_{lang}_greeting")
        return selected_languages

    @task(task_id="generate_english_greeting", task_display_name="Generate English greeting")
    def generate_english_greeting(name: str) -> str:
        return f"Hello {name}!"

    @task(task_id="generate_german_greeting", task_display_name="Erzeuge Deutsche Begrüßung")
    def generate_german_greeting(name: str) -> str:
        return f"Sehr geehrter Herr/Frau {name}."

    @task(task_id="generate_french_greeting", task_display_name="Produire un message d'accueil en français")
    def generate_french_greeting(name: str) -> str:
        return f"Bonjour {name}!"

    @task(task_id="print_greetings", task_display_name="Print greetings", trigger_rule=TriggerRule.ALL_DONE)
    def print_greetings(greetings1, greetings2, greetings3) -> None:
        for g in greetings1 or []:
            print(g)
        for g in greetings2 or []:
            print(g)
        for g in greetings3 or []:
            print(g)
        if not (greetings1 or greetings2 or greetings3):
            print("sad, nobody to greet :-(")

    lang_select = select_languages()
    names = get_names()
    english_greetings = generate_english_greeting.expand(name=names)
    german_greetings = generate_german_greeting.expand(name=names)
    french_greetings = generate_french_greeting.expand(name=names)
    lang_select >> [english_greetings, german_greetings, french_greetings]
    results_print = print_greetings(english_greetings, german_greetings, french_greetings)
