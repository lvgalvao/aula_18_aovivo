from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from python.controller import fetch_pokemon_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

@dag(
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=days_ago(1),
    tags=['pokemon']
)
def etl_dag():
    @task
    def define_pokemon():
        pokemon_id = 1  # Define o ID do Pokémon que você deseja extrair
        return pokemon_id

    @task
    def fetch_pokemon(pokemon_id):
        pokemon_schema = fetch_pokemon_data(pokemon_id)
        if pokemon_schema:
            return pokemon_schema
        else:
            raise ValueError(f"Não foi possível obter dados para o Pokémon com ID {pokemon_id}.")

    @task
    def print_pokemon(pokemon_schema):
        print(f"Dados do Pokémon: {pokemon_schema}")

    pokemon_id = define_pokemon()
    pokemon_data = fetch_pokemon(pokemon_id)
    print_pokemon(pokemon_data)

dag = etl_dag()
