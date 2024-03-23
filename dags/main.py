from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from include.controller import fetch_pokemon_data, add_pokemon_to_db

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
    tags=['pokemon'],
    catchup=False  # Define catchup como False para evitar execuções retroativas
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
    def add_pokemon_to_db(pokemon_schema):
        print(f"Dados do Pokémon: {pokemon_schema}")

    pokemon_id = define_pokemon()
    pokemon_data = fetch_pokemon(pokemon_id)
    add_pokemon_to_db(pokemon_data)

    define_pokemon >> fetch_pokemon >> add_pokemon_to_db

dag = etl_dag()
