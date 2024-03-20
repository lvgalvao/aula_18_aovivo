class PokemonSchema(Base, table=True):
    name: str
    type: str

class PokemonModel(PokemonSchema):
    id: int
    created_at: date

