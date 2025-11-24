from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date
from sqlalchemy.dialects.postgresql import insert
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

DB = {
    "database": "CursoAulaPB",
    "user": "postgres",
    "password": "101520",
    "host": "localhost",
    "port": 5432,
}

#STRING DE CONEXÃO
conx_str = (
    f"postgresql+psycopg2://{DB['user']}:{DB['password']}"
    f"@{DB['host']}:{DB['port']}/{DB['database']}"
)

#CRIANDO CONEXÃO
engine = create_engine(conx_str)
print("conexão bem sucedida.")

# Definir tabela aluno
metadata = MetaData()
aluno = Table(
    "aluno", metadata,
    Column("id_aluno", Integer, primary_key=True),
    Column("cpf", String(11), unique=True),
    Column("nome", String(50)),
    Column("datanascimento", Date),
    schema="academic"
)

#  UPSERT function
def upsert_aluno(df):
    """Upsert alunos from a pandas DataFrame into the aluno table.

    Args:
        df (pandas.DataFrame): DataFrame with columns 'cpf', 'nome', 'datanascimento'.
    """
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = insert(aluno).values(
                cpf=row["cpf"],
                nome=row["nome"],
                datanascimento=row["datanascimento"]
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["cpf"],
                set_={
                    "nome": stmt.excluded.nome,
                    "datanascimento": stmt.excluded.datanascimento
                }
            )
            conn.execute(stmt)


# Mass deletion function
def delete_alunos(df):
    """Remove students whose CPF is listed in the given DataFrame.

Args:
    df: pandas.DataFrame with column 'cpf'.
"""
    # Convert DataFrame to a list of CPFs
    cpfs = df["cpf"].astype(str).tolist()

    with engine.begin() as conn:
        stmt = aluno.delete().where(aluno.c.cpf.in_(cpfs))
        result = conn.execute(stmt)
        print(f"Registros deletados: {result.rowcount}")