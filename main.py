"""
Orientações para o TP4 - 2025:

Com base nos encontros de Projeto de Bloco, com alinhamento às diretivas do TP4 do Projeto de Bloco, você deve:

1 - Criar um arquivo json com orientação a registros para carga massiva (inserção ou atualização) de uma tabela do seu banco de dados PostgreSQL
2 - Criar um arquivo json com orientação a registros para deleção massiva de uma tabela do seu banco de dados PostgreSQL
3 - Crie um código em Python, usando SQLAlchemy, para conectar à sua base de dados para realizar o comando de UPSERT (INSERT e UPDATE) na tabela correspondente no seu banco de dados.
4 - Confira o sucesso da inserção e/ou atualização na tabela correspondente do seu Banco de Dados.
5 - Refaça as questões #3 e #4, porém, dessa vez, para delação massiva, usando o arquivo criado na questão #2.
6 - Confira o sucesso da deleção na tabela correspondente do seu Banco de Dados.
"""
import pandas as pd 

from sqlalchemy.dialects.postgresql import insert
from db_conect import upsert_aluno, delete_alunos

df = pd.read_json("alunos.json")

df_delete = pd.read_json("alunos_deletar.json")

# Making the call to the UPSERT function
upsert_aluno(df)
print("UPSERT CONCLUIDO COM SUCESSO !")

#Making the call to the delet function
delete_alunos(df_delete)
print("Deletado com Sucesso")