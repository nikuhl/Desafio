# Biblioteca para conexão com o banco
import psycopg2
from sqlalchemy import create_engine

# Biblioteca de manipulação para analise de dados
import pandas as pd  

# Variaveis de conexão, e cursor para scripts
con = psycopg2.connect(host='localhost', database='server_test',user='postgres', password='2207')
cnxn = create_engine("postgresql+psycopg2://postgres:2207@localhost:5432/server_test")
cur = con.cursor()

# Criação de todas as tabelas para a atividade
sql = "drop table if exists BDGT;"
sql = sql + " CREATE TABLE BDGT"
sql = sql + "(AST VARCHAR(4),SEKT VARCHAR(10), JAGH VARCHAR(4), MONAT01 NUMERIC(18,3),"
sql = sql + "MONAT02 NUMERIC(18,3),"
sql = sql + "MONAT03 NUMERIC(18,3),"
sql = sql + "MONAT04 NUMERIC(18,3),"
sql = sql + "MONAT05 NUMERIC(18,3),"
sql = sql + "MONAT06 NUMERIC(18,3),"
sql = sql + "MONAT07 NUMERIC(18,3),"
sql = sql + "MONAT08 NUMERIC(18,3),"
sql = sql + "MONAT09 NUMERIC(18,3),"
sql = sql + "MONAT10 NUMERIC(18,3),"
sql = sql + "MONAT11 NUMERIC(18,3),"
sql = sql + "MONAT12 NUMERIC(18,3));"
sql = sql + "drop table if exists FATO_ORCAMENTO;"
sql = sql + "CREATE TABLE FATO_ORCAMENTO (FILIAL VARCHAR(4),SETOR VARCHAR(10), DT_ORCAMENTO DATE,VALOR NUMERIC(18,3));"
cur.execute(sql)

#Inserção de dados teste
sql = "INSERT INTO bdgt(ast, sekt, jagh, monat01, monat02, monat03, monat04, monat05, monat06, monat07, monat08, monat09, monat10, monat11, monat12)"
sql = sql + "VALUES"
sql = sql + "('0001', 'ABCD1', '2021', 5000 , 1000, 2000, 50000, 3000, 88800, 21000, 8000, 0 , 0 ,0 , 0),"
sql = sql + "('0002', 'ABCD1', '2021', 5000 , 1000, 2000, 50000, 3000, 88800, 21000, 8000, 0 , 0 ,0 , 0),"
sql = sql + "('0003', 'ABCD1', '2021', 5000 , 1000, 2000, 50000, 3000, 88800, 21000, 8000, 0 , 0 ,0 , 0)"
cur.execute(sql)
con.commit()

# Inserção de dados para manipulação no dataframe
cur.execute('select * from bdgt')
colunas =["ast", "sekt", "jagh", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
df = pd.DataFrame(cur.fetchall(),columns=colunas)
# print(df)
df2 =  pd.melt(df,id_vars=["ast", "sekt","jagh"], var_name='Meses', value_name='Valor') # Utilizar o melt para desfazer o pivot na tabela matriz "unpivot"
df2['Data'] = df2['jagh'].astype(str) + df2['Meses'].astype(str)+'01' #adicionando a coluna com o primeiro dia de cada mês
df3 = df2[['ast','sekt','Data','Valor']] #formatar um dataframe com as colunas do insert
df4 = df3.rename(columns={'ast':'filial','sekt':'setor','Data' :'dt_orcamento','Valor':'valor'}) #Renomear as colunas para insert direto
# print(df4)
df4.to_sql('fato_orcamento', cnxn,if_exists='append',index=False)#insert utilizando o to_sql (Opção append utilizada para evitar conflito de tabela ja existente)
con.close()