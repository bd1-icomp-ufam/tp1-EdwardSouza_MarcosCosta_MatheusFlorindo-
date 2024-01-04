import psycopg2
import sys
import re

# funcao para criar o BD
def creatBD():
    connection = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        database = 'postgres', 
        user= 'postgres', 
        password = 'postgres'
    )
    connection.autocommit = True
    cursor = connection.cursor()

    sqlCreateDataset = '''CREATE DATABASE tp1'''
    cursor.execute(sqlCreateDataset)
    connection.close()

# funcao para excluir o BD
def dropDB():
    connection = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        database = 'postgres', 
        user= 'postgres', 
        password = 'postgres'
    )
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute('''SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='tp1';''')
    cursor.execute('''DROP DATABASE IF EXISTS TP1''')
    connection.close()

# funcao para criar as tabelas no BD
def createTables():

    cursor.execute("DROP TABLE IF EXISTS PRODUCT")
    cursor.execute("DROP TABLE IF EXISTS CLIENTE")
    cursor.execute("DROP TABLE IF EXISTS CATEGORY_PRODUCT")
    cursor.execute("DROP TABLE IF EXISTS SUBCATEGORY")
    cursor.execute("DROP TABLE IF EXISTS SUBCATEGORY_PPRODUCT")
    cursor.execute("DROP TABLE IF EXISTS COMMENT")
    cursor.execute("DROP TABLE IF EXISTS SIMILAR_PRODUCTS")
    sqlCreateTable =  '''
                    CREATE TABLE product (
                        id INTEGER PRIMARY KEY NOT NULL,
                        asin VARCHAR(255) NOT NULL,
                        title VARCHAR(1000),
                        category VARCHAR(255),
                        salesrank INTEGER
                    );
                    CREATE TABLE similar_products (
                        id SERIAL PRIMARY KEY NOT NULL,
                        id_product_1 INTEGER NOT NULL,
                        asin_product_2 VARCHAR(255) NOT NULL,
                        FOREIGN KEY (id_product_1) REFERENCES product(id)
                    );

                    CREATE TABLE comment (
                        id SERIAL PRIMARY KEY NOT NULL,
                        id_product INTEGER NOT NULL,
                        id_client VARCHAR(255) NOT NULL,
                        date DATE,
                        rating INTEGER,
                        votes INTEGER,
                        helpful INTEGER,
                        FOREIGN KEY (id_product) REFERENCES product(id)
                    );            
    '''
    cursor.execute(sqlCreateTable)

# CREATE TABLE product (
#     id INTEGER PRIMARY KEY NOT NULL,
#     asin VARCHAR(255) NOT NULL,
#     title VARCHAR(1000),
#     category VARCHAR(255),
#     salesrank INTEGER
# );
def insert_product_data (id, asin, title, category, salesrank):
    
    sqlInsert = " INSERT INTO product (id, asin, title, category, salesrank) VALUES (%s, %s, %s, %s, %s);"
    data = (id, asin, title, category, salesrank)
    cursor.execute(sqlInsert, data)

# CREATE TABLE similar_products (
#     id SERIAL PRIMARY KEY NOT NULL,
#     id_product_1 INTEGER NOT NULL,
#     asin_product_2 VARCHAR(255) NOT NULL,
#     FOREIGN KEY (id_product_1) REFERENCES product(id)
# );

def insert_similar_data (id_product_1, asin_product_2):
    
    sqlInsert = " INSERT INTO similar_products (id_product_1, asin_product_2) VALUES (%s, %s);"
    data = (id_product_1, asin_product_2,)
    cursor.execute(sqlInsert, data)

# CREATE TABLE comment (
#     id SERIAL PRIMARY KEY NOT NULL,
#     id_product INTEGER NOT NULL,
#     id_client VARCHAR(255) NOT NULL,
#     date DATE,
#     rating INTEGER,
#     votes INTEGER,
#     helpful INTEGER,
#     FOREIGN KEY (id_product) REFERENCES product(id)
# );

def insert_comment_data (id_product, id_client, date, rating, votes, helpful):
    
    sqlInsert = " INSERT INTO comment (id_product, id_client, date, rating, votes, helpful) VALUES (%s, %s, %s, %s, %s, %s);"
    data = (id_product, id_client, date, rating, votes, helpful,)
    cursor.execute(sqlInsert, data)


dropDB()
creatBD()

# criar conexao com o BD tp1
connection = psycopg2.connect(
        host = 'localhost',
        port = 5432,
        database = 'tp1', 
        user= 'postgres', 
        password = 'postgres'
    )
connection.autocommit = True
cursor = connection.cursor()

createTables()

# abrir arquivo, nome do arquivo é recebido por linha de comando
fileName = sys.argv[1]
file = open(fileName, 'r')
lines = file.readlines()

# expressoes regulares usadas para extrair dados do arquivo txt
id_pattern = re.compile(r'Id:\s+(\d+)')
asin_pattern = re.compile(r'ASIN:\s+(\d+)')
title_pattern = re.compile(r'title:\s+(.*)')
group_pattern = re.compile(r"group:\s*(.+)")
salesrank_pattern = re.compile(r'salesrank:\s+(\d+)')
similar_pattern = re.compile(r"similar:\s*(\d+)\s*([\w\s]+)")
reviews_pattern = re.compile(r"reviews:\s*total:\s*(\d+)\s*downloaded:\s*\d+\s*avg rating:\s*([\d.]+)")
review_item_pattern = re.compile(r"\s*(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s+(\w+)\s+rating:\s+([\d.]+)\s+votes:\s+(\d+)\s+helpful:\s+(\d+)")

#inicializacao de variaveis utilizadas como argumento da chamada das funcoes de insercao
id = 0
asin = ''
title = ''
group = ''
salesrank = 0
similar_num = 0
similar = []
reviews_total = 0
reviews_avg_rating = 0.0
id_client = ''
date = []
rating = 0.0 
votes = 0,
helpful = 0

discontinued = 1

# extracao dos dados e insercao no BD tp1 na tabela product
for line in lines:
    
    line = line.strip()

    id_match = id_pattern.match(line)
    if id_match:
        if(discontinued == 0):
            insert_product_data(id, asin, title, group, salesrank)
        else:
            discontinued = 0
        id = int(id_match.group(1))

    title_match = title_pattern.match(line)
    if title_match:
        title = title_match.group(1)

    salesrank_match = salesrank_pattern.match(line)
    if salesrank_match:
        salesrank = int(salesrank_match.group(1))

    asin_match = asin_pattern.match(line)
    if asin_match:
        asin = asin_match.group(1)

    group_match = group_pattern.match(line)
    if group_match:
        group = group_match.group(1)
        
    if(line == 'discontinued product'):
        discontinued = 1
   
insert_product_data(id, asin, title, group, salesrank)

# insercao dos dados na tabela similar_product e comment
for line in lines:
    
    line = line.strip()

    id_match = id_pattern.match(line)
    if id_match:
        if(discontinued != 0):
            discontinued = 0
        id = int(id_match.group(1))

    similar_match = similar_pattern.match(line)
    if similar_match:
        similar_num = similar_match.group(1)
        similar = similar_match.group(2)
        for i in range(int(similar_num)):
            similar.strip
            insert_similar_data (id, similar.split()[i])

    reviews_match = reviews_pattern.match(line)
    if reviews_match:
        reviews_total = reviews_match.group(1)
        reviews_avg_rating = reviews_match.group(2)

    review_item_match = review_item_pattern.match(line)
    if review_item_match:
        date = review_item_match.group(1)
        id_client = review_item_match.group(2)
        rating = review_item_match.group(3)
        votes = review_item_match.group(4)
        helpful = review_item_match.group(5)
        insert_comment_data(id, id_client, date, rating, votes, helpful)
        
    if(line == 'discontinued product'):
        discontinued = 1
    
# fecha arquivo e fecha a conexao com o BD
file.close()
connection.close()