import psycopg2

conn = psycopg2.connect(host='localhost', port='5432', user="postgres", password="postgres", database="tp1")
conn.autocommit = True
cur = conn.cursor()

def query(sql):
    try:
        cur.execute(sql)
    except Exception as e:
        return (f"Ocorreu um erro: {e}")
    return cur.fetchall()

def top_useful_comments_high_low_rating(product_id):
    sql = f"""
    SELECT * FROM (
        SELECT * FROM comment
        WHERE id_product = 1
        ORDER BY helpful DESC, rating DESC
        LIMIT 5
    ) AS top_comments
    UNION
    SELECT * FROM (
        SELECT * FROM comment
        WHERE id_product = 1
        ORDER BY helpful DESC, rating ASC
        LIMIT 5
    ) AS bottom_comments;
    """
    return query(sql)

def similar_products_higher_sales(product_id):
    sql = f"""
    SELECT sp.* FROM similar_products sp
    INNER JOIN product p ON sp.asin_product_2 = p.asin
    WHERE sp.id_product_1 = {product_id}
    AND p.salesrank > (
        SELECT salesrank FROM product
        WHERE id = {product_id}
    );
    """
    return query(sql)

def daily_avg_rating_evolution(product_id, start_date, end_date):
    sql = f"""
    SELECT date, AVG(rating) AS avg_rating
    FROM comment
    WHERE id_product = {product_id}
    AND date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY date
    ORDER BY date;
    """
    return query(sql)

def top_selling_products_in_each_category():
    sql = """
    SELECT * FROM (
        SELECT *,
        RANK() OVER(PARTITION BY category ORDER BY salesrank) AS category_rank
        FROM product
    ) AS ranked_products
    WHERE category_rank <= 10
    ORDER BY category, salesrank
    """
    return query(sql)

def top_products_avg_useful_positive_ratings():
    sql = """
    SELECT id_product, AVG(helpful) AS avg_helpful
    FROM comment
    GROUP BY id_product
    ORDER BY avg_helpful DESC
    LIMIT 10;
    """
    return query(sql)

def top_categories_avg_useful_positive_ratings():
    sql = """
    SELECT p.category, AVG(c.helpful) AS avg_helpful_per_product
    FROM product p
    INNER JOIN comment c ON p.id = c.id_product
    GROUP BY p.category
    ORDER BY avg_helpful_per_product DESC
    LIMIT 5;
    """
    return query(sql)

def top_clients_most_reviews_per_product_group():
    sql = """
    SELECT id_client, COUNT(*) AS review_count
    FROM comment
    GROUP BY id_client
    ORDER BY review_count DESC
    LIMIT 10;
    """
    return query(sql)

## Modifique as linhas 100 (productID), 101 (startDate) e 102 (endDate) colocando os dados para completar as querys de consulta

productID = 21
startDate = '1998-03-01'
endDate = '2005-03-01'

resultA = top_useful_comments_high_low_rating(productID)
resultB = similar_products_higher_sales(productID)
resultC = daily_avg_rating_evolution(productID, startDate, endDate)
resultD = top_selling_products_in_each_category()
resultE = top_products_avg_useful_positive_ratings()
resultF = top_categories_avg_useful_positive_ratings()
resultG = top_clients_most_reviews_per_product_group()

print("A - Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
for row in resultA:
    print(row)

print("B - Dado um produto, listar os produtos similares com maiores vendas do que ele")
for row in resultB:
    print(row)

print("C - Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada")
for row in resultC:
    print(row)

print("D - Listar os 10 produtos líderes de venda em cada grupo de produtos")
for row in resultD:
    print(row)

print("E - Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
for row in resultE:
    print(row)

print("F - Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
for row in resultF:
    print(row)

print("G - Listar os 10 clientes que mais fizeram comentários por grupo de produto")
for row in resultG:
    print(row)