import psycopg2

conn = psycopg2.connect(
    database="postgres", user="postgres", password="pass", host="127.0.0.1", port="5432"
)

print("Opened datebase successfully")

cur = conn.cursor()


cur.execute(
    """CREATE TABLE IF NOT EXISTS products
    (product_id SERIAL PRIMARY KEY,
    product_name TEXT UNIQUE,
    product_price FLOAT)
"""
)

cur.execute(
    """CREATE TABLE IF NOT EXISTS payments
    (payment_id SERIAL PRIMARY KEY,
    payment_type TEXT UNIQUE)
"""
)

cur.execute(
    """CREATE TABLE IF NOT EXISTS branchs
    (branch_id SERIAL PRIMARY KEY,
    branch_name TEXT UNIQUE)
"""
)

cur.execute(
    """CREATE TABLE IF NOT EXISTS transactions
    (transaction_id SERIAL PRIMARY KEY,
    branch_id INT,
    payment_type_id int,
    total_price FLOAT,
    date_time timestamp)
"""
)

cur.execute(
    """CREATE TABLE IF NOT EXISTS baskets
    (basket_item_id SERIAL PRIMARY KEY,
    transaction_id int,
    product_id int)
"""
)

cur.execute(
    """ALTER TABLE transactions
ADD CONSTRAINT fk_transaction_branch 
FOREIGN KEY (branch_id) 
REFERENCES branchs(branch_id);
"""
)

cur.execute(
    """ALTER TABLE transactions
ADD CONSTRAINT fk_transaction_payment 
FOREIGN KEY (payment_type_id) 
REFERENCES payments(payment_id);
"""
)

cur.execute(
    """ALTER TABLE baskets
ADD CONSTRAINT fk_basket_transaction
FOREIGN KEY (transaction_id) 
REFERENCES transactions(transaction_id);
"""
)

cur.execute(
    """ALTER TABLE baskets
ADD CONSTRAINT fk_basket_product 
FOREIGN KEY (product_id) 
REFERENCES products(product_id);
"""
)

print("Table created successfully")

conn.commit()
conn.close()
