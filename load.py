from transform import branch_df, payment_df, product_df, transaction_df, baskets_df

import psycopg2
import psycopg2.extras as extras


def execute_values(conn, df, table):

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ",".join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def increase_transaction_id(conn, df, table_name):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT transaction_id FROM {table_name} ORDER BY transaction_id DESC LIMIT 1;"
    )
    result = cursor.fetchone()
    cursor.close()
    increase_transaction_id = int(result[0])
    df["transaction_id"] += increase_transaction_id
    return df


conn = psycopg2.connect(
    database="postgres", user="postgres", password="pass", host="localhost", port="5432"
)

try:
    baskets_df = increase_transaction_id(conn, baskets_df, "baskets")
except:
    print("first insert")


execute_values(conn, branch_df, "branchs")
execute_values(conn, payment_df, "payments")
execute_values(conn, product_df, "products")
execute_values(conn, transaction_df, "transactions")
execute_values(conn, baskets_df, "baskets")
