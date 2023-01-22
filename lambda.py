import json
import psycopg2
import psycopg2.extras as extras
import boto3
import csv
import io
import pandas as pd
import os

s3Client = boto3.client("s3")


def lambda_handler(event, context):
    print(event)
    # Get our bucket and file name
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    print(bucket)
    print(key)

    # Get our object
    response = s3Client.get_object(Bucket=bucket, Key=key)

    # Process it
    data = response["Body"].read().decode("utf-8")
    reader = csv.reader(io.StringIO(data))
    df = pd.DataFrame(reader, columns=None)
    df.columns = [
        "date_time",
        "branch",
        "customer",
        "order_content",
        "total_price",
        "payment_type",
        "credit_card_number",
    ]
    # print(type(df))

    # print(df)

    def cleaning_and_arranging_df(df):

        # spliting the first time, turn order_content into a list
        df["order_content"] = df["order_content"].str.split(",")

        # spliting the order_content into individual row
        df = df.explode("order_content")

        # spliting from the last -, turn it into a list
        df["order_content"] = df["order_content"].str.rsplit("-", 1)

        # putting the two list items into two newly created columns
        df[["product_name", "product_price"]] = pd.DataFrame(
            df.order_content.tolist(), index=df.index
        )

        # striping whitespaces
        df["product_price"] = df["product_price"].str.strip()
        df["product_name"] = df["product_name"].str.strip()

        # drop old columns
        df = df.drop(columns=["order_content"])

        # rearrange columns
        new_col = [
            "date_time",
            "branch",
            "customer",
            "product_name",
            "product_price",
            "total_price",
            "payment_type",
            "credit_card_number",
        ]
        df = df[new_col]

        # change the column to datetime format
        df["date_time"] = pd.to_datetime(df["date_time"])

        return df

    def dropping_colums(df, drop_cols):
        df = df.drop(drop_cols, axis=1)
        return df

    def create_foreign_key_dict(df, col_name):
        foreign_key_dict = {}
        for i, item in enumerate(df[col_name].unique()):
            foreign_key_dict[item] = i + 1
        return foreign_key_dict

    def add_foreign_key_column(df, new_col, exist_col, foreign_key_dict):
        df[new_col] = df[exist_col].map(foreign_key_dict)
        return df

    branch_name = {"Chesterfield": 1, "Longridge": 2, "Uppingham": 3}

    branch_df = dropping_colums(
        df,
        [
            "date_time",
            "order_content",
            "total_price",
            "payment_type",
            "customer",
            "credit_card_number",
        ],
    )
    branch_df.rename(columns={"branch": "branch_name"}, inplace=True)
    branch_df = branch_df.drop_duplicates()
    branch_df.loc[0] = "Chesterfield"
    branch_df.loc[1] = "Longridge"
    branch_df.loc[2] = "Uppingham"

    payment_df = dropping_colums(
        df,
        [
            "date_time",
            "branch",
            "order_content",
            "total_price",
            "customer",
            "credit_card_number",
        ],
    )
    payment_df = payment_df.drop_duplicates()

    transaction_df = dropping_colums(
        df, ["order_content", "customer", "credit_card_number"]
    )
    transaction_df["date_time"] = pd.to_datetime(df["date_time"])

    branch_dict = create_foreign_key_dict(transaction_df, "branch")
    payment_type_dict = create_foreign_key_dict(transaction_df, "payment_type")

    transaction_df = add_foreign_key_column(
        transaction_df, "branch_id", "branch", branch_name
    )
    transaction_df = add_foreign_key_column(
        transaction_df, "payment_type_id", "payment_type", payment_type_dict
    )
    transaction_df = dropping_colums(transaction_df, ["branch", "payment_type"])

    df = cleaning_and_arranging_df(df)
    df = dropping_colums(df, ["customer", "credit_card_number"])
    product_df = dropping_colums(
        df, ["date_time", "branch", "total_price", "payment_type"]
    )
    product_df = product_df.drop_duplicates()

    df.index += 1
    baskets_df = df.rename_axis("transaction_id").reset_index()

    product_dict = {
        "Regular Flavoured iced latte - Hazelnut": 2.75,
        "Large Latte": 2.45,
        "Large Flavoured iced latte - Caramel": 3.25,
        "Regular Flavoured iced latte - Caramel": 2.75,
        "Large Flavoured iced latte - Hazelnut": 3.25,
        "Regular Flavoured latte - Hazelnut": 2.55,
        "Large Flat white": 2.45,
        "Regular Latte": 2.15,
        "Regular Flat white": 2.15,
        "Large Flavoured latte - Hazelnut": 2.85,
        "Regular Flavoured iced latte - Vanilla": 2.75,
        "Large Flavoured iced latte - Vanilla": 3.25,
        "Large Speciality Tea - Earl Grey": 1.6,
        "Large Smoothies - Glowing Greens": 2.5,
        "Regular Speciality Tea - Earl Grey": 1.3,
        "Regular Mocha": 2.3,
        "Regular Smoothies - Glowing Greens": 2,
        "Large Glass of milk": 1.1,
        "Regular Glass of milk": 0.7,
        "Regular Frappes - Strawberries & Cream": 2.75,
        "Large Frappes - Strawberries & Cream": 3.25,
        "Large Mocha": 2.7,
        "Large Speciality Tea - Peppermint": 1.6,
        "Regular Speciality Tea - Peppermint": 1.3,
        "Regular Smoothies - Berry Beautiful": 2,
        "Regular Flavoured latte - Caramel": 2.55,
        "Large Smoothies - Berry Beautiful": 2.5,
        "Large Red Label tea": 1.8,
        "Large Flavoured hot chocolate - Vanilla": 2.9,
        "Regular Cortado": 2.05,
        "Large Hot chocolate": 2.9,
        "Large Speciality Tea - Green": 1.3,
        "Regular Hot chocolate": 2.2,
        "Regular Flavoured hot chocolate - Vanilla": 2.6,
    }

    product_df = pd.DataFrame(
        list(product_dict.items()), columns=["product_name", "product_price"]
    )

    product_id_dict = create_foreign_key_dict(baskets_df, "product_name")
    baskets_df = add_foreign_key_column(
        baskets_df, "product_id", "product_name", product_id_dict
    )
    baskets_df = dropping_colums(
        baskets_df,
        [
            "date_time",
            "branch",
            "product_name",
            "product_price",
            "total_price",
            "payment_type",
        ],
    )
    baskets_df = baskets_df.convert_dtypes()

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

    Access_key = os.getenv("aws_access_key_id")
    Access_Secrete = os.getenv("aws_secret_access_key")
    dbname = os.getenv("dbname")
    host = os.getenv("host")
    user = os.getenv("user")
    password = os.getenv("password")
    # tablename = os.getenv('tablename')
    connection = psycopg2.connect(
        dbname=dbname, host=host, port="5439", user=user, password=password
    )

    try:
        baskets_df = increase_transaction_id(connection, baskets_df, "baskets")
    except:
        print("first insert")

    execute_values(connection, branch_df, "branches")
    print("Fire 1")
    execute_values(connection, payment_df, "payments")
    print("Fire 2")
    execute_values(connection, product_df, "products")
    print("Fire 3")
    execute_values(connection, transaction_df, "transactions")
    print("Fire 4")
    execute_values(connection, baskets_df, "baskets")
    print("Fire 5")
    print("is this even working?")
