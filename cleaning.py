import os
import numpy as np
import pandas as pd
import psycopg2

# file_path = "../reports_from_branches/chesterfield_25-08-2021_09-00-00.csv"
# column_names = ["date_time", "branch", "customer", "order_content", "total_price", "payment_type", "credit_card_number"]
# df = pd.read_csv(file_path, names=column_names)


def cleaning_and_arranging_df(df):

    # spliting the first time, turn order_content into a list
    df["order_content"] = df["order_content"].str.split(",")

    # spliting the order_content into individual row
    df = df.explode("order_content")

    # spliting from the last -, turn it into a list
    df["order_content"] = df["order_content"].str.rsplit(pat="-", n=1)

    # putting the two list items into two newly created columns
    df[["item", "price"]] = pd.DataFrame(df.order_content.tolist(), index=df.index)

    # striping whitespaces
    df["price"] = df["price"].str.strip()
    df["item"] = df["item"].str.strip()

    # drop old columns
    df = df.drop(columns=["order_content"])

    # rearrange columns
    new_col = [
        "date_time",
        "branch",
        "customer",
        "item",
        "price",
        "total_price",
        "payment_type",
        "credit_card_number",
    ]
    df = df[new_col]

    return df
