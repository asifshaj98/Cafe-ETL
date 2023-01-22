from extract import df

import pandas as pd


def cleaning_and_arranging_df(df):

    # spliting the first time, turn order_content into a list
    df["order_content"] = df["order_content"].str.split(",")

    # spliting the order_content into individual row
    df = df.explode("order_content")

    # spliting from the last -, turn it into a list
    df["order_content"] = df["order_content"].str.rsplit(pat="-", n=1)

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
product_df = dropping_colums(df, ["date_time", "branch", "total_price", "payment_type"])
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
