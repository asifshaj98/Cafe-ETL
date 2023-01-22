from cleaning import cleaning_and_arranging_df
import pandas as pd


def test_df_type():
    file_path = "reports_from_branches/chesterfield_25-08-2021_09-00-00.csv"
    column_names = [
        "date_time",
        "branch",
        "customer",
        "order_content",
        "total_price",
        "payment_type",
        "credit_card_number",
    ]
    df = pd.read_csv(file_path, names=column_names)

    cleaned_df = cleaning_and_arranging_df(df)

    assert isinstance(cleaned_df, pd.DataFrame) == True
