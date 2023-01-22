import pandas as pd

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
