import pandas as pd
import pytest
from cleaning import cleaning_and_arranging_df


@pytest.fixture
def data():
    data = pd.DataFrame(
        {
            "date_time": ["25/08/2021 09:00"],
            "branch": ["Chesterfield"],
            "customer": ["Richard Copeland"],
            "order_content": [
                "Regular Flavoured iced latte - Hazelnut - 2.75, Large Latte - 2.45"
            ],
            "total_price": [5.2],
            "payment_type": ["CARD"],
            "credit_card_number": [5494173772652516],
        }
    )

    return data


def test_dataframe(data):
    assert isinstance(data, pd.DataFrame)


# def test_clean_data(data):
#     clean_df = cleaning_and_arranging_df(data)

#     assert clean_df == data['credit_card_number']
