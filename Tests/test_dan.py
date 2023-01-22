from transform import branch_df, transaction_df, baskets_df, product_df, payment_df
import pytest


def test_columns_branch_df():
    assert branch_df.columns == "branch_name"


def test_columns_transaction_df():
    for col in transaction_df.columns:
        assert col in ["date_time", "total_price", "branch_id", "payment_type_id"]
    assert len(transaction_df.columns) == 4


def test_columns_baskets_df():
    for col in baskets_df.columns:
        assert col in ["transaction_id", "product_id"]
    assert len(baskets_df.columns) == 2


def test_columns_product_df():
    for col in product_df.columns:
        assert col in ["product_name", "product_price"]
    assert len(product_df.columns) == 2


def test_columns_payment_df():
    assert payment_df.columns == "payment_type"
