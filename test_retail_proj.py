import pytest
from lib.DataReader import read_customers , read_orders
from lib.DataManipulation import filter_closed_orders , count_orders_state, filter_orders_generic
from lib.configReader import get_app_config


@pytest.mark.datareader()
def test_read_customer_df(spark):
    cust_count = read_customers(spark,"LOCAL").count()
    assert cust_count == 12435

@pytest.mark.data_count()
def test_read_orders_df(spark):
    order_count = read_orders(spark,"LOCAL").count()
    assert order_count == 68884

@pytest.mark.data_count()
def test_filter_closed_orders(spark):
    order_df = read_orders(spark,"LOCAL")
    filtered_cnt = filter_closed_orders(order_df).count()
    assert filtered_cnt== 7556

@pytest.mark.config_check()
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config['orders.file.path'] == 'data/orders.csv'

@pytest.mark.data_transformation()
def test_count_order_status(spark, expeceted_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_result = count_orders_state(customers_df)

    assert actual_result == expeceted_results.collect()
    
@pytest.mark.skip()
def test_read_app_config1():
    config = get_app_config("LOCAL")
    assert config['orders.file.path'] == 'data/orders.csv'

@pytest.mark.latest()
def test_check_closed_count(spark):
    order_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(order_df,"CLOSED").count()
    assert filtered_count == 7556

@pytest.mark.latest()
def test_check_pending_payment_count(spark):
    order_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(order_df,"PENDING_PAYMENT").count()
    assert filtered_count == 15030

@pytest.mark.latest()
def test_check_completed_count(spark):
    order_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(order_df,"COMPLETE").count()
    assert filtered_count == 22900


@pytest.mark.new_latest()
@pytest.mark.parametrize(
    "status,count",
    [("CLOSED",7556),
     ("COMPLETE",22900),
     ("PENDING_PAYMENT",15030)]
)
def test_check_count(spark,status,count):
    order_df = read_orders(spark,"LOCAL")
    filtered_count = filter_orders_generic(order_df,status).count()
    assert filtered_count == count