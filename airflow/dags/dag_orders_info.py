import pendulum
from airflow.decorators import dag, task

from operators.postgres import (
    DataFrameToPostgresOverrideOperator,
    PostgresToDataFrameOperator,
)


@dag(schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["USwB"],
)

def order_info_result_dag():

    products = PostgresToDataFrameOperator(
        table_name="products",
        task_id="extract_products_dataset",
    )
    
    product_category_names = PostgresToDataFrameOperator(
        table_name="product_category_names",
        task_id="extract_product_category_names_dataset",
    )
    
    orders = PostgresToDataFrameOperator(
        table_name="orders",
        task_id="extract_orders_dataset",
    )
    
    order_items = PostgresToDataFrameOperator(
        table_name="order_items",
        task_id="extract_order_items_dataset",
    )
    
    @task
    def date_fix(orders):
        import pandas as pd

        orders.order_purchase_timestamp = pd.to_datetime(
            orders.order_purchase_timestamp
        )
        
        return orders
    
    @task
    def eng_name_prod(products, product_category_names):
        import pandas as pd
        
        products=products.drop(["product_name_lenght","product_description_lenght","product_photos_qty"],axis=1)
        
        eng_names_prod = products.merge(product_category_names, on="product_category_name")
        
        eng_names_prod = eng_names_prod.drop("product_category_name", axis=1)
        
        return eng_names_prod
        
    @task
    def order_price(orders, order_items):
        import pandas as pd
        
        orders = orders.drop(["order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],axis=1)
        
        order_items = order_items.drop(["order_item_id","seller_id","shipping_limit_date"],axis=1)
        
        orders_price = orders.merge(order_items, on="order_id")
        
        return orders_price
        
        
    @task
    def order_info(order, product_name):
        import pandas as pd
    
        orders_info = order.merge(product_name, on="product_id")
        return orders_info
    
    
    date_fix_result=date_fix(orders=orders.output)
    eng_name_prod_result = eng_name_prod(products=products.output, product_category_names=product_category_names.output)
    order_price_result=order_price(orders=date_fix_result, order_items=order_items.output)
    order_info_result=order_info(order=order_price_result, product_name=eng_name_prod_result)
    
    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_english_product_category_name_result",
        table_name="eng_name_prod_result",
        data=eng_name_prod_result,
    )
    
    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_order_price_result_result",
        table_name="orders_and_prices",
        data=order_price_result,
    )
    
    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_order_info_result_result",
        table_name="order_info_resultrmation",
        data=order_info_result,
    )
    
my_dag = order_info_result_dag()