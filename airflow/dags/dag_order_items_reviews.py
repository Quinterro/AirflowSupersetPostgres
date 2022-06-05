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

def order_items_reviews_dag():

    reviews = PostgresToDataFrameOperator(
        table_name="order_reviews",
        task_id="extract_order_reviews_dataset",
    )
    
    order_items = PostgresToDataFrameOperator(
        table_name="order_items",
        task_id="extract_order_items_dataset",
    )
    
    
    
    @task
    def date_fix(reviews):
        import pandas as pd

        reviews.review_creation_date = pd.to_datetime(
            reviews.review_creation_date
        )
        reviews.review_answer_timestamp = pd.to_datetime(
            reviews.review_answer_timestamp
        )        
        return reviews     
     
    @task
    def order_items_2(order_items):
        import pandas as pd
        import numpy as np
        
        orders_items = order_items.drop(["order_item_id","shipping_limit_date"],axis=1)
        return orders_items

    @task
    def merge(order_items,reviews):
        orderitems_reviews = reviews.merge(order_items, on="order_id")
        
        return orderitems_reviews

    date_fix_result = date_fix(reviews.output)
    
    order_items_2_result=order_items_2(order_items.output)
    
    merge_result = merge(
        reviews = date_fix_result,
        order_items = order_items_2_result,
    )
    
    
    
    load = DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres",
        table_name="order_items_reviews",
        data=merge_result,
    )
    
my_dag = order_items_reviews_dag()