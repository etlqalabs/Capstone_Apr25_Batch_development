import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging

oracle_engine = create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
mysql_engine_stag = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/stag_retaildwg")


logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)


def transform_filter_sales_data():
    try:
        logger.info("transform_filter_sales_data started....")
        query = """select * from staging_sales where sale_date>='2024-09-10'"""
        df = pd.read_sql(query,mysql_engine_stag)
        df.to_sql("filtered_sales_data",mysql_engine_stag,if_exists='replace',index=False)
        logger.info("transform_filter_sales_data completed....")
    except Exception as e:
        logger.error("Error encounted while transform_filter_sales_data,ex",e,exc_info=True)

def transform_router_sales_data():
    try:
        logger.info("transform_router_sales_data started....")
        query_low = """select * from filtered_sales_data where region = 'Low'"""
        query_high = """select * from filtered_sales_data where region = 'High'"""
        df_low = pd.read_sql(query_low,mysql_engine_stag)
        df_high = pd.read_sql(query_high, mysql_engine_stag)
        df_low.to_sql("low_sales",mysql_engine_stag,if_exists='replace',index=False)
        df_high.to_sql("high_sales", mysql_engine_stag, if_exists='replace', index=False)
        logger.info("transform_router_sales_data completed....")
    except Exception as e:
        logger.error("Error encounted while transform_router_sales_data,ex",e,exc_info=True)

def transform_aggregator_sales_data():
    try:
        logger.info("transform_aggregator_sales_data started....")
        query = """select f.product_id,month(f.sale_date) as month ,year(f.sale_date) as year, sum(f.price*f.quantity) as total_sales
                    from filtered_sales_data as f group by f.product_id,month(f.sale_date),year(f.sale_date)
                    order by product_id;"""
        df = pd.read_sql(query,mysql_engine_stag)
        df.to_sql("monthly_sales_summary_source",mysql_engine_stag,if_exists='replace',index=False)
        logger.info("transform_aggregator_sales_data completed....")
    except Exception as e:
        logger.error("Error encounted while transform_aggregator_sales_data,ex",e,exc_info=True)

def transform_aggregator_inventory_data():
    try:
        logger.info("transform_aggregator_inventory_data started....")
        query = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id"""
        df = pd.read_sql(query,mysql_engine_stag)
        df.to_sql("aggregated_inventory_level",mysql_engine_stag,if_exists='replace',index=False)
        logger.info("transform_aggregator_inventory_data completed....")
    except Exception as e:
        logger.error("Error encounted while transform_aggregator_inventory_data,ex",e,exc_info=True)

def transform_joiner_sales_products_stores_data():
    try:
        logger.info("transform_joiner_sales_products_stores_data started....")
        query = """select fs.sales_id,fs.sale_date,fs.price,fs.quantity,fs.price*fs.quantity as total_sales,
                    p.product_id,p.product_name,s.store_id,s.store_name
                     from filtered_sales_data as fs
                    inner join staging_product as p on fs.product_id = p.product_id
                    inner join staging_stores as s on s.store_id = fs.store_id"""
        df = pd.read_sql(query,mysql_engine_stag)
        df.to_sql("sales_with_details",mysql_engine_stag,if_exists='replace',index=False)
        logger.info("transform_joiner_sales_products_stores_data completed....")
    except Exception as e:
        logger.error("Error encounted while transform_joiner_sales_products_stores_data,ex",e,exc_info=True)






if __name__ == "__main__":
    logger.info("Data Transformation started ....")
    transform_filter_sales_data()
    transform_router_sales_data()
    transform_aggregator_sales_data()
    transform_aggregator_inventory_data()
    transform_joiner_sales_products_stores_data()
    logger.info("Data Transformation finished ....")



