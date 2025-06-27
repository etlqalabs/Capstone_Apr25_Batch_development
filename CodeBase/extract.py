import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging

oracle_engine = create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
mysql_engine = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/stag_retaildwg")

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode = 'a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)


def extract_sales_data_from_file():
    try:
        logger.info("sales data extraction started....")
        df = pd.read_csv("SourceSystems/sales_data_Linux_remote.csv")
        df.to_sql("staging_sales",mysql_engine,if_exists='replace',index=False)
        logger.info("sales data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the sales data file,{e}",exc_info=True)



def extract_product_data_from_file():
    try:
        logger.info("product data extraction started....")
        df = pd.read_csv("SourceSystems/product_data.csv")
        df.to_sql("staging_product",mysql_engine,if_exists='replace',index=False)
        logger.info("product data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the product data file,{e}",exc_info=True)


def extract_supplier_data_from_file():
    try:
        logger.info("supplier data extraction started....")
        df = pd.read_json("SourceSystems/supplier_data.json")
        df.to_sql("staging_supplier",mysql_engine,if_exists='replace',index=False)
        logger.info("supplier data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the supplier data file,{e}",exc_info=True)

def extract_inventory_data_from_file():
    try:
        logger.info("inventory data extraction started....")
        df = pd.read_xml("SourceSystems/inventory_data.xml",xpath=".//item")
        df.to_sql("staging_inventory",mysql_engine,if_exists='replace',index=False)
        logger.info("inventory data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the inventory data file,{e}",exc_info=True)

def extract_stores_data_from_oracle():
    try:
        logger.info("stores data extraction started....")
        query = """select * from stores"""
        df = pd.read_sql(query,oracle_engine)
        df.to_sql("staging_stores",mysql_engine,if_exists='replace',index=False)
        logger.info("stores data extraction completed....")
    except Exception as e:
        logger.error(f"Error encounted while extrcting the stores data from oracle,{e}",exc_info=True)




if __name__ == "__main__":
    extract_sales_data_from_file()
    extract_product_data_from_file()
    extract_supplier_data_from_file()
    extract_inventory_data_from_file()
    extract_stores_data_from_oracle()


