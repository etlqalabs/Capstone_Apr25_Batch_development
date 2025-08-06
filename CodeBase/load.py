from sqlalchemy import create_engine, text
import logging

# Connections (both on same MySQL, different schemas)
mysql_engine_tgt = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode='w',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def load_fact_sales_table():
    # Use fully qualified names for cross-schema insert
    query = text("""
        INSERT INTO retaildwh.fact_sales (sales_id, product_id, store_id, quantity, total_sales, sale_date)
        SELECT sales_id, product_id, store_id, quantity, total_sales, sale_date
        FROM stag_retaildwg.sales_with_details
    """)
    try:
        with mysql_engine_tgt.begin() as conn:  # begin() handles commit/rollback
            logger.info("fact_sales table started loading...")
            logger.info(str(query))
            result = conn.execute(query)
            logger.info(f"Rows inserted: {result.rowcount}")
            logger.info("fact_sales table completed loading...")
    except Exception as e:
        logger.error(f"Error encountered while loading into fact_sales: {e}", exc_info=True)


def load_fact_inventory_table():
    # Use fully qualified names for cross-schema insert
    query = text("""
        INSERT INTO retaildwh.fact_inventory (product_id,store_id,quantity_on_hand,last_updated)
        SELECT product_id,store_id,quantity_on_hand,last_updated
        FROM stag_retaildwg.staging_inventory
    """)
    try:
        with mysql_engine_tgt.begin() as conn:  # begin() handles commit/rollback
            logger.info("fact_inventory table started loading...")
            logger.info(str(query))
            result = conn.execute(query)
            logger.info(f"Rows inserted: {result.rowcount}")
            logger.info("fact_inventory table completed loading...")
    except Exception as e:
        logger.error(f"Error encountered while loading into fact_inventory: {e}", exc_info=True)

def load_monthly_sales_summary_table():
    # Use fully qualified names for cross-schema insert
    query = text("""
        INSERT INTO retaildwh.monthly_sales_summary(product_id,month,year,total_sales)
        SELECT product_id,month,year,total_sales
        FROM stag_retaildwg.monthly_sales_summary_source
    """)
    try:
        with mysql_engine_tgt.begin() as conn:  # begin() handles commit/rollback
            logger.info("monthly_sales_summary table started loading...")
            logger.info(str(query))
            result = conn.execute(query)
            logger.info(f"Rows inserted: {result.rowcount}")
            logger.info("monthly_sales_summary table completed loading...")
    except Exception as e:
        logger.error(f"Error encountered while loading into monthly_sales_summary: {e}", exc_info=True)

def load_inventory_level_by_store_table():
    # Use fully qualified names for cross-schema insert
    query = text("""
        INSERT INTO retaildwh.inventory_levels_by_store(store_id,total_inventory)
        SELECT store_id,total_inventory
        FROM stag_retaildwg.aggregated_inventory_level
    """)
    try:
        with mysql_engine_tgt.begin() as conn:  # begin() handles commit/rollback
            logger.info("inventory_level_by_store table started loading...")
            logger.info(str(query))
            result = conn.execute(query)
            logger.info(f"Rows inserted: {result.rowcount}")
            logger.info("inventory_level_by_store table completed loading...")
    except Exception as e:
        logger.error(f"Error encountered while loading into inventory_level_by_store: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Data Load started ....")
    load_fact_sales_table()
    load_fact_inventory_table()
    load_monthly_sales_summary_table()
    load_inventory_level_by_store_table()
    logger.info("Data Load completed ....")
