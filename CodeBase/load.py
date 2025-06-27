from sqlalchemy import create_engine, text
import logging

# Connections (both on same MySQL server, different schemas)
mysql_engine_tgt = create_engine("mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh")

logging.basicConfig(
    filename='LogFiles/Extraction.log',
    filemode='a',
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

if __name__ == "__main__":
    logger.info("Data Load started ....")
    load_fact_sales_table()
    logger.info("Data Load completed ....")
