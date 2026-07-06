from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")
 
@dp.table(
    name=f"{CATALOG_NAME}.gold.daily_revenue_gold",
    comment="Daily revenue aggregation by route and priority from invoiced shipments.",
)
def daily_revenue_gold():
    return (
        spark.readStream.table("silver.shipment_invoice_silver")
        .filter(col("is_invoiced") == True)
        .groupBy(col("shipment_date"), col("route_name"), col("priority"))
        .agg(
            sum(col("amount_inr")).alias("revenue")
        )
    )