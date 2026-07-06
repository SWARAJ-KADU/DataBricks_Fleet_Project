from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp
from transformations._pipeline_utils import bronze_table
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.silver.shipment_invoice_silver",
    comment="Shipment manifests joined with their corresponding invoices.",
)
@dp.expect_or_drop("valid_weight", "total_weight_kg > 0")
@dp.expect_or_drop("valid_package_count", "package_count > 0")
@dp.expect("valid_amount", "amount_inr IS NOT NULL")

def shipment_invoice_silver():
    dfm = spark.readStream.table(bronze_table("shipment_manifests"))
    di = spark.read.table(bronze_table("invoices"))
    return (
        dfm.alias("dfm")
        .join(
            di.alias("di"),
            col("dfm.manifest_id") == col("di.manifest_id"),
            "left"
        )
        .withColumn("is_invoiced", col("di.invoice_id").isNotNull())
        .select(
            "dfm.manifest_id", "dfm.shipment_date", "dfm.vehicle_id", "dfm.route_name",
            "dfm.customer_name", "dfm.destination_city", "dfm.package_count",
            "dfm.total_weight_kg", "dfm.is_cold_chain", "dfm.priority",
            "di.invoice_id", "di.amount_inr", "di.payment_status", "is_invoiced",
        )
    )