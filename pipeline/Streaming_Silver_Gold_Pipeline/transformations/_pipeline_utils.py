# _pipeline_utils.py
#
# Shared constants and small helper functions used by every transformation
# file in this Declarative Pipeline. Kept separate from the actual table
# definitions so each transformation file stays focused on its own logic.
#
# NOTE: this file does NOT define any @dp.table -- it's a plain Python
# module imported by the others.
#
# ENVIRONMENT AWARENESS:
# Nothing in this file (or any other file in this pipeline) hardcodes
# "fleet_dev". Instead, the catalog name is read from a pipeline-level
# configuration value at runtime via spark.conf.get(...). This is what lets
# the EXACT SAME CODE be deployed to fleet_dev, fleet_uat, or fleet_prod
# without a single line changing -- only the configuration value injected
# by the pipeline differs per environment.
#
# Where "bronze_catalog" comes from:
#   - Manually, today: set under Pipeline Settings > Configuration in the
#     Databricks UI as a key-value pair: bronze_catalog = fleet_dev
#   - Later (Phase 7), automatically: injected by the Databricks Asset
#     Bundle's per-environment target block (dev.yml / uat.yml / prod.yml),
#     so CI/CD promotion requires zero manual UI edits between environments.

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

BRONZE_CATALOG = spark.conf.get("CATALOG_NAME")


def _get_config(key: str, default: str) -> str:
    """
    Reads a pipeline configuration value, falling back to `default` if it
    isn't set (e.g. during local syntax testing outside a real pipeline run).
    """
    try:
        return spark.conf.get(key)
    except Exception:
        return default


# The catalog this pipeline run targets. Set via Pipeline Settings >
# Configuration as "bronze_catalog" and "target_catalog" -- defaults to
# fleet_dev so the file still behaves sensibly if those configs are unset.

BRONZE_SCHEMA = "bronze"

# The catalog Silver/Gold tables get WRITTEN to. In a Lakeflow Declarative
# Pipeline this is normally just set as the pipeline's own "Catalog" field
# in settings (so @dp.table doesn't need to specify it) -- we still expose
# it here as a constant in case any file needs to reference it explicitly
# (e.g. for a fully-qualified cross-reference).


def bronze_table(name: str) -> str:
    """Fully qualified Bronze table name, e.g. bronze_table('gps_events')
    -> '<bronze_catalog>.bronze.gps_events', where <bronze_catalog> resolves
    to fleet_dev / fleet_uat / fleet_prod depending on which environment
    this pipeline run was deployed to."""
    return f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{name}"


# Physically reasonable bounds, used by multiple data-quality expectations.
# Pulled from the same real-world ranges used in the data generator
# (data_generator/reference_data.py) so Bronze->Silver validation is
# consistent with how the data was actually generated.
# These are NOT environment-specific -- a valid GPS coordinate is valid
# regardless of whether you're in DEV, UAT, or PROD, so they stay as plain
# constants rather than pipeline configuration values.
VALID_LAT_RANGE = (8.0, 37.0)     # mainland India latitude bounds (generous)
VALID_LON_RANGE = (68.0, 97.0)    # mainland India longitude bounds (generous)
VALID_ENGINE_TEMP_RANGE = (-20.0, 150.0)   # physically plausible sensor range
                                            # (note: this is wider than the
                                            # "normal" 80-95C range -- anomalies
                                            # like overheating are VALID data we
                                            # want to keep, not quarantine; this
                                            # bound only catches impossible/
                                            # garbage sensor readings)
VALID_FUEL_RANGE = (0.0, 200.0)            # litres; truck tanks are 120-150L,
                                            # generous upper bound for safety
VALID_CARGO_TEMP_RANGE = (-30.0, 50.0)

DELIVERY_STATE_ORDER = {
    "created": 0,
    "picked_up": 1,
    "in_transit": 2,
    "out_for_delivery": 3,
    "delivered": 4,
}
