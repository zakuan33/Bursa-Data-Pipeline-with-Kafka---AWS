import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Script generated for node Gold Henshin
def GoldTranform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import regexp_extract, col, regexp_replace
    from awsglue.dynamicframe import DynamicFrame

    # Select the first (or specific) DynamicFrame from the collection
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

    # Gold-level transformations
    gold_df = (
        df.withColumn("BUY_PRICE", regexp_extract(col("BUY (QTY)"), r"([\d.]+)", 1).cast("double"))
          .withColumn("BUY_QTY", regexp_extract(col("BUY (QTY)"), r"\((\d+)\)", 1).cast("int"))
          .withColumn("SELL_PRICE", regexp_extract(col("SELL (QTY)"), r"([\d.]+)", 1).cast("double"))
          .withColumn("SELL_QTY", regexp_extract(col("SELL (QTY)"), r"\((\d+)\)", 1).cast("int"))
          .withColumn("SPREAD", (col("HIGH") - col("LOW")).cast("double"))
          .withColumn("DELTA", (col("PRICE") - col("CLOSE")).cast("double"))
          .withColumn("PCT_CHANGE", regexp_replace(col("%chg"), "%", "").cast("double"))
          .withColumn("CASHTAG", regexp_replace(col("cashtag"), "\\$", ""))
          # use .select() to explicitly define the final schema
          .select(
              "CASHTAG",
              "NAME",
              "PRICE",
              "CHG",
              "VOLUME",
              "BUY_PRICE",
              "BUY_QTY",
              "SELL_PRICE",
              "SELL_QTY",
              "SPREAD",
              "DELTA",
              "PCT_CHANGE"
          )
    )

    # Convert back to DynamicFrame
    gold_dyf = DynamicFrame.fromDF(gold_df, glueContext, "gold_dyf")

    # Glue Studio expects a DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": gold_dyf}, glueContext)
# Script generated for node Silver Henshin
def STransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import regexp_replace, col
    from awsglue.dynamicframe import DynamicFrame

    # Get your main DataFrame from the input DynamicFrameCollection
    dyf = dfc.select(list(dfc.keys())[0])  # selects the first input node
    spark_df = dyf.toDF()

    # Clean %chg and normalize cashtag
    curated_df = spark_df \
        .withColumn("PCT_CHANGE", regexp_replace(col("%chg"), "%", "").cast("double")) \
        .withColumn("CASHTAG", regexp_replace(col("cashtag"), "\\$", "")) \
        .drop("%chg") 

    # Convert back to DynamicFrame
    curated_dyf = DynamicFrame.fromDF(curated_df, glueContext, "curated_dyf")

    # Return as DynamicFrameCollection
    return DynamicFrameCollection({"CustomTransform0": curated_dyf}, glueContext)
    #return curated_dyf
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1752564905317 = glueContext.create_dynamic_frame.from_catalog(database="bursa_market_share", table_name="bursa_raw", transformation_ctx="AWSGlueDataCatalog_node1752564905317")

# Script generated for node Drop Duplicates
DropDuplicates_node1752564928165 =  DynamicFrame.fromDF(AWSGlueDataCatalog_node1752564905317.toDF().dropDuplicates(["cashtag"]), glueContext, "DropDuplicates_node1752564928165")

# Script generated for node Silver Henshin
SilverHenshin_node1752565328512 = STransform(glueContext, DynamicFrameCollection({"DropDuplicates_node1752564928165": DropDuplicates_node1752564928165}, glueContext))

# Script generated for node Gold Henshin
GoldHenshin_node1752570736538 = GoldTranform(glueContext, DynamicFrameCollection({"DropDuplicates_node1752564928165": DropDuplicates_node1752564928165}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1752568566239 = SelectFromCollection.apply(dfc=SilverHenshin_node1752565328512, key=list(SilverHenshin_node1752565328512.keys())[0], transformation_ctx="SelectFromCollection_node1752568566239")

# Script generated for node Select From Collection
SelectFromCollection_node1752634095735 = SelectFromCollection.apply(dfc=GoldHenshin_node1752570736538, key=list(GoldHenshin_node1752570736538.keys())[0], transformation_ctx="SelectFromCollection_node1752634095735")

# Script generated for node bursa-curated(silver)
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1752568566239, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752564878691", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
bursacuratedsilver_node1752568621505 = glueContext.getSink(path="s3://bursa-curated", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="bursacuratedsilver_node1752568621505")
bursacuratedsilver_node1752568621505.setCatalogInfo(catalogDatabase="bursa_market_share",catalogTableName="bursa_silver")
bursacuratedsilver_node1752568621505.setFormat("json")
bursacuratedsilver_node1752568621505.writeFrame(SelectFromCollection_node1752568566239)
# Script generated for node bursa-transform(gold)
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1752634095735, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752629171496", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
bursatransformgold_node1752634680708 = glueContext.getSink(path="s3://bursa-transformed", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="bursatransformgold_node1752634680708")
bursatransformgold_node1752634680708.setCatalogInfo(catalogDatabase="bursa_market_share",catalogTableName="bursa_gold")
bursatransformgold_node1752634680708.setFormat("json")
bursatransformgold_node1752634680708.writeFrame(SelectFromCollection_node1752634095735)
job.commit()