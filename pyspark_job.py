from pyspark.sql import SparkSession, types
from pyspark.sql.functions import pandas_udf, broadcast, round
import pandas as pd

spark = SparkSession \
          .builder \
          .master('local[*]') \
          .appName('test') \
          .getOrCreate()

# process measurements data
measurements_schema = types.StructType([
  types.StructField('day', types.DateType(), nullable=False),
  types.StructField('interval', types.IntegerType(), nullable=False),
  types.StructField('detid', types.StringType(), nullable=False),
  types.StructField('flow', types.IntegerType(), nullable=True),
  types.StructField('occ', types.FloatType(), nullable=True),
  types.StructField('error', types.IntegerType(), nullable=True),
  types.StructField('city', types.StringType(), nullable=False),
  types.StructField('speed', types.FloatType(), nullable=True)
])

@pandas_udf(types.IntegerType())
def get_hour(intervals: pd.Series) -> pd.Series:
  return intervals.floordiv(60 ** 2)

# read in the raw data
measurements_df = spark.read \
                    .option("header", True) \
                    .schema(measurements_schema) \
                    .csv('./data/measurements_test.csv')

# engineer variables
measurements_df = measurements_df \
                    .repartition(20, 'day', 'city') \
                    .withColumn('hour', get_hour('interval')) \
                    .withColumn('density', round(measurements_df.occ * 100)) \
                    .drop('interval', 'occ')

# convert to hourly average measurements
hrly_measurements_df = measurements_df \
  .groupBy(['city', 'detid', 'day', 'hour']) \
  .agg({
    'flow': 'mean',
    'density': 'mean',
    'speed': 'mean',
    'error': 'array_agg'
  })

hrly_measurements_df = hrly_measurements_df.withColumnsRenamed(
  {
    'collect_list(error)': 'errors',
    'avg(flow)': 'flow',
    'avg(density)': 'density',
    'avg(speed)': 'speed'
  }
)

# process detectors data
detectors_schema = types.StructType([
  types.StructField('detid', types.StringType()),
  types.StructField('length', types.DoubleType()),
  types.StructField('pos', types.DoubleType()),
  types.StructField('fclass', types.StringType()),
  types.StructField('road', types.StringType()),
  types.StructField('limit', types.IntegerType()),
  types.StructField('citycode', types.StringType()),
  types.StructField('lanes', types.IntegerType()),
  types.StructField('linkid', types.IntegerType()),
  types.StructField('long', types.DoubleType()),
  types.StructField('lat', types.DoubleType())
])

# read in the raw data
detectors_df = spark.read \
                .option("header", True) \
                .schema(detectors_schema) \
                .csv('./data/detectors_public.csv')

# clean column names
detectors_df = detectors_df \
                .drop('linkid', 'pos', 'length') \
                .withColumnRenamed('citycode', 'city')

# merge measurements and detectors datasets
hrly_df = hrly_measurements_df.join(broadcast(detectors_df), on=['detid', 'city'])


# write the augmented data to parqet
hrly_df.write.parquet('./data/pq/measurements.csv', mode='overwrite')

spark.stop()


