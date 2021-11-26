import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import to_date, col, from_unixtime, unix_timestamp


class AwsGlueJobBuilder(object):

    _DB_NAME = "ny_bikes"
    _BIKE_TRIP_TABLE_NAME = "glue_ny_bikes_bike_trip"
    _WEATHER_REPORT_TABLE_NAME = "glue_ny_bikes_weather_report"

    _OUTPUT_BUCKET = "s3://ny-bikes-glue"

    def __init__(self, options):
        self._spark_context = SparkContext()
        self._glue_context = GlueContext(self._spark_context)
        self._spark = self._glue_context.spark_session
        self._glue_job = Job(self._glue_context)
        self._options = options

    def build(self):
        self._glue_job.init(self._options["JOB_NAME"], self._options)

        bike_trip_with_weather_df = Join.apply(
            frame1=self._build_bike_trip_df(),
            frame2=self._build_weather_report_df(),
            keys1=["started_date"],
            keys2=["date_only"]
        )
        self._glue_context.write_dynamic_frame.from_options(
            frame=bike_trip_with_weather_df,
            connection_type="s3",
            connection_options={"path": self._OUTPUT_BUCKET},
            format="parquet"
        )

        return self._glue_job

    def _build_bike_trip_df(self):
        df = BikeTripDataFrameBuilder.build(self._glue_context, self._DB_NAME, self._BIKE_TRIP_TABLE_NAME)
        df = DynamicFrame.fromDF(df, self._glue_context, "bike_trip_df")

        return df

    def _build_weather_report_df(self):
        df = WeatherReportDataFrameBuilder.build(self._glue_context, self._DB_NAME, self._WEATHER_REPORT_TABLE_NAME)
        df = DynamicFrame.fromDF(df, self._glue_context, "weather_report_df")

        return df


class DataFrameNormalizer(object):

    def __init__(self, function):
        self._function = function
        self._normalizers = []

    def __call__(self, *args, **kwargs):
        res = self._function(*args, **kwargs)
        for normalizer in self._normalizers:
            res = normalizer(res)

        return res


class BikeTripDateFrameNormalizer(DataFrameNormalizer):

    def __init__(self, function):
        super(BikeTripDateFrameNormalizer, self).__init__(function)

        self._normalizers = [
            DataFrameNullCleaner(["started_at"]),
            DataFrameDuplicatesCleaner(["ride_id"]),
            DataFrameDateTimeToDateNormalizer("started_at", "started_date")
        ]


class WeatherReportDataFrameNormalizer(DataFrameNormalizer):

    def __init__(self, function):
        super(WeatherReportDataFrameNormalizer, self).__init__(function)

        self._normalizers = [
            DataFrameNullCleaner(["date_recorded"]),
            DataFrameDuplicatesCleaner(["station_id", "date_recorded"]),
            DataFrameDateTimeToDateNormalizer("date_recorded", "date_only")
        ]


class DataFrameNullCleaner(object):

    def __init__(self, fields_to_clean=None):
        """
        :param [str] fields_to_clean:
        """
        self._fields_to_clean = [] if fields_to_clean is None else fields_to_clean

    def __call__(self, df):
        for field in self._fields_to_clean:
            df = df.where(f"`{field}` IS NOT NULL")

        return df


class DataFrameDuplicatesCleaner(object):

    def __init__(self, composite_key=None):
        """
        :param [str] composite_key:
        """
        self._composite_key = [] if composite_key is None else composite_key

    def __call__(self, df):
        df = df.dropDuplicates(self._composite_key)
        return df


class DataFrameDateTimeToDateNormalizer(object):

    def __init__(self, datetime_column_name, new_date_column_name="new_date_column"):
        self._datetime_column_name = datetime_column_name
        self._new_date_column_name = new_date_column_name

    def __call__(self, df):
        df = df.withColumn(self._new_date_column_name, to_date(col(self._datetime_column_name)))
        return df


class BikeTripDataFrameBuilder(object):

    @staticmethod
    @BikeTripDateFrameNormalizer
    def build(glue_context, db_name, table_name):
        return glue_context\
            .create_dynamic_frame\
            .from_catalog(database=db_name, table_name=table_name)\
            .toDF()


class WeatherReportDataFrameBuilder(object):

    @staticmethod
    @WeatherReportDataFrameNormalizer
    def build(glue_context, db_name, table_name):
        return glue_context\
            .create_dynamic_frame\
            .from_catalog(database=db_name, table_name=table_name)\
            .toDF()


job_builder = AwsGlueJobBuilder(getResolvedOptions(sys.argv, ['JOB_NAME']))
job = job_builder.build()
job.commit()
