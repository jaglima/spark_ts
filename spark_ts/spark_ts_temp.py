from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import functions as fun
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

""" 
Summary: compute missing data statistics for the dataframe
INPUT: Receive a dataframe containing tabular data
OUTPUT: Returns a dictionary with key column names and as values the percentual of missing for the columns
"""

def summary(df):
    for k, v in sorted(
        df
        .agg(*[
            fun.round((1 - fun.count(c) / fun.count('*')),2)
            .alias(c + '_miss')
            for c in df.columns
        ])
        .collect()[0]
        .asDict()
        .items()
        , key=lambda el: el[1]
        ,reverse = True
        ):
        print (k, v)


""" 
Summary: Given a Dataframe object with a date column, search for max and minimum date in the range
INPUT: Receive a Dataframe and the name of a datetype column
OUTPUT: Returns the max and minimal dates in the range
""" 
def dateLimits(df, date):
	from pyspark.sql import Row
	from pyspark.sql.functions import from_unixtime, unix_timestamp, min, max

	row = Row("ts")
	df = rdd.map(row).toDF()

	df.withColumn("ts", unix_timestamp("ts")).agg(
	    from_unixtime(min("ts")).alias("min_ts"), 
	    from_unixtime(max("ts")).alias("max_ts")
	).show()


""" 
Summary: Given a Dataframe object with a date column, returns gaps between two consecutive dates for 
	each interval greater then limit
INPUT: Receive a Dataframe, the name of a datetype column and a integer limit
OUTPUT: Returns all the data ranges greater than limit
""" 
def gaps(df, date, limit, minDate, maxDate):
	from pyspark.sql.functions import datediff, to_date, lit

	df.withColumn(date, 
              datediff(to_date(lit("2017-05-02")),
                       to_date("low","yyyy/MM/dd"))).show()


"""
Interpolate missing data from a given column
"""
def interp(col, precision):
    def inner(df):
        window = Window.partitionBy("Estacao").orderBy("Data").rowsBetween(-2, 2)
        df = (df
            .withColumn("interp", fun.avg(df[col]).over(window))
            )
        df = df.withColumn("interp", fun.round(df["interp"], precision))
        df = (df
            .withColumn(col, fun.coalesce(df[col], df["interp"]))
            .drop("interp")
        )
        return df
    return inner


# Outliars treatment
def outliars(col):
    def inner(df):
        # compute monthly average 
        window = Window.partitionBy("Estacao").orderBy("Data").rowsBetween(-30, 0)
        df = df.withColumn("movingAvg", fun.avg(df[col]).over(window))

        # compute monthly stdev
        window = Window.partitionBy("Estacao").orderBy("Data").rowsBetween(-30, 0)
        df = df.withColumn("movingStd", fun.stddev(df[col]).over(window))
        
        df = df.withColumn("maxLimit", df['movingAvg'] + 3*df["movingStd"])
        df = df.withColumn("minLimit", df['movingAvg'] - 3*df["movingStd"])
        
        return df
    return inner



__all__ = ['outliars', 'interp', 'summary']
