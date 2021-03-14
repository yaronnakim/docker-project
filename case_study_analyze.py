
import pyspark as ps
import datetime
import sys
import logging
from pyspark import SQLContext, SparkContext, SparkConf
from pyspark import SparkFiles
from pyspark.sql.functions import col, date_trunc
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField, DateType

# static variables
URL_METADATA = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz'
URL_RATING = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv'
RATING_SCHEMA=  StructType([
    StructField("reviewerID", StringType(), True),
    StructField("asin", IntegerType(), True),
    StructField("overall", StringType(), True),
    StructField("unixReviewTime", IntegerType(), True)])

# ### create the full data frame for analysis
def download_and_create_dataframe():
    spark_context = SparkContext.getOrCreate(SparkConf()) 
    spark_context.addFile(URL_METADATA)
    spark_context.addFile(URL_RATING)
    sql_context = SQLContext.getOrCreate(spark_context)
    #
    # after succefull downloading the CSV and JSON i set the logs to ERROR level only
    # to reduse the many INFO logs
    #
    log4j = spark_context._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    
    ratings_df = sql_context.read.schema(RATING_SCHEMA).csv("file://"+ SparkFiles.get("ratings_Movies_and_TV.csv"), header=False, inferSchema= True)
    metadata_df = sql_context.read.json("file://"+SparkFiles.get("meta_Movies_and_TV.json.gz"))
    return ratings_df.join(metadata_df, on = ['asin']), sql_context

# recive dataFrame, clean the data
# Input: dataFrame
# Outpot: dataFrame

def clean_data(df):
    # add a date column from the unixReviewTime column
    df = df.withColumn('review_date', F.from_unixtime('unixReviewTime').cast(DateType()))
    # remove no null corrupt records
    df = df.filter(df['_corrupt_record'].isNull())
    # filter title with null
    df = df.filter(df['title'].isNotNull())
    # select only Movies , Movies & TV category from the category column
    # use explode to open the array and select only that categories
    df = df.withColumn('explode_categories', F.explode(df['categories']))
    df = df.withColumn('category', F.explode(df['explode_categories']))
    df = df.filter(df['category'].isin('Movies','Movies & TV'))
    # use distinct to remove duplicates
    df = df.drop('explode_categories','category').distinct()
    
    return df

# function to recive 1 month dataframe 
# Input: dataFrame , date
# Outpot: dataFrame in the corresponding date
def get_one_month(df,date):
    start = datetime.date(date.year, date.month,1)
    end   = start + datetime.timedelta(days = 31)
    end   = datetime.date(end.year, end.month,1)
    
    return df.filter(F.col('review_date').between(start,end))

# function to recive 2 following month dataframe 
# Input: dataFrame , date
# Outpot: dataFrame in the corresponding date
def get_two_following_months(df,date):
    start = datetime.date(date.year, date.month,1)
    end   = start + datetime.timedelta(days = 62)
    end   = datetime.date(end.year, end.month,1)
    
    return df.filter(F.col('review_date').between(start,end))

# function for anelize and present the first question

def top_bottom(df,date):
    df  = get_one_month(df,date)
  
    df = df.groupby('asin','title').agg(
        F.avg(F.col('overall')).alias('avg_rating'),
        F.count(F.col('overall')).alias('count_rating')
    ) 
    top_5_ratings = df.orderBy(F.col('avg_rating').desc(),F.col('count_rating').desc()).limit(5)
    buttom_5_ratings = df.orderBy(F.col('avg_rating').asc(),F.col('count_rating').desc()).limit(5)

    top_5_ratings.show(5,False) 
    buttom_5_ratings.show(5,False)
            
# function for anelize and present the second question
def max_5_avg_diff(df,date,sql_context):
    # calcuate the necessary dates for creating two dataframes
    start = datetime.date(date.year, date.month,1)
    previous_month = start - datetime.timedelta(days = 1)
    start_previous_month = datetime.date(previous_month.year, previous_month.month, 1)
    date_next_month = start + datetime.timedelta(days = 31)
    end_date = datetime.date(date_next_month.year, date_next_month.month,1)
    df_1 = get_one_month(df,date)
    df_2 = get_one_month(df,start_previous_month)
    two_month_df = df_1.union(df_2)
    # create a temp view for SQL analysis 
    two_month_df.createOrReplaceTempView('df')    
    result_2 = sql_context.sql(f"""
    with previous_month as 
    (
    SELECT asin,title, avg(overall) as avg_rating_previous_month
    from df
    where review_date between '{previous_month.strftime('%Y-%m-%d')}' and '{start.strftime('%Y-%m-%d')}'
    group by asin,title
    ),
    current_month as
    (
    SELECT asin,title, avg(overall) as avg_rating_current_month
    from df
    where review_date between '{start.strftime('%Y-%m-%d')}' and '{end_date.strftime('%Y-%m-%d')}'
    group by asin,title
    )
    SELECT c.asin , c.title, abs(c.avg_rating_current_month - p.avg_rating_previous_month) as max_avg_difference
        , p.avg_rating_previous_month, c.avg_rating_current_month
    FROM previous_month as p join current_month as c
        on p.asin=c.asin and p.title=c.title
    WHERE c.avg_rating_current_month > p.avg_rating_previous_month
    order by max_avg_difference desc
    LIMIT 5
    """)

    result_2.show(5,False)
    
# # main function 
# Input: date string
# Output: 3 tables: for the given month
#         1 - top 5 avg movies reviews 
#         2 - bottom 5 avg movies reviews 
#         3 - 5 movies whose average monthly ratings increased the most compared with the previous month
def main(argv):
    date = datetime.datetime.strptime(argv[1], '%Y-%m-%d').date()
    df,sql_context = download_and_create_dataframe()
    df = clean_data(df)   
    top_bottom(df,date)   
    max_5_avg_diff(df,date,sql_context)

if __name__ == '__main__':
    main(sys.argv)