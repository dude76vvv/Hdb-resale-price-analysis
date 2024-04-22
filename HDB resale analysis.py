# Databricks notebook source
# read csv from
salesDf = spark.read.option('header','true').csv('/FileStore/tables/ResaleflatpricesSg.csv',inferSchema=True)
salesDf.show()

# COMMAND ----------

salesDf.printSchema()

# COMMAND ----------

# get total records 
salesDf.count()

# COMMAND ----------

#alternative way displaying data using pandas
salesDf.limit(5).toPandas()

# COMMAND ----------

#inspect for empty value counts for each cols
from pyspark.sql.functions import isnan, when, count, col
dfNull = salesDf.select([count( when (col(x).isNull(),x ) ).alias(x) for x in salesDf.columns])
dfNull.show()

# COMMAND ----------

#inspect nan for number cols
colLis = ['floor_area_sqm','lease_commence_date','resale_price']

dfNan = salesDf.select([count( when (col(x).isNull(),x ) ).alias(x) for x in colLis])
dfNan.show()

# COMMAND ----------

salesDf.select('remaining_lease').display()

# COMMAND ----------

from pyspark.sql.functions import udf

#handle whern there is months and month
#thee will be years tho
def getTotalMonths(s1):

    yrs=0
    mth=0
    lis:list = s1.split()

    if 'month' in s1  or 'months' in s1:
        mth = int(lis[-2])
    
    yrs = int(lis[0])

    #get total months

    return yrs*12 + mth





# COMMAND ----------

#convert method to udf , to be use
from pyspark.sql.types import IntegerType

getTotalMonths_udf = udf(getTotalMonths,IntegerType())

# COMMAND ----------

#test the columns udf function on the remaining lease

leaseDf = salesDf.select('remaining_lease',getTotalMonths_udf('remaining_lease').alias('remaining_Lease_months'))

leaseDf.show(1000)


# COMMAND ----------

#get the years from the date columm

from pyspark.sql.functions import year
dateDf = salesDf.select('month',year('month').alias('year'))
dateDf.display()

# COMMAND ----------

#add new col to the dataframe
df = salesDf.withColumn('remaining_Lease_months',getTotalMonths_udf('remaining_lease'))
df = df.withColumn('year',year('month'))

df.show(10)



# COMMAND ----------

df.tail(1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Top 10 highest hdb resale transaction
# highest resale-price
df.orderBy('resale_price',ascending=[False]).limit(10).display() 

# COMMAND ----------

# DBTITLE 1,Highest Resale transactioin
df.orderBy('resale_price',ascending=[False]).limit(1).display() 

# COMMAND ----------

#using temp view sql query to do the above task

df.createOrReplaceTempView("tbl")

res0=spark.sql("select * from tbl order by resale_price desc limit 10")
# spark.catalog.dropTempView("tbl")
res0.show()





# COMMAND ----------

# DBTITLE 1,Average Resale Price for each town
#get the average reasale for each town area

res1=spark.sql("select town,avg(resale_price) as avg_resale_price from tbl group by town order by avg_resale_price desc")
res1.display()


# COMMAND ----------

# DBTITLE 1,Average flat type price
res1b=spark.sql(" select flat_type, avg(resale_price) as flat_type_avgPrice from tbl group by flat_type")
res1b.display()

# COMMAND ----------

# DBTITLE 1,Highest resale price for each town
res2=spark.sql("select town,max(resale_price) as max_resale_price from tbl group by town order by max_resale_price desc")
res2.display()

# COMMAND ----------

# DBTITLE 1,Average Resale Price over years
#find the avg resale price over the years

res3=spark.sql("select year,avg(resale_price) as avg_resale_price from tbl group by year order by avg_resale_price desc")
res3.display()



# COMMAND ----------

# DBTITLE 1,Total transactions for each town over the years
#get total transaction for each area

res4=spark.sql("select town,count(*) as total_transaction from tbl group by town order by total_transaction desc")
res4.display()







# COMMAND ----------

# DBTITLE 1,Transaction Count for each town and flat type
#get total transaction for each type

res5=spark.sql("select town,flat_type,count(*) as transactionCount from tbl group by town,flat_type order by transactionCount desc")
res5.display()


# COMMAND ----------


