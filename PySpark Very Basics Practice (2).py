# Databricks notebook source
# MAGIC %md
# MAGIC Pyspark is an interface for Apache Spark in Python for development of code using Python API.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, round

# COMMAND ----------

# MAGIC %md # 1.Pyspark help

# COMMAND ----------

# DBTITLE 1,Pyspark help
# help(spark.read)
# help(df10.write)
help(spark.createDataFrame)

# COMMAND ----------

# delta table creation

from delta.tables import *

DeltaTable.createIfNotExists(spark)\
  .tableName("employee")\
  .addColumn("id","INT")\
  .addColumn("name","STRING")\
  .addColumn("age","INT")\
  .property("description","employee table")\
  .location("/FileStore/tables/delta/employee")\
  .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo_new(
# MAGIC   id int,
# MAGIC   name char(100),
# MAGIC   age int
# MAGIC ) using delta
# MAGIC location '/FileStore/tables/delta/demo_new'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize demo_new

# COMMAND ----------

# create softlink for delta table for running pyspark commands on it
dfInstance = DeltaTable.forPath(spark,'/FileStore/tables/delta/demo_new')
display(dfInstance.toDF())

# dfInstance.delete("id=2")
# dfInstance.delete("id=2 and age=32")
# dfInstance.update(condition=col('id')==1, set={'age':'35'})

# display(dfInstance.toDF())

display(dfInstance.history())
# dfInstance.optimize().executeCompaction()

# COMMAND ----------

# DBTITLE 1,vacuum command
# MAGIC %sql
# MAGIC vacuum demo_new dry run;
# MAGIC vacuum demo_new retain 0 dry run;
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=False;
# MAGIC vacuum demo_new;
# MAGIC optimize demo_new zorder id;
# MAGIC

# COMMAND ----------

# DBTITLE 1,schema evolution
df.write.format('delta').option('mergeSchema', True).mode('append').saveAsTable('demo_new') # in order to merge schema
df.write.format('delta').option('overwriteSchema', True).mode('append').saveAsTable('demo_new') # in order to overwrite schema

# COMMAND ----------

# DBTITLE 1,Time travel using timestamp
df = spark.read.format('delta').option('timestampAsOf', '2024-10-01T14:19:00.000+00:00').table('demo_new')
df2 = spark.read.format('delta').option('versionAsOf', 2).table('demo_new')
display(df)
display(df2)

# COMMAND ----------

# DBTITLE 1,Restore the version of delta table
restoreDelta = DeltaTable.forPath(spark, '/FileStore/tables/delta/demo_new')
# restoreDelta.restoreToVersion(2)
# restoreDelta.restoreToTimeStamp('2024-10-01T14:19:00.000+00:00')

display(restoreDelta.toDF())
display(restoreDelta.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history demo_new

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo_new values (1, 'Dhrubo', 34)

# COMMAND ----------

# Create Delta Table:
# df.write.format("delta").save("/path/to/delta-table")
# Creates a Delta table by writing DataFrame data in Delta format.

# Read Delta Table:
# spark.read.format("delta").load("/path/to/delta-table")
# Reads data from a Delta table.

# Update Delta Table:
# deltaTable.update(condition, { "column": "new_value" })
# Updates rows in the Delta table based on a condition.

# Merge Data:
# deltaTable.merge(source, condition).whenMatchedUpdate().whenNotMatchedInsert()
# Merges new data into the Delta table, updating or inserting based on a condition.

# Delete Data:
# deltaTable.delete(condition)
# Deletes rows from the Delta table that meet the condition.

# Optimize Table:
# spark.sql("OPTIMIZE delta./path/to/table")
# Optimizes the Delta table by compacting files.

# Vacuum Table:
# deltaTable.vacuum(retentionHours=168)
# Removes old data files that are no longer needed.

# Time Travel:
# spark.read.format("delta").option("versionAsOf", 1).load("/path/to/table")
# Reads a Delta table from a specific version or timestamp.

# Convert to Delta:
# spark.sql("CONVERT TO DELTA parquet./path/to/parquet-table")
# Converts an existing Parquet table to a Delta table.

# Describe History:
# spark.sql("DESCRIBE HISTORY delta./path/to/table")
# Shows the history of changes made to a Delta table.

# COMMAND ----------

# MAGIC %md #2.Create data manually with hard coded values

# COMMAND ----------

# DBTITLE 1,Create data manually with hard coded values
data = [(1, 'Maheer'), (2,'Wafa')]
schema = ['id','name']
df1 = spark.createDataFrame(data, schema)
df1.show()

# If data is in dictionary format, we donot need to pas schema
data = [{'id':1,'name':'Maheer'}, {'id':2,'name':'Wafa'}]
df2 = spark.createDataFrame(data)
df2.show()
df2.printSchema()

data = [{'id':1,'name':'Maheer'}, {'id':2,'name':'Wafa'}]
schema = StructType([StructField(name='id', dataType=IntegerType()),
                     StructField(name='name', dataType=StringType())])
df3 = spark.createDataFrame(data, schema)
df3.show()
df3.printSchema()

# COMMAND ----------

# DBTITLE 1,Explain plan
# see the physical, logical plan etc modes = extended,simple,formatted,cost

df1.explain('extended')

# COMMAND ----------

# If you need help regarding createDataFrame function
help(spark.createDataFrame)

# COMMAND ----------

# MAGIC %md
# MAGIC #3.Read csv file into Dataframe using Pyspark

# COMMAND ----------

# DBTITLE 1,Read csv file into Dataframe using Pyspark
# read csv without header
df4 = spark.read.format('csv').load('/FileStore/resources/employees.csv')
df4.printSchema()
df4.show()

# read csv with header
df5 = spark.read.option('header', True).option('schema',df4.printSchema()).csv('/FileStore/resources/employees.csv')
df5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #4.Write dataframe using Pyspark

# COMMAND ----------

# DBTITLE 1,Write dataframe using Pyspark
df1.write.option('header', True).csv('/FileStore/resources/df1.csv')
df1.write.options(header='True', delimiter=',').csv('/FileStore/resources/df1_copy.csv')
df1.write.csv(path='dbfs:/FileStore/resources/df1_copy2.csv', header=True, mode='overwrite')

# mode: overwrite(overwrites existing data), append(appends to existing file), ignore(ignores write when file exists), error(default option when file already exists, it returns an error)
df2.write.format('csv').mode('overwrite').save('/FileStore/resources/df2.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #5.Read JSON into dataframe using Pyspark

# COMMAND ----------

# DBTITLE 1,Read JSON into dataframe using Pyspark
# Following will work if json in the file is in single line
df6 = spark.read.format('org.apache.spark.sql.json').load('dbfs:/FileStore/resources/zipcode_single.json')
df6.printSchema()
df6.show()

df7 = spark.read.json('dbfs:/FileStore/resources/zipcode_single.json')
df7.printSchema()
df7.show()

# if json file is in multiline, read using following
df8 = spark.read.json('dbfs:/FileStore/resources/zipcode.json', multiLine=True)
df8.printSchema()
df8.show()

# read all json files from directory | we can define schema as well when we read json
schema = StructType().add(field='City', data_type=StringType())\
        .add(field='RecordNumber', data_type=IntegerType())\
        .add(field='State', data_type=StringType())\
        .add(field='ZipCodeType', data_type=StringType())\
        .add(field='Zipcode', data_type=IntegerType())
df9 = spark.read.json(path='dbfs:/FileStore/resources/*.json', multiLine=True, schema=schema)
df9.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #6.Write dataframe to JSON

# COMMAND ----------

# DBTITLE 1,Write dataframe to JSON
data = [{'id':1,'name':'Dhruv','department':'tech','salary':1000},{'id':2,'name':'Subarna','department':'hr','salary':2000}]

df10 = spark.createDataFrame(data=data)
df10.show()

df10.write.json('dbfs:/FileStore/resources/df10_write.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #7.Read Parquet file to DF

# COMMAND ----------

# DBTITLE 1,Read Parquet file to DF
df11 = spark.read.parquet('dbfs:/FileStore/resources/mt_cars.parquet')
display(df11)

# COMMAND ----------

# MAGIC %md
# MAGIC #8.Write DF to parquet file

# COMMAND ----------

# DBTITLE 1,Write DF to parquet file
df11.write.parquet('dbfs:/FileStore/resources/parquet-write','overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC #9. .show()

# COMMAND ----------

# DBTITLE 1,.show()
# truncate=False will show entire text in the column
df11.show(n=2,truncate=False)

df11.show(n=1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 .withColumn()

# COMMAND ----------

# DBTITLE 1,.withColumn()
df12 = df11.withColumn('Cost', round(col('mpg')*col('cyl')*col('disp')))

display(df12)

# COMMAND ----------

# MAGIC %md
# MAGIC #11. .withColumnRenamed()

# COMMAND ----------

# DBTITLE 1,.withColumnRenamed()
df13 = df12.withColumnRenamed('Cost','CostOnRoad')
display(df13)

# COMMAND ----------

# MAGIC %md
# MAGIC #12. StructType() and StructField()

# COMMAND ----------

# DBTITLE 1,StructType() and StructField()
'''Pyspark StructType and StructField classes are used to programmatically specify the schema to the DataFrame and create complex columns like nested struct, array and map columns. StructType is a collection of StructField's'''

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Plain Schema
data = [(1, 'Maheer', 3000), (2, 'Wafa', 4000)]
schema = StructType([StructField(name='id', dataType=IntegerType()),\
                    StructField(name='name', dataType=StringType()),\
                    StructField(name='salary', dataType=IntegerType())])
df14 = spark.createDataFrame(data, schema)
df14.show()
df14.printSchema()

# Create nested schema (struct within struct)
data = [(1, ('Maheer','Shaik'), 3000), (2, ('Wafa','Shaik'), 4000)]

structName = StructType([StructField(name='first', dataType=StringType()),\
                        StructField(name='last', dataType=StringType())])
schema = StructType([StructField(name='id', dataType=IntegerType()),\
                    StructField(name='name', dataType=structName),\
                    StructField(name='salary', dataType=IntegerType())])
df15 = spark.createDataFrame(data, schema)
df15.show()
df15.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC #13. ArrayType Columns in PySpark

# COMMAND ----------

# DBTITLE 1,ArrayType Columns in PySpark
data = [('abc',[1,2]), ('mno',[4,5]), ('xyz',[7,8])]
schema = ['id','numbers']

df16 = spark.createDataFrame(data, schema)
df16.show()
df16.printSchema()

# Manual schema creation for array
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

schema = StructType([StructField('id', StringType()),StructField('numbers', ArrayType(IntegerType()))])
df17 = spark.createDataFrame(data,schema)
df17.show()
df17.printSchema()

# Fetch value from array as new column
df18 = df17.withColumn('firstNumber', col('numbers')[0])
df18.show()

# combine columns to array
from pyspark.sql.functions import col, array
df19 = spark.createDataFrame([(33,44),(55,66)],['num1', 'num2'])
df19.show()
df19.withColumn('nums', array(df19.num1, df19.num2)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #14.explode(), split(), array() & array_contains() functions

# COMMAND ----------

# DBTITLE 1,explode(), split(), array() & array_contains() functions
# Array functions

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import explode,col,posexplode

data = [('abc',[1,2]),('mno',[4,5]),('xyz',[7,8])]
schema = StructType([StructField('id', StringType()),StructField('numbers',ArrayType(IntegerType()))])

df20 = spark.createDataFrame(data, schema)
df20.show()

# Explode
print('explode')
df20.withColumn('explodedColumn', explode(col('numbers'))).show()

# Split
from pyspark.sql.functions import split

data = [(1,'maheer','.net,azure,sql'),(2,'wafa','java,aws,sql')]
schema = ('id','name','skills')

df21 = spark.createDataFrame(data, schema)
df21.show()

print('split')
df21.withColumn('skills', split('skills', ',')).show()

# array
data = [(1,'maheer','.net', 'azure'),(2,'wafa','java','sql')]
schema = ('id','name','primarySkill','secondarySkill')

df22 = spark.createDataFrame(data, schema)
df22.show()

df22.withColumn('skillsArray', array(col('primarySkill'),col('secondarySkill'))).show()

# array_contains --> check if array contains a value
from pyspark.sql.functions import split, array, col, array_contains
data = [(1,'maheer',['.net','azure']),(2,'wafa',['java','aws'])]
schema = ['id','name','skills']
df23 = spark.createDataFrame(data,schema)
df23 = df23.withColumn('HasJavaSkill', array_contains(col('skills'), 'java'))
display(df23)
df23.printSchema()

# COMMAND ----------

# DBTITLE 1,MapType column in pyspark
from pyspark.sql.types import MapType

data = [('maheer', {'hair':'black','eyes':'brown'}),('wafa', {'hair':'black','eyes':'blue'})]

schema = StructType([\
                    StructField('name',StringType()),\
                    StructField('properties',MapType(StringType(),StringType()))])

df24 = spark.createDataFrame(data, schema)
df24.show(truncate=False)
df24.printSchema()

df24.withColumn('eyes', df24.properties.eyes).show()
df24.withColumn('hair', df24.properties.hair).show()

# COMMAND ----------

# DBTITLE 1,map_keys(), map_values() & explode() functions
from pyspark.sql.functions import explode, map_keys, map_values

# explode map to columns
df24.select('name', 'properties', explode(df24.properties)).show(truncate=False)

# map_keys
df24.select('name', 'properties', map_keys(df24.properties)).show(truncate=False)

#map_values
df24.select('name', 'properties', map_values(df24.properties)).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Row() class in PySpark
# pyspark.sql.Row which is represented as a record/row in DataFrame, one canc reate a Row object by using named arguments or create a custon Row like class

from pyspark.sql import Row
row = Row('maheer', 2000)
print(row[0] + ' ' + str(row[1]))

# using row named argument(note how we have used name and salary)
row = Row(name='maheer', salary=2000)
print(row[0] + ' ' + str(row.salary))

# create dataframe using row objects
row1 = Row(name='Maheer', salary=2000)
row2 = Row(name='Wafa', salary=3000)
data = [row1, row2]

df25 = spark.createDataFrame(data)
df25.show()

## Alternative way to create Row objects
Person = Row('name','salary')
p1 = Person('maheer', 2000)
p2 = Person('wafa', 3000)

print(p1.name + ' ' + p2.name)

data = [p1, p2]
df26 = spark.createDataFrame(data)
df26.show()

## Alternative way to create dataframe using Row objects
data = [Row(name='maheer', prop = Row(age=30, gender='male')),\
    Row(name='wafa', prop = Row(age=4, gender='female'))]

df27 = spark.createDataFrame(data)
df27.show()
df27.printSchema()

# COMMAND ----------

# DBTITLE 1,Column class in PySpark(Pending)
# pyspark.sql.Column class provides several functions to work with DataFrame to manipulate the Column values, evaluate the boolean expression to filter rows, retrieve a value or part of a value from a DataFrame column

from pyspark.sql.functions import lit
col1 = lit("abcd")
print(type(col1))

data = [('maheer','male',2000), ('wafa','male',4000)]
schema = ['name','gender','salary']
df28 = spark.createDataFrame(data, schema)
df28.show()
df28.printSchema()
df29 = df28.withColumn('newCol', lit('newColVal'))
df29.show()

# To see details of specific column
df29.select(col('name')).show()

# if datatype of column in maptype, you can navigate to the key
data = [('maheer','male',2000,('black','brown')), ('wafa','male',4000,('blue','red'))]
schema = StructType([\
    StructField('name',StringType()),\
        StructField('gender',StringType()),\
            StructField('salary',IntegerType()),\
                StructField('properties',StructType([StructField('eye',StringType()),StructField('hair',StringType())]))])
df30 = spark.createDataFrame(data,schema)
df30.show()
df30.printSchema()
df30.select(col('properties.eye')).show()

# COMMAND ----------

# DBTITLE 1,when() and .otherwise() functions---> If-else equivalent
from pyspark.sql.functions import when

data = [(1,'maheer','M',2000), (2,'wafa','F',4000), (3,'abcd','',7000)]
schema = StructType([\
    StructField('id',IntegerType()),\
        StructField('name',StringType()),\
            StructField('gender',StringType()),\
                StructField('salary',IntegerType())])

df31 = spark.createDataFrame(data,schema)
df31.show()
df31.printSchema()
df31.withColumn('gender_full',when(col('gender')=='M','Male').when(col('gender')=='F','Female').otherwise('unknown')).show()

# COMMAND ----------

# DBTITLE 1,alias(), asc(), desc(), cast(), like() function
# .alias()
data = [(1,'maheer','M',2000), (2,'wafa','F',4000), (3,'abcd','',7000)]

schema = ['id','name','gender','salary']

df32 = spark.createDataFrame(data,schema)
df32.select(col('id').alias('emp_id'),col('name').alias('emp_name')).show()

# sort asc() and desc()
df32.sort(col('name').asc()).show()
df32.sort(col('salary').desc()).show()

# cast()
df32.printSchema()
df33 = df32.select('id','name','gender',col('salary').cast(IntegerType()))
df33.printSchema()

# like()
df33.filter(col('name').like('m%')).show()

# COMMAND ----------

# DBTITLE 1,filter() and where() ---> both work as same 
data = [(1,'maheer','M',2000), (2,'wafa','F',4000), (3,'abcd','',7000)]
schema = StructType([\
    StructField('id',IntegerType()),\
        StructField('name',StringType()),\
            StructField('gender',StringType()),\
                StructField('salary',IntegerType())])
df34 = spark.createDataFrame(data,schema)
df34.show()
df34.printSchema()

df34.where(df34.gender=='M').show()
df34.filter(df34.gender=='M').show()

# COMMAND ----------

# DBTITLE 1,distinct() and dropDuplicates()
# distinct() is used to remove the duplicate rows(all columns)
# dropDuplicates() is used to drop rows based on selected(one or multiple) columns

data = [(1,'maheer','M',2000), (1,'maheer','M',2000), (1,'mahe','M',2000), (2,'wafa','F',4000), (3,'abcd','',7000)]
schema = StructType([\
    StructField('id',IntegerType()),\
        StructField('name',StringType()),\
            StructField('gender',StringType()),\
                StructField('salary',IntegerType())])
df35 = spark.createDataFrame(data,schema)
df35.show()

# distinct()
df35.distinct().show()
df35.dropDuplicates(['id','salary']).show()

# COMMAND ----------

# DBTITLE 1,orderBy(), sort() ---> by default ascending
data = [(1,'maheer','M',2000), (2,'wafa','F',4000), (3,'abcd','',7000)]
schema = StructType([\
    StructField('id',IntegerType()),\
        StructField('name',StringType()),\
            StructField('gender',StringType()),\
                StructField('salary',IntegerType())])
df36 = spark.createDataFrame(data, schema)
df36.sort(col('id').desc()).show()
df36.orderBy(col('name')).show()
df36.sort(col('id').desc(),col('name').desc()).show()

# COMMAND ----------

# DBTITLE 1,union() and unionAll()--->IMP:both doesnot ignore duplicates
# IMP: union and unionAll will merge everything, even the duplicates

data = [(1,'maheer','M',2000), (2,'wafa','F',4000), (3,'abcd','',7000)]
schema = StructType([\
    StructField('id',IntegerType()),\
        StructField('name',StringType()),\
            StructField('gender',StringType()),\
                StructField('salary',IntegerType())])
df37 = spark.createDataFrame(data, schema)

data = [(1,'maheer','M',2000), (2,'wafa','F',4000), (3,'Jeff','M',9000)]
schema = StructType([\
    StructField('id',IntegerType()),\
        StructField('name',StringType()),\
            StructField('gender',StringType()),\
                StructField('salary',IntegerType())])
df38 = spark.createDataFrame(data, schema)

df37.union(df38).show()
df37.unionAll(df38).show()

## Important Note: Union and UnionAll both donot drop duplicates, unionAll is deprecated

# COMMAND ----------

# DBTITLE 1,groupBy() ---> used to collect identical data into groups
# used to collect identical data into groups on DataFrame and perform count, sum, avg, min, max functions on the grouped data

data = [(1,'Maheer','M',5000,'IT'),\
        (2,'wafa','M',6000,'IT'),\
        (3,'asi','F',2500,'Payroll'),\
        (4,'sarfaraj','M',7000,'HR'),\
        (5,'tarique','M',9000,'HR'),\
        (6,'mahboob','M',3000,'Payroll'),\
        (7,'ayesha','F',10000,'IT')]
schema = ['id','name','gender','salary','dep']
df39 = spark.createDataFrame(data,schema)
df39.groupBy('dep').count().show()
df39.groupBy('dep','gender').count().show()
df39.groupBy('dep').min('salary').show()
df39.groupBy('dep').max('salary').show()
df39.groupBy('dep').avg('salary').show()

# COMMAND ----------

# DBTITLE 1,groupBy.agg() ---> perform collection of operations 
# in groupBy agg, you can generate collection of operations in single command. ex: (count, min, max) as shown below. Whereas sipmple groupby will give direct count, sum etc, which are single opertions.

data = [(1,'Maheer','M',5000,'IT'),\
        (2,'wafa','M',6000,'IT'),\
        (3,'asi','F',2500,'Payroll'),\
        (4,'sarfaraj','M',7000,'HR'),\
        (5,'tarique','M',9000,'HR'),\
        (6,'mahboob','M',3000,'Payroll'),\
        (7,'ayesha','F',10000,'IT')]
schema = ['id','name','gender','salary','dep']
df40 = spark.createDataFrame(data,schema)
df40.groupBy('dep').count().show()

from pyspark.sql.functions import count, min, max
df40.groupBy('dep').agg(count('*').alias('countOfEmps'),\
                min('salary').alias('minSal'),\
                max('salary').alias('maxSal')).show()

# COMMAND ----------

# DBTITLE 1,unionByName() ---> merge two different trees
# unionByName() lets you to merge/union two DataFrames with a different number of columns (different schema) by passing allowMissingColumns with value true

data1 = [(1,'maheer','male')]
schema1 = ['id','name','gender']

data2 = [(1,'maheer',2000)]
schema2 = ['id','name','salary']

df41 = spark.createDataFrame(data1,schema1)
df42 = spark.createDataFrame(data2,schema2)

# without unionByName(), wrong data is getting unioned
df41.union(df42).show()

# with unionByName(), right data getting unioned
df41.unionByName(allowMissingColumns=True, other=df42).show()

# COMMAND ----------

# DBTITLE 1,select() ---> select columns 
data = [(1,'Maheer','M',5000,'IT'),\
        (2,'wafa','M',6000,'IT'),\
        (3,'asi','F',2500,'Payroll'),\
        (4,'sarfaraj','M',7000,'HR'),\
        (5,'tarique','M',9000,'HR'),\
        (6,'mahboob','M',3000,'Payroll'),\
        (7,'ayesha','F',10000,'IT')]
schema = ['id','name','gender','salary','dep']
df43 = spark.createDataFrame(data, schema)
df43.select('id','name').show()

# COMMAND ----------

# DBTITLE 1,join() ---> similar to SQL joins
# similar to SQL join. We can combine columns from different DataFranes based on condition. It supports all basic join types such as INNER, LEFT OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, SELF

data1 = [(1,'maheer',2000,2),(2,'wafa',3000,1),(3,'abcd',1000,4)]
schema1 = ['id','name','salary','dep']

data2 = [(1,'IT'),(2,'HR'),(3,'Payroll')]
schema2 = ['id','name']

df44 = spark.createDataFrame(data1, schema1)
df45 = spark.createDataFrame(data2, schema2)

df44.show()
df45.show()

df44.join(df45, df44.dep==df45.id, 'inner').show()
df44.join(df45, df44.dep==df45.id, 'left').show()
df44.join(df45, df44.dep==df45.id, 'right').show()
df44.join(df45, df44.dep==df45.id, 'full').show()

# COMMAND ----------

# DBTITLE 1,join() ---> leftsemi, leftanti, self
df44.join(df45, df44.dep==df45.id, 'leftsemi').show()
df44.join(df45, df44.dep==df45.id, 'leftanti').show()

# COMMAND ----------

# DBTITLE 1,pivot() / unpivot() ---> rotate data
# pivot() is used to rotate data in one column into multiple columns.
# It is an aggregation where one of the grouping column values will be converted in individual columns.

data = [(1,'Maheer','M',5000,'IT'),\
        (2,'wafa','M',6000,'IT'),\
        (3,'asi','F',2500,'Payroll'),\
        (4,'sarfaraj','M',7000,'HR'),\
        (5,'tarique','M',9000,'HR'),\
        (6,'mahboob','M',3000,'Payroll'),\
        (7,'ayesha','F',10000,'IT')]
schema = ['id','name','gender','salary','dep']
df46 = spark.createDataFrame(data, schema)
df46.show()

print('Pivot')
df46.groupBy('dep').pivot('gender').count().show()


# PySpark SQL doesnot have unpivot function, hence will use the stack() function.
print('Unpivot')
from pyspark.sql.functions import expr
data = [('IT', 8, 5),\
        ('Payroll', 3, 2),\
        ('HR', 2, 4)]
schema = ['dep','male','female']
df47 = spark.createDataFrame(data, schema)
print('---Parent---')
df47.show()
print('---Unpivot---')
df47.select('dep', expr("stack(2, 'male', male, 'female', female) as (gender, count)")).show()

# COMMAND ----------

# DBTITLE 1,fill() & fillna() ---> deal with null values
data = [(1,'Maheer','male',1000,None),(2,'Asi','female',2000,'IT'),(3,'Abcd',None,1000,'HR')]
schema = ['id','name','gender','salary','dep']

df48 = spark.createDataFrame(data,schema)
df48.show()

# column wise fill Null values
df48.na.fill('unknown',['gender']).show()
df48.fillna('unknown',['dep']).show()

# COMMAND ----------

# DBTITLE 1,sample() ---> sample subset of data
df49 = spark.range(start=1, end=101)
df49 = df49.sample(fraction=0.1, seed=123)
df50 = df49.sample(fraction=0.1, seed=123)
display(df49)
display(df50)

# COMMAND ----------

# DBTITLE 1,collect() ---> retrieves all elements from df to Array of row type
# collect() retrieves all elements in a dataframe as an Array of Row type to the driver node.
# Once data is in array, you can use python loop to process it further.
# Note: use it with small DataFrames. With big DataFrames it may result in out of memory error as it returns entire data to single node(driver)

data = [(1,'Maheer','male',1000,None),(2,'Asi','female',2000,'IT'),(3,'Abcd',None,1000,'HR')]
schema = ['id','name','gender','salary','dep']

df50 = spark.createDataFrame(data,schema)
df50.show()

dataRows = df50.collect()
print(dataRows)
print(dataRows[0])
print(dataRows[0][0])

# COMMAND ----------

# DBTITLE 1,DataFrame.transform()---> apply functions to dataframe
data = [(1,'Maheer',1000),(2,'Asi',2000)]
schema = ['id','name','salary']

df51 = spark.createDataFrame(data,schema)
df51.show()

from pyspark.sql.functions import upper

def convertToUpper(df):
    return df.withColumn('name',upper(df.name))

def doubleTheSalary(df):
    return df.withColumn('salary', df.salary*2)

df52 = df51.transform(convertToUpper)\
            .transform(doubleTheSalary)
df52.show()

# COMMAND ----------

# DBTITLE 1,pyspark.sql.fuctions import transform
data = [(1,'maheer',['azure','dotnet']),(2,'wafa',['aws','java'])]
schema = ['id','name','skills']
df53 = spark.createDataFrame(data,schema)
df53.show()
df53.printSchema()

from pyspark.sql.functions import transform
df53.select('id','name',transform('skills', lambda x: upper(x)).alias('skills')).show()

def convertToUpper1(x):
    return upper(x)

from pyspark.sql.functions import transform
df53.select(transform('skills', convertToUpper1)).show()

# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView() ---> live in current session
data = [(1,'maheer',2000),(2,'asi',3000)]
schema = ['id','name','salary']

df54 = spark.createDataFrame(data,schema)

df54.createOrReplaceTempView('employees')
df54 = spark.sql('SELECT * FROM employees')
df54.show()

# COMMAND ----------

# DBTITLE 1,createOrReplaceGlobalTempView() ---> live across sessions
# In order to query, we need to type global_temp.<tablename>

data = [(1,'maheer',2000)]
schema = ['id','name','salary']
df55 = spark.createDataFrame(data, schema)
df55.createOrReplaceGlobalTempView('empGlobal')

# list tables from current session
spark.catalog.listTables(spark.catalog.currentDatabase())

# to view global temp tables
spark.catalog.listTables('global_temp')

# COMMAND ----------

# DBTITLE 1,udf
@udf(returnType=IntegerType())
def totalPay(s,b):
    return s+b

data = [(1,'maheer',2000,500),(2,'wafa',4000,1000)]
schema = ['id','name','salary','bonus']
df56 = spark.createDataFrame(data, schema)

df56.withColumn('totalPay', totalPay(df56.salary, df56.bonus)).show()

# COMMAND ----------

# DBTITLE 1,RDD ---> resilient distributed dataset
# RDD is collection of objects, similar to list in Python. Its immutable and in memory processing, by using parallelize() function, we can create RDD.

data = [(1,'maheer'),(2,'wafa')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
df57 = rdd.toDF(['id','name'])
df57.show()

# COMMAND ----------

# DBTITLE 1,map() ---> apply function using lambda(works with RDD)
# RDD transformations use map() to apply a change to every elemnt of rdd and return a new rdd
# doesnot work with dataframe, we have to change a dataframe to rdd before applying map()

data = [('maheer','shaik'),('wafa','shaik')]
rdd = spark.sparkContext.parallelize(data)

rdd1 = rdd.map(lambda x: x + (x[0]+' '+x[1],))
print(rdd1.collect())
df58 = rdd1.toDF(['fn','ln','combo'])
df58.show()

# COMMAND ----------

# DBTITLE 1,flatMap()
# flatMap() is a transformation operation that flattens the RDD(array/map DataFrame columns) after applying the functon on every element and returns a new PySpark RDD.
# Its again not available in dataframes. Explode() functions can be used in dataframes to flatten arrays.

data = ['maheer shaik', 'wafa shaik']
rdd = spark.sparkContext.parallelize(data)

for item in rdd.collect():
    print(item)

rdd1 = rdd.flatMap(lambda x: x.split(' '))
for item in rdd1.collect():
    print(item)

# COMMAND ----------

# DBTITLE 1,partitionBy()
# Its used to partition large Dataset into smaller files based on one or multiple columns

data = [(1,'maheer','male','IT'),(2,'asi','male','HR'),(3,'wafa','female','IT')]
schema = ['id','name','gender','dep']

df59 = spark.createDataFrame(data,schema)

# Following will create single level folder i.e data will be stored under dep=HR and dep=IT folders
df59.write.parquet(path='/FileStore/data/outputemps/',mode='overwrite',partitionBy='dep')

# Following will create two level of folders i.e data will be stored under dep=HR/dep=male, dep=HR/dep=female
df59.write.parquet(path='/FileStore/data/outputemps1/',mode='overwrite',partitionBy=['dep','gender'])

# COMMAND ----------

# DBTITLE 1,from_json() ---> convert json string to MapType
# It's used to convert json string in to MapType or StructType. In this video we discuss about converting it to MapType.

data = [('maheer','{"hair":"black","eye":"brown"}')]
schema = ['name','props']

df60 = spark.createDataFrame(data,schema)
df60.show(truncate=False)
df60.printSchema()

from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType

df61 = df60.withColumn('propsMap',from_json(df60.props, MapType(StringType(),StringType())))
df61.show(truncate=False)
df61.printSchema()
display(df61)

# accessing 'eye' key from MapType column 'propsMap'
df62 = df61.withColumn('eye', df61.propsMap.eye)
df62.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,from_json() ---> convert json string to StructType
data = [('maheer','{"hair":"black","eye":"brown"}')]
schema = ['name','props']

df63 = spark.createDataFrame(data,schema)
df63.show(truncate=False)
df63.printSchema()

from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType, StructType, StructField

structSchema = StructType([\
                            StructField('hair',StringType()),\
                            StructField('eye',StringType())])

df64 = df63.withColumn('propsMap',from_json(df63.props,structSchema))
df64.show(truncate=False)
df64.printSchema()
display(df64)

df65 = df64.withColumn('eye', df64.propsMap.eye)
df65.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,to_json()
# It is used to convert DataFrame column MapType or StructType to JSON string.

from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import to_json

data = [('maheer',{"hair":"black","eye":"brown"})]
schema = ['name','props']

df66 = spark.createDataFrame(data,schema)
df66.show(truncate=False)
df66.printSchema()

df67 = df66.withColumn('prop', to_json(df66.props))
df67.show()
df67.printSchema()

# COMMAND ----------

# DBTITLE 1,json_tuple()
data = [('maheer','{"hair":"black","eye":"brown"}'),
        ('wafa','{"hair":"eye","eye":"red"}')]
schema = ['name','props']

df68 = spark.createDataFrame(data,schema)
df68.show(truncate=False)
df68.printSchema()

from pyspark.sql.functions import json_tuple
df69 = df68.select(df68.name, json_tuple(df68.props,'hair','eye').alias('hair','skin'))
df69.show()

# COMMAND ----------

# DBTITLE 1,get_json_object()
data = [('maheer','{"address":{"city":"hyd","state":"telangana"},"gender":"male"}'),
        ('wafa','{"address":{"city":"banglore","state":"karnataka"},"eye":"blue"}')]

schema = ['name','props']

df70 = spark.createDataFrame(data, schema)
df70.show(truncate=False)
df70.printSchema()

from pyspark.sql.functions import get_json_object
df71 = df70.select('name', get_json_object('props', '$.address.city').alias('city'))
df71.show(truncate=False)
df71.printSchema()

# COMMAND ----------

# DBTITLE 1,Date Functions
# Default format: yyyy-MM-dd

from pyspark.sql.functions import current_date, date_format, lit, to_date

df72 = spark.range(1)

# datatype default format is yyyy-MM-dd
# gives current date in yyyy-MM-dd format
df72.withColumn('todaysDate', current_date()).show()
# converts yyyy-MM-dd datetype to specified format
df72.withColumn('newFormat', date_format(lit('2023-12-25'),'MM.dd.yyyy')).show()
# converts string values of date to DateType
df72.withColumn('newDateCol', to_date(lit('25-12-2023'),'dd-MM-yyyy')).show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import col, min, max, round

data = [
("Country A",5000000, 300000000),
("Country B",20000000, 1200000000),
("Country C",1000000, 500000000),
("Country D",500000000, 40000000000),
("Country E",100000000, 6000000000),
]

data_schema = StructType([StructField("Country_Name", StringType()),
                          StructField("Population", LongType()),
                          StructField("GDP", LongType())])

df = spark.createDataFrame(data=data, schema=data_schema)
df_calculation = df.withColumn("GPD_per_capita", round(col("GDP")/col("Population")))
# df_calculation = df_calculation.orderBy(col("GDP").desc())
display(df_calculation)

min_value = df_calculation.agg(min(col("GPD_per_capita"))).first()[0]
max_value = df_calculation.agg(max(col("GPD_per_capita"))).first()[0]
print(min_value)
print(max_value)
df_calculation.filter(col("GPD_per_capita")==min_value).show()
df_calculation.filter(col("GPD_per_capita")==max_value).show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, concat

data = [
(2023,1,50000),
(2023,2,60000),
(2023,3,55000),
(2023,4,70000),
(2023,5,80000),
(2023,6,65000),
(2023,7,75000),
(2023,8,90000),
(2023,9,120000),
(2023,10,45000),
(2023,11,35000),
(2023,12,30000),
]

schema = StructType([StructField("Year",StringType()),
                     StructField("Month",StringType()),
                     StructField("Sales", IntegerType())])

df = spark.createDataFrame(data=data,schema=schema)

df = df.withColumn("formatted_date", concat(lit(col("Year")),lit("-"),lit(col("Month"))))

df = df.orderBy(col("Sales").desc())

df = df.select("formatted_date","Sales").limit(3)
df.show()

# COMMAND ----------

df_calculation.head()

# COMMAND ----------

# DBTITLE 1,datediff(),months_between(),add_month(),date_add(),month(),year()
from pyspark.sql.functions import datediff, months_between, add_months, date_add, year, month, current_date

df73 = spark.createDataFrame([('2015-04-08','2015-05-08')],['d1','d2'])
df73.withColumn('diff', datediff(df73.d2, df73.d1)).show()
df73.withColumn('monthsBetween', months_between(df73.d2, df73.d1)).show()
df73.withColumn('addmonth', add_months(df73.d2, 4)).show()
df73.withColumn('submonth', add_months(df73.d2, -4)).show()
df73.withColumn('addDate', date_add(df73.d2, 4)).show()
df73.withColumn('subDate', date_add(df73.d2, -4)).show()
df73.withColumn('year', year(df73.d2)).show()
df73.withColumn('month', month(df73.d2)).show()

# COMMAND ----------

# DBTITLE 1,Timestamp functions
from pyspark.sql.functions import current_timestamp, to_timestamp, lit, hour, minute, second
df74 = spark.range(1)
df74.show()
df75 = df74.withColumn('timestamp', current_timestamp())
df75.show(truncate=False)
df75.printSchema()
df76 = df75.withColumn('toTimestamp', to_timestamp(lit('25.12.20222 06.10.13'), 'dd.MM.yyyy HH.mm.ss'))
df76.show()
df76.printSchema()
df76.select('id', hour(current_timestamp()).alias('hour'),\
    minute(current_timestamp()).alias('min'),\
    second(current_timestamp()).alias('second')).show()

# COMMAND ----------

# DBTITLE 1,approx_count_distinct(),avg(),collect_list(),collect_set(),countDistinct(),count()
from pyspark.sql.functions import approx_count_distinct, avg, collect_list,collect_set,countDistinct, count
simpleData = [("Maheer","HR",1500), ("Wafa","IT",3000), ("Asi","HR",1500)]
schema = ["employee_name","department","salary"]
df77 = spark.createDataFrame(data=simpleData, schema=schema)
df77.show()
df77.select(approx_count_distinct('salary')).show()
df77.select(avg('salary')).show()
df77.select(collect_list('salary')).show()
df77.select(collect_set('salary')).show()
df77.select(countDistinct('salary')).show()
df77.select(count('salary')).show()

# COMMAND ----------

# DBTITLE 1,IMP: row_number(),rank(),dense_rank() functions
'''
When we need to partition the data using Window.partitionBy(), and for row number and rank function we need to additionally order by on partition data using orderBy clause.

row_number() --> window function is used to give the sequential row number starting from 1 to the result of each window partition.

rank() --> window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.

dense_rank() --> window function is used to get the result with rank of rows within a window partition without any gaps. this is similar to rank() function difference being rank function leaves gaps in rank when there are ties.
'''

from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window

data = [('maheer', 'HR', 2000),\
        ('wafa', 'IT', 3000),\
        ('asi', 'HR', 1500),\
        ('annu', 'payroll', 3500),\
        ('shakti', 'IT', 3000),\
        ('pradeep', 'IT', 4000),\
        ('kranthi', 'payroll', 2000),\
        ('himanshu', 'IT', 2000),\
        ('bhargava', 'HR', 2000),\
        ('martin', 'IT', 2500)]
schema = ['name','dep','salary']
df78 = spark.createDataFrame(data,schema)
df78.show()

window = Window.partitionBy('dep').orderBy('salary')
df78.withColumn('rowNumber', row_number().over(window)).show()
df78.withColumn('rank', rank().over(window)).show()
df78.withColumn('denserank', dense_rank().over(window)).show()

# COMMAND ----------

# MAGIC %md # Broadcast variable

# COMMAND ----------

# DBTITLE 1,Broadcast variable
from pyspark.sql.functions import col, broadcast

movie_ratings = spark.read.format('csv').option('delimiter','::').load('dbfs:/FileStore/resources/trendytech/movies_201019_002101.dat')

# broadcast movie-rating table
broadcast_movie_ratings = broadcast(movie_ratings)

# COMMAND ----------

# do partitioning and bucketing example

# COMMAND ----------

# MAGIC %md
# MAGIC End of Practice databricks notebook

# COMMAND ----------

# DBTITLE 1,DBUTILS remove file code
# dbutils.fs.rm('dbfs:/FileStore/resources/zipcode_single.json',True)
