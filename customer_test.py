from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


my_conf = SparkConf()

my_conf.set("Spark.app.name", "customer_test")
my_conf.set("spark.master", "local[2]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

#  Creating Customer Dataframe
customerDf = spark.read \
            .option("header", True)\
            .option("inferSchema", True) \
            .csv("/Users/rockvikesh/downloads/python-assignment-level2/input_data/starter/customers.csv")\


# creating Products Dataframe
productDf = spark.read \
            .option("header", True)\
            .option("inferSchema", True) \
            .csv("/Users/rockvikesh/downloads/python-assignment-level2/input_data/starter/products.csv")\
            .withColumnRenamed("product_id", "p_id")


# creating Transactions Dataframe
transactionsDf = spark.read\
    .option("multiline", True)\
    .json("/Users/rockvikesh/downloads/python-assignment-level2/input_data/starter/transactions")\
    .withColumnRenamed("customer_id", "c_id")


# flattening the Transactions basket column
flatten_TransactionsDf = transactionsDf.select(transactionsDf.c_id, explode(transactionsDf.basket).alias("values"))\
                        .select("c_id", "values.price", "values.product_id")


# Joining flatTransDf and transactionsDf
Joined_Transactions_ProductDf = flatten_TransactionsDf\
                                .join(productDf, productDf.p_id == flatten_TransactionsDf.product_id, 'inner')


# Joining customerDf and JoinedTransactionsProductDf and dropping the c_id ,p_id , product_description, price columns
Joined_Customer_Transactions_ProductDf = customerDf.\
                                        join(Joined_Transactions_ProductDf, customerDf.customer_id==Joined_Transactions_ProductDf.c_id, "inner")\
                                        .drop('c_id', 'p_id', 'product_description', 'price')

# Calculating the product_count for each customer
Joined_Customer_Transactions_ProductDf.createOrReplaceTempView("Customer_Transactions_ProductDf")

Customer_Purchase_countDf = spark.sql("select customer_id ,count(distinct product_id) as purchase_count "
                                      "from Customer_Transactions_ProductDf group by customer_id")\

# Renaming the customer_id column in Customer_Purchase_countDf

new_Customer_Purchase_countDf = Customer_Purchase_countDf.withColumnRenamed("customer_id", "c_id")


# final output
final_Output_Df = Joined_Customer_Transactions_ProductDf\
                .join(new_Customer_Purchase_countDf,
                new_Customer_Purchase_countDf.c_id == Joined_Customer_Transactions_ProductDf.customer_id, 'inner')\
                .drop('c_id').dropDuplicates().orderBy("customer_id")

# final_Output_Df.show(100)

final_Output_Df.write.mode(saveMode="OVERWRITE")\
    .json("/Users/rockvikesh/downloads/python-assignment-level2/solution/output/final")

spark.stop
