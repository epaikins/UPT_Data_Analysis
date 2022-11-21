from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc, asc, round

spark = SparkSession.builder.appName("ITC Reports").getOrCreate()

baseFilePath = "/workspaces/UPT_Data_Analysis/data/"

df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(baseFilePath+"14-11-2022_contribution_data_mop.csv")

df = df.sort(df.created.asc())


# Showing all data
# df.show()

# Count of All MOP Transactions
# print("Count of All MOP Transactions: " + str(df.count()))

# Get all Pension MOP Transactions
# pensions_data = df.filter(df.account_id.startswith("UPTCF")) 

# Count of MOP Pension Transactions
# print("Count of MOP Pension Transactions: " + str(pensions_data.count()))

# Generate MOP Pension Transactions CSV
# pensions_data.coalesce(1).write.option("header",True).mode('overwrite').csv(baseFilePath + "Pension_Transactions")

# Get all Savings MOP Transactions
# savings_data = df.filter(df.account_id.startswith("UPTPP"))

# Count of MOP Savings Transactions
# print("Count of MOP Savings Transactions: " + str(savings_data.count()))

# Generate MOP Savings Transactions CSV
# savings_data.coalesce(1).write.option("header",True).mode('overwrite').csv(baseFilePath + "Savings_Transactions")

# Distinct unitholders for MOP Pension Transactions
# pensions_data_distinct = pensions_data.dropDuplicates(["msisdn"]).select(["created","account_id","customer_name", "msisdn"])

# Showing MOP Pension Contributors
# pensions_data_distinct.show()

# Count of MOP Pension Contributors
# print("Count of MOP Pension Contributors: " + str(pensions_data_distinct.count()))

# Distinct unitholders for MOP Pension Transactions
# savings_data_distinct = savings_data.dropDuplicates(["msisdn"]).select(["created","account_id","customer_name", "msisdn"])

# Showing MOP Pension Contributors
# savings_data_distinct.show()

# Count of MOP Pension Contributors
# print("Count of MOP Savings Contributors: " + str(savings_data_distinct.count()))

# Count transactions for each contributor
contributor_no_of_transactions = df.groupBy("msisdn").agg(round(_sum("amount"),2).alias("Total_Contributions")).sort(asc("Total_Contributions"))
contributor_no_of_transactions.show()

# Generate Contributor Number of Transactions CSV
# contributor_no_of_transactions.coalesce(1).write.option("header",True).mode('overwrite').csv(baseFilePath + "Contributor_No_Of_Transactions")

#  Search by UnitholderNumber
# df.filter(df.msisdn == "233541064626").show(truncate=False)