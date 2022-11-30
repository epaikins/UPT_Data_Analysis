# Report on MOP From ITC

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc, asc, round

spark = SparkSession.builder.appName("ITC Reports").getOrCreate()

baseFilePath = "/workspaces/UPT_Data_Analysis/data/ITC_Data/"
exportFilePath = "/workspaces/UPT_Data_Analysis/data/exports/"
itc_exportFolderPath = "ITC_exports/"

df = spark.read.options(header="True", inferSchema="True", delimiter=",").csv(
    baseFilePath + "14-11-2022_contribution_data_mop.csv"
)

df = df.sort(df.created.asc())


# # Showing all data
# df.show()

# # Count of All MOP Transactions
# print("Count of All MOP Transactions: " + str(df.count()))

# # Get all Pension MOP Transactions
# pensions_data = df.filter(df.account_id.startswith("UPTCF"))

# # Count of MOP Pension Transactions
# print("Count of MOP Pension Transactions: " + str(pensions_data.count()))

# # Generate MOP Pension Transactions CSV
# pensions_data.coalesce(1).write.option("header", True).mode("overwrite").csv(
#     baseFilePath + "Pension_Transactions"
# )

# # Get all Savings MOP Transactions
# savings_data = df.filter(df.account_id.startswith("UPTPP"))

# # Count of MOP Savings Transactions
# print("Count of MOP Savings Transactions: " + str(savings_data.count()))

# # Generate MOP Savings Transactions CSV
# savings_data.coalesce(1).write.option("header", True).mode("overwrite").csv(
#     baseFilePath + "Savings_Transactions"
# )

# # Distinct unitholders for MOP Pension Transactions
# pensions_data_distinct = pensions_data.dropDuplicates(["msisdn"]).select(
#     ["created", "account_id", "customer_name", "msisdn"]
# )

# # Showing MOP Pension Contributors
# pensions_data_distinct.show()

# # Count of MOP Pension Contributors
# print("Count of MOP Pension Contributors: " + str(pensions_data_distinct.count()))

# # Distinct unitholders for MOP Pension Transactions
# savings_data_distinct = savings_data.dropDuplicates(["msisdn"]).select(
#     ["created", "account_id", "customer_name", "msisdn"]
# )

# # Showing MOP Pension Contributors
# savings_data_distinct.show()

# # Count of MOP Pension Contributors
# print("Count of MOP Savings Contributors: " + str(savings_data_distinct.count()))

# # Count transactions for each contributor
# contributor_no_of_transactions = df.groupBy("msisdn").count()
# contributor_no_of_transactions.show()

# # Count transactions for each contributor Pension Scheme
# contributor_no_of_transactions_pension = pensions_data.groupBy("msisdn").count()
# contributor_no_of_transactions_pension.show()

# # Count transactions for each contributor Pension Scheme
# contributor_no_of_transactions_savings = savings_data.groupBy("msisdn").count()
# contributor_no_of_transactions_savings.show()

# # Total amount transactions for each contributor
# contributor_total_amount = (
#     df.groupBy("msisdn")
#     .agg(round(_sum("amount"), 2).alias("Total_Contributions"))
#     .sort(desc("Total_Contributions"))
# )
# contributor_total_amount.show()

# # Total amount transactions for each contributor Pension scheme
# contributor_total_amount_pension = (
#     pensions_data.groupBy("msisdn")
#     .agg(round(_sum("amount"), 2).alias("Total_Contributions_Pension"))
#     .sort(desc("Total_Contributions"))
# )
# contributor_total_amount_pension.show()

# # Total amount transactions for each contributor Savings scheme
# contributor_total_amount_savings = (
#     savings_data.groupBy("msisdn")
#     .agg(round(_sum("amount"), 2).alias("Total_Contributions_Savings"))
#     .sort(desc("Total_Contributions"))
# )
# contributor_total_amount_savings.show()

# # Generate Contributor Number of Transactions CSV
# contributor_no_of_transactions.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributor_No_Of_Transactions")
# contributor_no_of_transactions_pension.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributor_No_Of_Transactions_Pension")
# contributor_no_of_transactions_savings.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributor_No_Of_Transactions_Savings")


# # Generate Contributor Total Amount of Transactions CSV
# contributor_total_amount.coalesce(1).write.option("header", True).mode("overwrite").csv(
#     baseFilePath + "Contributor_Total_Amount"
# )
# contributor_total_amount_pension.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(baseFilePath + "Contributor_Total_Amount_Pension")
# contributor_total_amount_savings.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(baseFilePath + "Contributor_Total_Amount_Savings")


# #  Search by UnitholderNumber
# # df.filter(df.msisdn == "233541064626").show(truncate=False)
# contributor_total_amount.filter(df.msisdn == "233548018088").show(truncate=False)

# # Filter Customers for amount range 0 - 5
# df_contribution_range_0_5 = df.filter( (df.amount > 0) & (df.amount < 5))

# # Generate CSV for amount range 0 - 5
# df_contribution_range_0_5.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_0_5")

# df_contribution_range_0_5.show()

# # Filter Customers for amount range 5 - 10
# df_contribution_range_5_10 = df.filter( (df.amount > 5) & (df.amount < 10))

# # Generate CSV for amount range 5 - 10
# df_contribution_range_5_10.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_5_10")

# df_contribution_range_5_10.show()

# # Filter Customers for amount range 10 - 19
# df_contribution_range_10_20 = df.filter( (df.amount > 10) & (df.amount < 20))

# # Generate CSV for amount range 10 - 19
# df_contribution_range_10_20.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_10_20")

# df_contribution_range_10_20.show()

# # Filter Customers for amount range 20 - 49
# df_contribution_range_20_50 = df.filter( (df.amount > 20) & (df.amount < 50))

# # Generate CSV for amount range 20 - 49
# df_contribution_range_20_50.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_20_50")

# df_contribution_range_20_50.show()

# # Filter Customers for amount range 50 - 99
# df_contribution_range_50_100 = df.filter( (df.amount > 50) & (df.amount < 100))

# # Generate CSV for amount range 50 - 99
# df_contribution_range_50_100.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_50_100")

# df_contribution_range_50_100.show()

# # Filter Customers for amount range 100 - 999
# df_contribution_range_100_1000 = df.filter( (df.amount > 100) & (df.amount < 1000))

# # Generate CSV for amount range 100 - 999
# df_contribution_range_100_1000.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_100_1000")

# df_contribution_range_100_1000.show()

# # Filter Customers for amount range 1000 - 1999
# df_contribution_range_1000_2000 = df.filter( (df.amount > 1000) & (df.amount < 2000))

# # Generate CSV for amount range 0 - 5
# df_contribution_range_1000_2000.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_1000_2000")

# df_contribution_range_1000_2000.show()

# # Filter Customers for amount range 2000 - 4999
# df_contribution_range_2000_5000 = df.filter( (df.amount > 2000) & (df.amount < 5000))

# # Generate CSV for amount range 2000 - 4999
# df_contribution_range_2000_5000.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_2000_5000")

# df_contribution_range_2000_5000.show()

# # Filter Customers for amount range 5000 - 7999
# df_contribution_range_5000_8000 = df.filter( (df.amount > 5000) & (df.amount < 8000))

# # Generate CSV for amount range 5000 - 7999
# df_contribution_range_5000_8000.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_5000_7999")

# df_contribution_range_5000_8000.show()

# # Filter Customers for amount range 8000 - 9999
# df_contribution_range_8000_10000 = df.filter( (df.amount > 8000) & (df.amount < 10000))

# # Generate CSV for amount range 8000 - 9999
# df_contribution_range_8000_10000.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_8000_10000")

# df_contribution_range_8000_10000.show()

# # Filter Customers for amount range 10000 - infinity
# df_contribution_range_above_10000 = df.filter(df.amount > 10000)

# # Generate CSV for amount range 0 - 5
# df_contribution_range_above_10000.coalesce(1).write.option("header", True).mode(
#     "overwrite"
# ).csv(exportFilePath+ itc_exportFolderPath + "Contributions_Range_above_1000")

# df_contribution_range_above_10000.show()