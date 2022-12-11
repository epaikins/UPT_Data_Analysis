# Report on MOP From Edwin 
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc, asc, round

spark = SparkSession.builder.appName("ITC Reports").getOrCreate()

baseFilePath = "/workspaces/UPT_Data_Analysis/data/MOP_Data/"
exportFilePath = "/workspaces/UPT_Data_Analysis/data/exports/"
mop_exportFolderPath = "MOP_exports/"

df_balances = spark.read.options(header="True", inferSchema="True", delimiter=",").csv(
    baseFilePath + "Balances.csv"
)

# Showing balances data
df_balances.show()


df_contribution = spark.read.options(header="True", inferSchema="True", delimiter=",").csv(
    baseFilePath + "Contribution.csv"
)

# Showing balances data
df_contribution.show()


df_full_balances = spark.read.options(header="True", inferSchema="True", delimiter=",").csv(
    baseFilePath + "Full_Balances_all.csv"
)

# Showing balances data
df_full_balances.show()


df_redemptions = spark.read.options(header="True", inferSchema="True", delimiter=",").csv(
    baseFilePath + "Redemptions.csv"
)

# Showing balances data
df_redemptions.show()
