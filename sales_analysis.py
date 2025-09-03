import sys
sys.stdout.reconfigure(line_buffering=True) #

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# יצירת Spark session עם כל הליבות (Spark יריץ את הקוד על כל ליבות המחשב (local[*]) - זהו מצב מבוזר מקומי)
spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# קריאת CSV ל-DataFrame
df = spark.read.csv("sales.csv", header=True, inferSchema=True)
df.show()

# הוספת עמודה מחושבת TotalPrice
df = df.withColumn("TotalPrice", col("Quantity") * col("Price"))
df.show()

# סיכום מכירות לפי לקוח
df.groupBy("Customer").sum("TotalPrice").show()

# שאילתות Spark SQL
df.createOrReplaceTempView("sales")
spark.sql("""
    SELECT Customer, SUM(TotalPrice) as TotalSpent
    FROM sales
    GROUP BY Customer
    ORDER BY TotalSpent DESC
""").show()
# סינון ומיון
df.filter(col("TotalPrice") > 5).orderBy(col("TotalPrice").desc()).show()
# ממוצע מחיר לכל מוצר
df.groupBy("Product").avg("Price").show()
# בשלב זה אנחנו יוצרים DataFrame גדול יותר על ידי cross join של שלוש עותקים של DataFrame המקורי ומחלקים את המידע ל-4 PARTITIONS (פיסת מידע ש-SPARK מעבד בצורה מקבילית) וזאת על מנת להדגים את יכולת העיבוד המקבילי של Spark בצורה מקומית.
df1 = df.alias("df1")
df2 = df.alias("df2")
df3 = df.alias("df3")
big_df = df1.crossJoin(df2).crossJoin(df3).repartition(4) # 7 x 7 x 7  = 343 rows,
big_df = big_df.withColumn("Calc", col("df1.Quantity") * col("df1.Price") * 1.5)
big_df.groupBy(col("df1.Customer")).sum("Calc").show()

print("Script finished, Spark UI is available at http://localhost:4040")
time.sleep(600)  # השהייה של 10 דקות לפני סגירת הקונטיינר

spark.stop()
