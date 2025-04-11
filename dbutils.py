# COMMAND ----------

# Step 1: Create the widget
dbutils.widgets.dropdown("country", "US", ["US", "CA", "IN"])

# COMMAND ----------

# Step 2: Get the selected value
country = dbutils.widgets.get("country")
print(f"Selected country: {country}")

# COMMAND ----------

# Step 3: (Optional) Create a demo table if it doesn't exist
# You can skip this if your 'users' table already exists

from pyspark.sql import Row

sample_data = [
    Row(user_id=1, name="Alice", country="US"),
    Row(user_id=2, name="Bob", country="CA"),
    Row(user_id=3, name="Charlie", country="IN"),
    Row(user_id=4, name="David", country="US"),
    Row(user_id=5, name="Eva", country="CA")
]

df_sample = spark.createDataFrame(sample_data)
df_sample.write.mode("overwrite").saveAsTable("users")

# COMMAND ----------

# Step 4: Run the query using the widget value
query = f"SELECT * FROM users WHERE country = '{country}'"
filtered_df = spark.sql(query)

# Step 5: Display the filtered data
display(filtered_df)
