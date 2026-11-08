from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Створюємо сесію Spark
spark = SparkSession.builder.appName("DE_Homework_03").getOrCreate()

# Завантажуємо датасет
products_df = spark.read.csv('./products.csv', header=True, inferSchema=True)
purchases_df = spark.read.csv('./purchases.csv', header=True, inferSchema=True)
users_df = spark.read.csv('./users.csv', header=True, inferSchema=True)

# Очищаємо дані: видаляємо рядки з будь-якими пропущеними значеннями
print(f"До очищення: products={products_df.count()}, "
      f"purchases={purchases_df.count()}, users={users_df.count()}")

products_df = products_df.dropna()
purchases_df = purchases_df.dropna()
users_df = users_df.dropna()

print(f"Після очищення: products={products_df.count()}, "
      f"purchases={purchases_df.count()}, users={users_df.count()}")

# Виводимо перші 10 записів
# products_df.show(10)
# purchases_df.show(10)
# users_df.show(10)

# Загальна сума покупок за кожною категорією продуктів
# (сума покупки = quantity * price)
total_by_category_df = (
    purchases_df
    .join(products_df, on="product_id", how="inner")
    .withColumn("purchase_amount", F.col("quantity") * F.col("price"))
    .groupBy("category")
    .agg(F.round(F.sum("purchase_amount"), 2).alias("total_amount"))
    .orderBy(F.col("total_amount").desc())
)
total_by_category_df.show()

# Сума покупок за кожною категорією продуктів для віку від 18 до 25 включно
total_by_category_18_25_df = (
    purchases_df
    .join(users_df, on="user_id", how="inner")
    .filter(F.col("age").between(18, 25))
    .join(products_df, on="product_id", how="inner")
    .withColumn("purchase_amount", F.col("quantity") * F.col("price"))
    .groupBy("category")
    .agg(F.round(F.sum("purchase_amount"), 2).alias("total_amount"))
    .orderBy(F.col("total_amount").desc())
)
total_by_category_18_25_df.show()

# Частка покупок за кожною категорією від сумарних витрат для віку 18-25
total_spent_18_25 = total_by_category_18_25_df.agg(
    F.sum("total_amount")
).collect()[0][0]

share_by_category_18_25_df = (
    total_by_category_18_25_df
    .withColumn(
        "share_percent",
        F.round(F.col("total_amount") / F.lit(total_spent_18_25) * 100, 2)
    )
    .orderBy(F.col("share_percent").desc())
)
share_by_category_18_25_df.show()

# Топ-3 категорії з найвищим відсотком витрат для віку 18-25
top3_categories_18_25_df = share_by_category_18_25_df.limit(3)
top3_categories_18_25_df.show()

# Закриваємо сесію Spark
spark.stop()
