from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

product_data = [  # Пример таблицы с продуктами без категории и с более чем одной категорией
    (0, 'book'),  # Без категории
    (1, 'tea'),
    (2, 'coffee'),
    (3, 'lemonade'),
    (4, 'bread'),
    (5, 'cookie'),
    (6, 'paper')  # Без категории
]

category_data = [  # Список категорий
    (100, 'drinks'),
    (101, 'food'),
    (102, 'snacks'),
    (103, 'eatable')
]

product_category_data = [  # Таблица приязки категорий к продуктам, один ко многим.
    (1, 100),
    (2, 100),
    (3, 100),
    (4, 101), (4, 103),
    (5, 102), (4, 103),
]

# Создаем DataFrame'ы
products = spark.createDataFrame(product_data, ["product_id", "product_name"])
categories = spark.createDataFrame(category_data, ["category_id", "category_name"])
prod_cat = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

print("Исходные данные")
products.show()
categories.show()
prod_cat.show()
