from pyspark.sql import SparkSession, DataFrame


def get_all_products_category(product_data: DataFrame, category_data: DataFrame, product_category_data: DataFrame):
    """
    
    :param product_data: 
    :param category_data: 
    :param product_category_data: 
    :return: 
    """
    products_with_links = product_data.join(product_category_data,
                                            product_data.product_id == product_category_data.product_id,
                                            "left").select(product_data.product_name, product_category_data.category_id)
    # return products_with_links
    # Получаем все продукты с идентификатором категории.
    # +------------+-----------+
    # | product_name | category_id |
    # +------------+-----------+
    # | book | NULL |
    # | tea | 100 |
    # | coffee | 100 |
    # | lemonade | 100 |
    # | bread | 103 |
    # | bread | 103 |
    # | bread | 101 |
    # | cookie | 102 |
    # | paper | NULL |
    # +------------+-----------+

    products_with_categories_name = products_with_links.join(category_data,
                                                             products_with_links.category_id == category_data.category_id,
                                                             "left").select(products_with_links.product_name,
                                                                            category_data.category_name)

    # Теперь присваиваем категориям имена
    # +------------+-------------+
    # | product_name | category_name |
    # +------------+-------------+
    # | book | NULL |
    # | paper | NULL |
    # | bread | eatable |
    # | bread | eatable |
    # | tea | drinks |
    # | coffee | drinks |
    # | lemonade | drinks |
    # | bread | food |
    # | cookie | snacks |
    # +------------+-------------+
    return products_with_categories_name


if __name__ == "__main__":
    try:
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

        print("Итоговые данные")
        result = get_all_products_category(products, categories, prod_cat)
        result.show()

        spark.stop()

    finally:
        if spark is not None and spark.active:
            spark.stop()
