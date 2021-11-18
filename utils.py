def read_data(data_path, spark):
    """Read data given a path.

    Parameters
    ----------
    data_path : str
        Description of parameter `x`.

    spark : pyspark.sql.session.SparkSession
        Spark session.

    Returns
    -------
    tuple
        Of data_rdd, items_rdd, items_counts_rdd where data_rdd is a list of sets representing baskets
        of items, items_rdd a list of unique items, items_counts_rdd is a list of tuples of items and their counts.
        All items are represented as integers.
    """
    # read raw data
    data_raw_rdd = spark.sparkContext.textFile(data_path)
    # transform raw data to list of lists (list of ordered baskets)
    data_rdd = data_raw_rdd.map(lambda x: set([int(y) for y in x.strip().split(" ")]))
    # get item counts (k:v where k is hashed item id and v is count)
    items_counts_rdd = data_rdd.flatMap(lambda list: list).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
    # get items
    items_rdd = items_counts_rdd.map(lambda x: x[0])

    return data_rdd, items_rdd, items_counts_rdd
