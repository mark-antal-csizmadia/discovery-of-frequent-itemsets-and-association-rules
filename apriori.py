import time
import numpy as np
from math import comb
import multiprocessing as mp


def get_singletons(items_counts_rdd, s):
    """Read data given a path.

    Parameters
    ----------
    items_counts_rdd : pyspark.rdd.PipelinedRDD
        A list of tuples of items and their counts. All items are represented as integers.

    s : int
        Support, min number of occurence across baskets.

    Returns
    -------
    singletons_rdd : pyspark.rdd.PipelinedRDD
        A list of items and their counts/support.
        E.g.:
        [({448}, 1370),
         ({834}, 1373),
         ({240}, 1399),
         ({368}, 7828),
         ({274}, 2628),
         ({52}, 1983),
         ({630}, 1523),
         ({538}, 3982),
         ({704}, 1794),
         ({814}, 1672)]
    """
    singletons_rdd = items_counts_rdd.filter(lambda x: s <= x[1])
    singletons_rdd = singletons_rdd.map(lambda x: (set([x[0]]), x[1]))
    return singletons_rdd


def construct_itemsets_apriori(k, itemsets_frequent_rdd, spark, from_ckpt=False):
    """Construct itemsets, or generate candidates for filtering in the Apriori algorithm.

    Parameters
    ----------
    k : int
        Length of proposed itemsets.

    itemsets_frequent_rdd : pyspark.rdd.PipelinedRDD
        A list of tuples of itemsets and their counts, from previous step. All items are represented as integers.

    spark : pyspark.sql.session.SparkSession
        Spark session.

    from_ckpt : bool
        If True, load candidate sets from checkpoint, else compute them.

    Returns
    -------
    candidates : pyspark.rdd.PipelinedRDD
        A list of list where the first element is the id of the candidate itemset
        and the second element is the itemset.
        E.g.:
        [[0, {413, 494}],
         [1, {874, 978}],
         [2, {701, 946}],
         [3, {335, 804}],
         [4, {576, 583}],
         [5, {242, 684}],
         [6, {597, 641}],
         [7, {581, 766}],
         [8, {335, 538}],
         [9, {39, 884}],
         [10, {516, 854}],
         [11, {115, 735}],
         [12, {126, 952}],
         [13, {854, 895}],
         [14, {682, 740}],
         [15, {774, 984}],
         [16, {468, 984}],
         [17, {738, 749}],
         [18, {675, 790}],
         [19, {529, 600}]]
    """
    assert 1 < k, f"k={k}, needs to be 1<k"

    if from_ckpt:
        candidates = np.load(f'ckpt/candidates_k_{k}.npy', allow_pickle=True)
        candidates = spark.sparkContext.parallelize(candidates.tolist())
        print(f"loaded proposed n={candidates.count()} candidates")
    else:
        # get singelton itemsets
        singletons_rdd = itemsets_frequent_rdd.filter(lambda x: len(x[0]) == 1)
        # get itemsets of length k-1
        k_minus_1_tons_rdd = itemsets_frequent_rdd.filter(lambda x: len(x[0]) == k - 1)
        # use singletions an k-1tons to prodcue cartesian product of itemsets
        singletons_rdd = singletons_rdd.map(lambda x: x[0])
        k_minus_1_tons_rdd = k_minus_1_tons_rdd.map(lambda x: x[0])
        cartesian_rdd = singletons_rdd.cartesian(k_minus_1_tons_rdd)
        # keep from cartesian the itemsets that are k long
        cartesian_k_rdd = cartesian_rdd.map(lambda x: x[0].union(x[1])).filter(lambda x: len(x) == k)
        cartesian_k = cartesian_k_rdd.collect()
        # remove duplicate sets (could have due to cartesian)
        cartesian_k_no_dupl = [set(item) for item in set(frozenset(item) for item in cartesian_k)]
        # make candidate list with structure
        candidates_list = [(x, 0) for idx, x in enumerate(cartesian_k_no_dupl)]
        candidates = spark.sparkContext.parallelize(candidates_list)
        # unpruned lenght
        n_before_prune = candidates.count()
        # save as ckpt
        np.save(f'ckpt/candidates_notpruned_k_{k}', np.array(candidates.collect()))

        # prune candidates: only keep the ones whose all subsets are also frequent itemsets
        # subsets are of legnth=1,...,k-1
        for i in range(1, k):
            # k choose i, n_comb is the number of possible subsets in
            n_comb = comb(k, i)
            # get frequent itemsets of lenght i
            itemsets_i = itemsets_frequent_rdd.filter(lambda x: len(x[0]) == i).collect()
            # for each candidate, get the intersecton of the candidate with all itemsets of lenght i, and
            # sum up the intersections of size i (i.e.: the number of possible subsets of size i in a
            # candidate itemset of size k are k choose i, to make sure all n_comb subsets of size i are in the
            # previous frequent itemsets, find all of the subsets there)
            candidates = \
                candidates.map(lambda x: (x[0], sum([len(x[0].intersection(s[0])) == i for s in itemsets_i]))) \
                    .filter(lambda x: n_comb == x[1])

        # zip candidates with id
        candidates = candidates.map(lambda x: x[0]).zipWithIndex().map(lambda x: (x[1], x[0]))
        n_after_prune = candidates.count()

        # save candidates as ckpt
        np.save(f'ckpt/candidates_k_{k}', np.array(candidates.collect()))

        print(f"proposing n={candidates.count()} candidates (n_pruned={n_before_prune - n_after_prune})")

    return candidates


def get_support(candidate, data_rdd_c, k):
    """Get support of a candidate itemset from across the baskets.

    Parameters
    ----------
    candidate : set
        A candidate itemset of lenght k.

    data_rdd_c : list
        List of sets of items (list of baskets)

    k : int
        Length of candidate itemset.

    Returns
    -------
    int
        The support of a candidate itemset in the data, i.e.: from across the baskets.
    """
    # get and filter the intersections of size k with the baskets, and sum the number of such intersections up
    return len(list(filter(lambda x: len(x) == k, map(lambda x: x & candidate, data_rdd_c))))


def filter_itemsets_apriori(candidates_rdd, k, s, data_rdd_c, spark, from_ckpt=False):
    """Get support of a candidate itemset from across the baskets.

    Parameters
    ----------
    candidates_rdd : pyspark.rdd.PipelinedRDD
        A list of tuple where the first elements are ids of candidates and second are sets of items,
        i.e.: proposed itemsets. Each candidate itemset is oz size k.

    k : int
        Length of candidate itemsets.

    s : int
        Support threshold beyond which the canidate itemset is regarded as frequent itemset.
        I.e. the number of occurence from across baskets is the support

    data_rdd_c : list
        List of sets of items (list of baskets)

    spark : pyspark.sql.session.SparkSession
        Spark session.

    from_ckpt : bool
        If True, load candidate sets from checkpoint, else compute them.

    Returns
    -------
    res : pyspark.rdd.PipelinedRDD
        List of tuples where the first elements are the itemsets and the second is theri supports.
    """
    if from_ckpt:
        start_time = time.time()
        print("Filtering loading from file...")
        res = np.load(f'ckpt/filtered_candidates_k_{k}_s_{s}.npy', allow_pickle=True)
        res = spark.sparkContext.parallelize(res.tolist())
        end_time = time.time()
        print(f"k={k}, t={end_time - start_time:.8f} seconds, n={res.count()}")
    else:
        start_time = time.time()
        print("Staring filtering...")
        candidates = candidates_rdd.collect()

        # multiprocessing
        # make n number of processes equal to number of cpu cores
        nprocs = mp.cpu_count()
        pool = mp.Pool(processes=nprocs)
        # compute the support of each candidate in the data
        supports = pool.starmap(get_support, [(c, data_rdd_c, k) for (idx, c) in candidates])

        # keep canidates with support above threshold
        res = \
            spark.sparkContext.parallelize(candidates) \
                .filter(lambda x: s <= supports[x[0]]).map(lambda x: (x[1], supports[x[0]]))

        # save res as ckpt
        np.save(f'ckpt/filtered_candidates_k_{k}_s_{s}', np.array(res.collect()))

        end_time = time.time()
        print(f"k={k}, t={end_time - start_time:.8f} seconds, n={res.count()}")

    return res
