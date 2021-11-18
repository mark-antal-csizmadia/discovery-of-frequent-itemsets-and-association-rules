from itertools import chain, combinations
import numpy as np


def get_subsets(myset):
    """Get the subsets of a set. No empty sets and no identity set as subsets.
    Source: https://stackoverflow.com/questions/1482308/how-to-get-all-subsets-of-a-set-powerset

    Parameters
    ----------
    myset : set
        A set of items.

    Returns
    -------
    valid_subsets : list
        List of subsets of myset. Subsets are not empty and not the same as myset.
    """
    # max size of subset is max_ - 1
    max_ = len(myset)
    min_ = 0
    subsets = \
        [set(subset) for subset in
         list(chain.from_iterable(combinations(myset, r) for r in range(len(myset) + 1)))]
    valid_subsets = [subset for subset in subsets if min_ < len(subset) < max_]

    return valid_subsets


def get_association_rules(itemsets_frequent, c_thresh=None):
    """Get the association rules based on frequent itemsets.

    Parameters
    ----------
    itemsets_frequent : list
        List of tuples where first element is frequent itemset second is its support.

    c_thresh : int
        Confidence threshold for association rules.

    Returns
    -------
    association_rules : list
        List of tuples where first el is frequent itemset, second is a list of its association rules.
        The list of association rules is a list of tuples where the first el is teh association rule
        (first el -> secnd el) and the second el is its confidence score.
    """
    # get non singletion freuqnt itemsets
    itemsets_frequent_not_singleton = list(filter(lambda x: 1 < len(x[0]), itemsets_frequent))

    association_rules = []

    # for each
    for itemset_frequent, union_support in itemsets_frequent_not_singleton:
        # get subsets of freq itemset
        subsets = get_subsets(itemset_frequent)
        # make rule sets for itemset
        rule_sets = [(subset, itemset_frequent.difference(subset)) for subset in subsets]

        association_rule_sets = []

        # for each rule set, get confidence
        for rule_set in rule_sets:
            i = rule_set[0]
            i_idx = \
                np.where(np.array(i) == np.array([itemset_frequent[0] for itemset_frequent in itemsets_frequent]))[0][0]
            i_support = itemsets_frequent[i_idx][1]

            conf = union_support / i_support

            # if above conf thresh
            if c_thresh <= conf:
                association_rule_sets.append((rule_set, conf))

        association_rules.append((itemset_frequent, association_rule_sets))

    return association_rules


def pretty_print_association_rules(association_rules):
    """Pretty pring association rules."""
    for frequent_itemset, association_rule_sets in association_rules:
        print(f"frequent itemset: {frequent_itemset}")

        for association_rule_set, confidence in association_rule_sets:
            print(f"\tassociation rule: {association_rule_set[0]} -> {association_rule_set[1]}"
                  f" with confidence={confidence:.4f}")
        print()
