from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


def mine_patterns(transactions_matrix, min_supp, min_lift, min_conf):
    """
    Функция для поиска ассоциативных правил в матрице транзакций.

    Аргументы:
    - transactions_matrix (DataFrame): Матрица транзакций, представленная в виде DataFrame.
    - min_supp (float): Минимальное значение поддержки для поиска частых наборов.
    - min_lift (float): Минимальное значение подъема для поиска ассоциативных правил.
    - min_conf (float): Минимальное значение достоверности для фильтрации ассоциативных правил.

    Возвращает:
    - DataFrame: DataFrame с найденными ассоциативными правилами.
    """

    # Поиск частых наборов с параметром минимальной поддержки
    freaquent_itemsets = apriori(transactions_matrix, min_support=min_supp, use_colnames=True)

    # Поиск ассоциативных правил с заданной метрикой и её минимальным значением
    rules = association_rules(freaquent_itemsets, metric='lift', min_threshold=min_lift)

    # Редактирование столбцов
    rules.drop('antecedent support', axis=1, inplace=True)
    rules.drop('consequent support', axis=1, inplace=True)
    rules.drop('leverage', axis=1, inplace=True)
    rules.drop('conviction', axis=1, inplace=True)
    # rules.drop('zhangs_metric', axis=1, inplace=True)
    rules["support"] = rules["support"].round(4)
    rules["confidence"] = rules["confidence"].round(4)
    rules["lift"] = rules["lift"].round(4)

    if min_conf:
        rules = rules[rules["confidence"] >= min_conf]

    return rules.dropna()