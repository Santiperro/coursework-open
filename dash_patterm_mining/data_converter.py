import pandas as pd
from constants import *


def convert_to_transactions(journals, students, ege, grades, ratings):
    """
    Конвертирует 5 таблиц с данными в матрицу транзакций для поиска шаблонов.

    Аргументы:
    - journals (pd.DataFrame): Таблица с данными о журналах.
    - students (pd.DataFrame): Таблица с данными о студентах.
    - ege (pd.DataFrame): Таблица с данными об экзаменах ЕГЭ.
    - grades (pd.DataFrame): Таблица с данными об оценках.
    - ratings (pd.DataFrame): Таблица с данными о рейтингах.

    Возвращает:
    - pd.DataFrame: Таблица транзакций для поиска шаблонов.
    """
    mark_deciles = [50, 75, 85, 100]

    contract_deciles = [25, 50, 75]


    def get_contrac_deciles(percent):
            cur_decile = min(contract_deciles)
            for decile in contract_deciles:
                if percent >= decile:
                    cur_decile = decile
            if percent == 0:
                return f"В группе нет контрактников"
            elif cur_decile == min(contract_deciles):
                return f"% контрактников в группе < {cur_decile}"
            elif cur_decile == 100:
                return f"В группе все контракники"
            else:
                return f"% контрактников в группе >= {cur_decile}"
            

    def rename_column(name):
        return name[name.index("_") + 1:]


    def is_high_score(ege_marks):
            if ege_marks >= 80:
                return "Высокобалльник"
            return "Не высокобалльник"


    def is_local(city):
            if city.lower().replace(" ", "") == "г.челябинск":
                return "Местный"
            return "Иногородний"


    def get_description_for_expelled(status):
        if isinstance(status, int):
            return f"Отчислен на {status} семестре"
        return status.capitalize()
    

    def get_description_financial_form(form):
        if form.lower().replace(" ", "") == "контракт":
            return "Контрактник"
        elif form.lower().replace(" ", "") == "бюджет":
            return "Бюджетник"


    def get_begining_year_discription(year):
        return "Поступил в " + str(year)
        

    def get_mark_deciles(percent):
        cur_decile = min(mark_deciles)
        for decile in mark_deciles:
            if percent >= decile:
                cur_decile = decile
        if percent < min(mark_deciles):
            return f"% оценок выше 4 < {cur_decile}"
        elif cur_decile == 100:
            return f"% оценок выше 4 = {cur_decile}"
        else:
            return f"% оценок выше 4 >= {cur_decile}"


    def get_term_discription(term):
        return str(term) + " Семестр"


    def is_retake(mark):
        if mark <= 2:
            return "Пересдача"
        return "Без пересдачи"
    

    students = students.dropna(subset="RegisterCity")

    # add Id for future merging 
    student_transactions = pd.DataFrame()
    student_transactions["StudentId"] = students.drop_duplicates(
        subset='Id')["Id"]
    student_transactions.reset_index(inplace=True, drop=True)

    # add produst "Budget" or "Contract" financial form of education
    student_transactions = student_transactions.join(
        students.drop_duplicates(subset="Id").set_index("Id")["FinancialForm"].apply(get_description_financial_form), 
        on="StudentId")
    
    # add produst Percentage of сontractors
    percentage_of_сontractors = students.groupby(["JournalId"], as_index=0)["FinancialForm"].apply(lambda x: (x == "контракт").sum() / len(x) * 100)
    percentage_of_сontractors["Percentage of сontractors"] = percentage_of_сontractors["FinancialForm"].apply(get_contrac_deciles)
    percentage_of_сontractors.drop("FinancialForm", axis=1, inplace=True)
    percentage_of_сontractors = students.join(percentage_of_сontractors.set_index("JournalId"), on="JournalId")
    student_transactions = student_transactions.join(
            percentage_of_сontractors.drop_duplicates(subset="Id").set_index("Id")["Percentage of сontractors"], 
            on="StudentId")

    # add a product "Local" or "From other city"
    student_transactions = student_transactions.join(
        students.drop_duplicates(subset="Id")
                .set_index("Id")["RegisterCity"]
                .apply(lambda x: is_local(x)),
        on="StudentId")
    student_transactions

    # add a product "Ege high grades" or "No ege high grades"
    mean_ege = ege.groupby(by="StudentId", as_index=False)["Mark"].mean()
    mean_ege["Mark"] = mean_ege["Mark"].apply(lambda x: is_high_score(x))
    student_transactions = student_transactions.join(mean_ege.set_index("StudentId"), on="StudentId")

    # add a product of student education status
    tmp = student_transactions.join(
        students.drop_duplicates(subset='Id').set_index("Id")["Status"], 
        on="StudentId")
    journals_with_terms = pd.DataFrame()
    journals_with_terms["JournalId"] = journals["Id"]
    journals_with_terms["Term"] = (
        journals["CourseNumber"] - 1
        ) * 2 + journals["Term"]
    students_with_journal_terms = students.join(
        journals_with_terms.set_index("JournalId"), on="JournalId")
    max_term_by_student_id = students_with_journal_terms.groupby(
        'Id'
        )['Term'].max()
    tmp = tmp.join(max_term_by_student_id, on="StudentId")
    condition = tmp["Status"] == "отчислен"
    tmp.loc[condition, "Status"] = tmp.loc[condition, "Term"]
    tmp['Status'] = tmp['Status'].apply(get_description_for_expelled)
    student_transactions = student_transactions.join(
        tmp.set_index("StudentId")["Status"], on="StudentId")

    # add a product of education begining
    tmp = journals
    tmp["BeginYear"] = tmp["Year"] - (tmp["CourseNumber"] - 1)
    tmp = students.drop_duplicates(subset="Id").join(
        tmp.set_index("Id")["BeginYear"], 
        on="JournalId")
    tmp["BeginYear"] = tmp["BeginYear"].apply(get_begining_year_discription)
    student_transactions = student_transactions.join(
        tmp.set_index("Id")["BeginYear"], 
        on="StudentId")

    # add a products of term number and grades percentage of ratings above 4
    grades_by_terms = grades.copy()[["StudentId", "JournalId"]]
    grades_by_terms["GradeDicile"] = (grades["Grade"] / grades["GradeMax"] * 5)
    grades_by_terms.drop(grades_by_terms[grades_by_terms["GradeDicile"] > 5].index, axis=0, inplace=True)
    grades_by_terms = grades_by_terms.join(journals_with_terms.set_index("JournalId"), on="JournalId")
    grades_by_terms = grades_by_terms.groupby(['StudentId', 'Term'], as_index=0)['GradeDicile'].apply(lambda x: (x >= 4).sum() / len(x) * 100)
    grades_by_terms['GradeDicile'] = grades_by_terms['GradeDicile'].apply(get_mark_deciles)
    result_transactions = grades_by_terms.join(student_transactions.set_index("StudentId"), on="StudentId")
    

    # add a products of retake status
    ratings_with_retake = ratings.join(
        journals_with_terms.set_index("JournalId"), on="JournalId")
    ratings_with_retake = ratings_with_retake.groupby(
        ["StudentId", "Term"], 
        as_index=0
        )["Mark"].min()
    ratings_with_retake["IsRetake"] = ratings_with_retake["Mark"].apply(
        is_retake)
    result_transactions = result_transactions.merge(
        ratings_with_retake.drop(columns=["Mark"]), 
        on=["StudentId", "Term"], 
        how="left")

    # some data cleaning
    result_transactions["Term"] = result_transactions["Term"].apply(
        get_term_discription)
    result_transactions.drop(columns="StudentId", inplace=True)
    result_transactions.dropna(inplace=True)
    result_transactions.reset_index(inplace=True, drop=True)

    # convertion to sparse matrix
    transactions_matrix = pd.get_dummies(result_transactions)

    # editing column names
    for column in transactions_matrix.columns:
        transactions_matrix.rename(
            columns={column: rename_column(column)},
            inplace=True)

    return transactions_matrix