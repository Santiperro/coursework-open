import pandas as pd
from constants import *
import asyncio
from aiohttp import ClientSession
from tenacity import retry, wait_fixed, stop_after_attempt


@retry(wait=wait_fixed(1), stop=stop_after_attempt(3))
async def make_request(url, semaphore):
        """
        Функция отправляет HTTP GET-запрос на указанный `url` и возвращает 
        ответ в формате JSON.

        Параметры:
        - url (str): URL, на который отправляется HTTP GET-запрос.

        Возвращает:
        - Объект `dict`, представляющий JSON-ответ на GET-запрос.
        """
        async with semaphore:
            async with ClientSession() as session:
                async with session.get(url) as response:
                    return await response.json()


async def get_journals_by_year(years):
    """
    Функция получает данные о журналах за указанные годы.
    Она создает несколько задач для выполнения запросов с использованием 
    функции `make_request` и объединяет результаты в один объект Pandas 
    DataFrame.

    Параметры:
    - years (list): Список целых чисел, представляющих годы, для которых 
    запрашиваются данные о журналах.

    Возвращает:
    - Объект Pandas DataFrame, содержащий данные о журналах.
    """
    semaphore = asyncio.Semaphore(100)
    tasks = []
    for year in years:
        tasks.append(asyncio.create_task(
            make_request(JOURNALS_BY_YEAR_REQUEST_URL + str(year), semaphore)))

    journals = pd.DataFrame()
    for task in tasks:
        journals_by_year = pd.DataFrame.from_dict(await task)
        journals = pd.concat([journals, journals_by_year], 
                            ignore_index=True)
    
    return journals.drop_duplicates()


async def get_and_bind_table_with_id(url, foreign_ids, foreign_id_column_name):
        """
        Функция получает данные с указанного `url` для каждого `foreign_id`.
        Она создает несколько задач для выполнения запросов с использованием 
        функции `make_request` и объединяет результаты в один объект Pandas 
        DataFrame. Кроме того, она добавляет столбец в DataFrame, содержащий 
        внешний идентификатор (`foreign_id`).

        Параметры:
        - url (str): URL, на который отправляются запросы.
        - foreign_ids (list): Список внешних идентификаторов.
        - foreign_id_column_name (str): Название столбца, в котором будет 
        сохранен внешний идентификатор.

        Возвращает:
        - Объект Pandas DataFrame, содержащий данные из запросов.
        """
        semaphore = asyncio.Semaphore(100)
        tables = pd.DataFrame()
        tasks = {}
        for id in foreign_ids:
            tasks[asyncio.create_task(
                make_request(url + str(id), semaphore)
            )] = id
        for task, id in tasks.items():
            table_by_id = pd.DataFrame.from_dict(await task)
            table_by_id[foreign_id_column_name] = id

            tables = pd.concat([tables, table_by_id], 
                                 ignore_index=True)

        return tables.drop_duplicates()


async def get_params(years):
    """
    Эта функция сохраняет параметры журналов в формате JSON.
    Она вызывает функцию get_journals_by_year для получения данных о
    журналах, затем фильтрует и сохраняет только необходимые параметры.

    Параметры:
    - years (list): Список целых чисел, представляющих годы, для которых 
    запрашиваются данные о журналах.

    Возвращает:
    - Объект Pandas DataFrame, содержащий параметры журналов.
    """
    journals = await get_journals_by_year(years)
    params = journals.filter(["Year", "DirectionName", "Speciality"])
    return params.drop_duplicates()


async def request(years, qualifications, direction_names):
    """
    Эта функция выполняет запросы и сохраняет данные о журналах, студентах, 
    оценках, рейтингах и ЕГЭ.

    Параметры:
    - years (int, list): Год или список лет, для которых запрашиваются данные 
    о журналах.
    - qualifications (str, list): Квалификация или список квалификаций, по 
    которым фильтруются данные о журналах.
    - direction_names (str, list): Направление или список направлений, по 
    которым фильтруются данные о журналах.

    Возвращает:
    - Объекты Pandas DataFrame, содержащие данные о журналах, студентах, 
    оценках, рейтингах и ЕГЭ.
    """
    def to_list_if_not(obj):  # Returns the list if the object is not sequence
        try:
            iter(obj)
            if type(obj) is not str:
                return obj   
        except TypeError:
            pass
        return [obj]
        
        
    years = to_list_if_not(years)
    qualifications = to_list_if_not(qualifications)
    direction_names = to_list_if_not(direction_names)
        
    # getting journals 
    journals = await get_journals_by_year(years)
    if direction_names:
        journals = journals[
            journals["DirectionName"].isin(direction_names)
            ]
    if qualifications:
        journals = journals[
            journals["Speciality"].isin(qualifications)
            ]
    journal_ids = journals['Id'].unique()

    # getting students
    students = await get_and_bind_table_with_id(STUDENTS_BY_JOURNAL_REQUEST_URL, journal_ids, "JournalId")
    students_ids = students["Id"].unique()

    # getting grades
    grades = await get_and_bind_table_with_id(GRADES_BY_JOURNAL_REQUEST_URL, journal_ids, "JournalId")
    
    # getting ratings
    ratings = await get_and_bind_table_with_id(RATINGS_BY_JOURNAL_REQUEST_URL, journal_ids, "JournalId")

    # getting ege
    ege = await get_and_bind_table_with_id(EGE_BY_STUDENT_REQUEST_URL, students_ids, "StudentId")

    return journals, students, grades, ratings, ege


# asyncio.run(request_and_save(r"D:\Work Data\SusuRepos\coursework\dash_patterm_mining\json_databig_data", 
#                  [2019, 2020, 2021], 
#                  "бакалавр", 
#                 ['Бизнес-информатика', 'Реклама и связи с общественностью',
#        'Информационная безопасность', 'Технология транспортных процессов',
#        'Зарубежное регионоведение', 'Техносферная безопасность',
#        'Прикладная математика и информатика']))