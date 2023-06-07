from dash import dcc, html, Dash, callback, dash_table
from dash.dependencies import Input, Output, State
import pandas as pd
import json
from constants import *
import base64
from data_converter import convert_to_transactions
from pattern_miner import mine_patterns
from data_requester import get_data, get_data_params
import asyncio
import zipfile
import io


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)

params = asyncio.run(get_data_params([i for i in (range(2010, 2023))]))

files = {JOURNALS_FILE_NAME: None, STUDENTS_FILE_NAME: None, 
         GRADES_FILE_NAME: None, RATINGS_FILE_NAME: None, EGE_FILE_NAME: None}

patterns = None

url_bar_and_content_div = html.Div([
    html.Div(style={'font-size': '24px'},
            children=[
                # html.Div(
                #     style={
                #         'display': 'flex',
                #         'flex-direction': 'column',
                #         'align-items': 'center',
                #     },
                #     children=[
                #         html.H1('Поиск шаблонов', style={'margin-bottom': '20px'}),
                #     ]
                # ),
                html.Div(
                    style={'display': 'flex', 'justify-content': 'center'},
                    children=[
                        dcc.Link('Получение данных', href='/page-1', style={'margin': '10px', 'text-decoration': 'none', 'color': '#333', 'padding': '10px 20px'}),
                        dcc.Link('Поиск шаблонов', href='/', style={'margin': '10px', 'text-decoration': 'none', 'color': '#333', 'padding': '10px 20px'}),
                    ]
                )
            ]
        ),
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

layout_index = html.Div(
    children=[
        html.Div(
            style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'padding': '20px'},
            children=[
                html.Div([
                    html.Label('Загрузить данные:'),
                    dcc.Upload(
                        id="upload-json-data",
                        children=html.Div([
                            'Перетащите или ',
                            html.A('выберите файл')
                        ]),
                        style={
                            'width': '500px',
                            'height': '50px',
                            'line-height': '50px',
                            'border-width': '1px',
                            'border-style': 'dashed',
                            'border-radius': '5px',
                            'text-align': 'center',
                        },
                        multiple=True
                    ),
                ]),
            ]
        ),
        html.Div(
            style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'margin-right': '10px'},
            children=[
                html.Div(id='output-upload-json-data', style={'width': '490px'}),
            ]),
        html.Div(
            style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'padding': '10px'},
            children=[
                html.Div(style={'display': 'flex', 'justify-content': 'center', 'flex-direction': 'column'}, 
                        children=[
                            html.Div([
                                html.Label('Введите поддержку:'),
                                dcc.Input(id='sup', type='number', value=0.1, min=0, step="any", style={'width': '500px'}),
                            ], style={'margin': '10px'}),
                            html.Div([
                                html.Label('Введите достоверность:'),
                                dcc.Input(id='conf', type='number', value=0.1, min=0, step="any", style={'width': '500px'}),
                            ], style={'margin': '10px'}),
                            html.Div([
                                html.Label('Введите подъем:'),
                                dcc.Input(id='lift', type='number', value=1, min=0, step="any", style={'width': '500px'}),
                            ], style={'margin': '10px'}),
                            html.Div([
                                html.Label('Максимум элементов слева:'),
                                dcc.Input(id='left_el', type='number', value=3, min=1, step=1, style={'width': '500px'}),
                            ], style={'margin': '10px'}),
                            html.Div([
                                html.Label('Максимум элементов справа:'),
                                dcc.Input(id='right_el', type='number', value=1, min=1, step=1, style={'width': '500px'}),
                            ], style={'margin': '10px'}),
                            html.Div([
                                    html.Button('Выполнить поиск', id='search-btn', n_clicks=0, style={'width': '500px', 'height': '50px', 'font-size': '12px'}),
                                    ],
                                    style={"margin-left":"10px", 'margin': '10px'}
                                ),
                        ],
                ),
                html.Div(
                    style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center'},
                    children=[
                        html.Div(
                            style={'display': 'flex', 'justify-content': 'space-between', 'margin-top': '10px'},
                            children=[
                                html.Div(
                                    children=[
                                        html.Label('Имя скачиваемого файла эксель:'),
                                        dcc.Input(
                                            id='excel-filename-input', 
                                            type='text', 
                                            value='Шаблоны', 
                                            style={'width': '290px'}),
                                    ],
                                    style={'margin-left': '10px'}
                                ),
                                html.Div([
                                    # html.Button('Просмотр'),
                                    # html.Div(style={'margin-top': '10px'}),
                                    html.Button('Скачать шаблоны', id="export-btn", n_clicks=0, style={'height': '50px'}),
                                    html.Div(id="output-export-btn"),
                                    dcc.Download(id="download-dataframe-xlsx"),
                                ], style={'display': 'flex', 'flex-direction': 'column', 'margin': "10px", 'margin-left': '20px'})
                            ]
                        ),
                    ]
                ),
                html.Div(
                    style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center'},
                    children=[
                        html.Div(id='output-search-btn', style={'text-align': 'center', "margin": "20px"})
                    ]
                )
            ]
        )
    ],
)

layout_page_1 = html.Div([
    html.Div(
        style={'display': 'flex', 'flex-direction': 'column', 'align-items': 'center', 'padding': '20px'},
        children=[
            html.Div(
                children=[
                    html.Label('Выберите года:'),
                    dcc.Dropdown(
                        id='year-dropdown',
                        options=[{'label': year, 'value': year} for year in params["Year"].unique()],
                        value=[],
                        multi=True,
                        optionHeight=50,
                        style={'width': '500px', 'maxWidth': '500px'}
                    )
                ],
                style={'margin-bottom': '20px'}
            ),
            html.Div(
                children=[
                    html.Label('Выберите квалификации:'),
                    dcc.Dropdown(
                        id='qualification-dropdown', 
                        multi=True,
                        optionHeight=50,
                        style={'width': '500px', 'maxWidth': '500px'}
                    )
                ],
                style={'margin-bottom': '20px'}
            ),
            html.Div(
                children=[
                    html.Label('Выберите специальности:'),
                    dcc.Dropdown(
                        id='directions-dropdown',
                        multi=True,
                        optionHeight=50,
                        style={'width': '500px', 'maxWidth': '500px'}
                    )
                ],
                style={'margin-bottom': '20px'},
            ),
            html.Div([
                html.Button('Получить данные', 
                    id='get-data-btn', 
                    n_clicks=0,
                    style={'height': '50px', 'width': '500px'}),
                html.Div(id='output-get-data-btn')
                ],
                style={'margin-bottom': '20px'}
            ),  
            html.Div(
                style={'display': 'flex', 'justify-content': 'space-between', 'margin-top': '10px'},
                children=[
                    html.Div(
                        children=[
                            html.Label('Скачать архив с именем:'),
                            dcc.Input(
                                id='filename-input', 
                                type='text', 
                                value='data', 
                                style={'width': '280px'}),
                        ],
                        style={'margin-left': '10px'}
                    ),
                    html.Div([
                        html.Button('Скачать данные', 
                            id="download-data-button", 
                            n_clicks=0, 
                            style={'height': '50px', 'width': '200px'}),
                        dcc.Download(id="download-data-zip"),
                    ], style={'display': 'flex', 'flex-direction': 'column', 'margin': '12px', 'margin-left': '20px'}
                    )
                ]
            ),
        ]
    ),
])

# index layout
app.layout = url_bar_and_content_div

# "complete" layout
app.validation_layout = html.Div([
    url_bar_and_content_div,
    layout_index,
    layout_page_1
])


# Index callbacks
@callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    """
    Функция обработчик для обновления содержимого страницы на основе пути URL.

    Параметры:
    - pathname (str): Путь URL.

    Возвращает:
    - Объект, представляющий содержимое страницы для отображения.
    """
    if pathname == "/page-1":
        return layout_page_1
    else:
        return layout_index
    

@app.callback(Output("download-dataframe-xlsx", "data"),
          Input("export-btn", "n_clicks"),
          State('excel-filename-input', "value"),
          prevent_initial_call=True)
def download_excel(n_clicks, filename):
    """
    Функция обработчик для скачивания данных в формате Excel (.xlsx).

    Параметры:
    - n_clicks (int): Количество кликов на кнопку скачивания.
    - filename (str): Имя файла для сохранения.

    Возвращает:
    - Объект, представляющий данные файла Excel для скачивания.
    """
    if n_clicks > 0 and isinstance(patterns, pd.DataFrame):
        return dcc.send_data_frame(patterns.to_excel, filename + ".xlsx", sheet_name="Sheet_name_1")

    
def check_files():
    """
    Функция для проверки наличия загруженных файлов.

    Возвращает:
    - Кортеж из двух строк: найденные файлы и недостающие файлы.
    """
    found_files = []
    missing_files = []
    for filename, data in files.items():
        if data is not None:
            found_files.append(filename)
        else:
            missing_files.append(filename)
    
    missing_files = ", ".join(missing_files)
    found_files = ", ".join(found_files)

    return found_files, missing_files


def parse_json_data(contents, filenames):
    """
    Функция для разбора и обработки данных JSON, полученных из загруженных файлов.

    Параметры:
    - contents (List[str]): Список содержимого загруженных файлов.
    - filenames (List[str]): Список имен загруженных файлов.

    Возвращает:
    - Объект, представляющий результат разбора и обработки данных.
    """      
    for content, filename in zip(contents, filenames):
        if filename.endswith('.zip'):
            try:
                content_type, content_string = content.split(',')
                decoded = base64.b64decode(content_string)
                zip_str = io.BytesIO(decoded)
                zip_data = zipfile.ZipFile(zip_str)
                for json_file in zip_data.infolist():
                    if json_file.filename in files:
                        json_data = zip_data.read(json_file)
                        json_data = json.loads(json_data)
                        df = pd.DataFrame(json_data)
                        files[json_file.filename] = df
            except Exception as e:
                print(e)
                return html.Div(["Ошибка при распаковке ZIP-архива"])
        elif filename in files.keys():
            try:
                content_type, content_string = content.split(',')
                decoded = base64.b64decode(content_string)
                json_data = json.loads(decoded)
                df = pd.DataFrame(json_data)
                files[filename] = df
            except Exception as e:
                print(e)
                return html.Div(["Ошибка при загрузке файла JSON"])
        else:
            return html.Div(["Неверное имя загружаемого файла"])
    
    
@app.callback(Output('output-upload-json-data', 'children'),
              Input('upload-json-data', 'contents'),
              State('upload-json-data', 'filename'),
              State('upload-json-data', 'last_modified'))
def update_output(list_of_contents, list_of_names, list_of_dates):
    """
    Функция обработчик для обновления вывода после загрузки данных JSON.

    Параметры:
    - list_of_contents (List[str]): Список содержимого загруженных файлов.
    - list_of_names (List[str]): Список имен загруженных файлов.
    - list_of_dates (List[int]): Список временных меток загруженных файлов.

    Возвращает:
    - Объект, представляющий обновленный вывод для отображения.
    """
    warning = html.Div()
    if list_of_contents is not None:
        warning = parse_json_data(list_of_contents, list_of_names)
    found_files, missing_files = check_files()
    if len(missing_files) == 0:
        return html.Div([
            warning,
            f"Найдены все нужные файлы: {found_files}"
            ])
    elif len(found_files) == 0:
        return html.Div([
            warning,
            f"Добавьте файлы: {missing_files}"
            ])
    else:
        return html.Div([
            warning,
            html.Div(f"Найдены файлы: {found_files}"),
            html.Div(f"Добавьте файлы: {missing_files}")
            ])

    
@app.callback(Output('output-search-btn', 'children'),
              Input('search-btn', 'n_clicks'),
              State('sup', 'value'),
              State('conf', 'value'),
              State('lift', 'value'),
              State('left_el', 'value'),
              State('right_el', 'value'))
def get_data_from_api(n_clicks, sup, conf, lift, max_left_elements, max_right_elements):
    """
    Функция обработчик для получения данных из API.

    Параметры:
    - n_clicks (int): Количество кликов на кнопку получения данных.
    - sup (float): Значение для поддержки (support).
    - conf (float): Значение для достоверности (confidence).
    - lift (float): Значение для уровня связи (lift).
    - max_left_elements (int): Максимальное количество элементов в антецедентах.
    - max_right_elements (int): Максимальное количество элементов в консеквентах.

    Возвращает:
    - Объект, представляющий результат получения данных для отображения.
    """
    def cell_to_string(cell):
        if not isinstance(cell, str):
            try:
                iter(cell)
                string = ""
                for item in cell:
                    string += f"({item}), "
                string = string[:-2]
                return string
            except TypeError:
                pass
        return cell
    

    global patterns
    if n_clicks > 0:
        for value in files.values():
            if not isinstance(value, pd.DataFrame):
                return html.Span("Вы загрузили не все файлы", id="notification", style={})
        transactions = convert_to_transactions(
           files[JOURNALS_FILE_NAME],
           files[STUDENTS_FILE_NAME],
           files[EGE_FILE_NAME],
           files[GRADES_FILE_NAME],
           files[RATINGS_FILE_NAME]  
        )
        patterns = mine_patterns(transactions, sup, lift, conf)
        patterns["antecedents"] = patterns["antecedents"].apply(lambda x: x if len(x) <= max_left_elements else None)
        patterns["consequents"] = patterns["consequents"].apply(lambda x: x if len(x) <= max_right_elements else None)
        patterns["antecedents"] = patterns["antecedents"].apply(lambda x: cell_to_string(x))
        patterns["consequents"] = patterns["consequents"].apply(lambda x: cell_to_string(x))
        patterns.dropna(inplace=True)

        return dash_table.DataTable(patterns.to_dict('records'),
                                    [{"name": i, "id": i} for i in patterns.columns], 
                                    id='tbl',
                                    filter_action="native",
                                    sort_action="native",
                                    sort_mode="multi",
                                    style_data={
                                        'width': '100px', 'minWidth': '100px', 'maxWidth': '700px',
                                        'overflow': 'hidden',
                                        'textOverflow': 'ellipsis',
                                        'font-size': '16px',
                                    },
                                    style_header={
                                        'fontSize': '18px',
                                    },
                                    style_filter={
                                        'fontSize': '18px',
                                    })


# Page 1 callbacks
@app.callback(
    Output('qualification-dropdown', 'options'),
    Input('year-dropdown', 'value')
)
def update_dish_dropdown(selected_years):
    """
    Функция обновления вариантов выбора для выпадающего списка квалификаций.

    Параметры:
    - selected_years (List[str]): Выбранные годы.

    Возвращает:
    - Список вариантов выбора для выпадающего списка квалификаций.
    """
    if selected_years:
        selected_params = params[params["Year"].isin(selected_years)]
        options = [{'label': qualifiation, 'value': qualifiation} for qualifiation in selected_params["Speciality"].unique()]
    else:
        options = []
    return options


@app.callback(
    Output('directions-dropdown', 'options'),
    Input('qualification-dropdown', 'value'),
    State('year-dropdown', 'value')
)
def update_dish_dropdown(selected_qualifications, selected_years):
    """
    Функция обновления вариантов выбора для выпадающего списка направлений.

    Параметры:
    - selected_qualifications (List[str]): Выбранные квалификации.
    - selected_years (List[str]): Выбранные годы.

    Возвращает:
    - Список вариантов выбора для выпадающего списка направлений.
    """
    if selected_qualifications:
        selected_params = params[params["Year"].isin(selected_years)]
        selected_params = selected_params[selected_params["Speciality"].isin(selected_qualifications)]
        options = [{'label': specialization, 'value': specialization} for specialization in selected_params["DirectionName"].unique()]
    else:
        options = []
    return options


@app.callback(Output('output-get-data-btn', 'children'),
              Input('get-data-btn', 'n_clicks'),
              State('year-dropdown', 'value'),
              State('qualification-dropdown', 'value'),
              State('directions-dropdown', 'value'))
def get_data_from_api(n_clicks, years, qualifications, directions):
    """
    Функция обработчик для получения данных из API на странице 1.

    Параметры:
    - n_clicks (int): Количество кликов на кнопку получения данных.
    - years (List[str]): Выбранные годы.
    - qualifications (List[str]): Выбранные квалификации.
    - directions (List[str]): Выбранные направления.

    Возвращает:
    - Объект, представляющий результат получения данных на странице 1 для отображения.
    """
    global files
    if n_clicks > 0:
        if years and qualifications and directions:
            data = asyncio.run(get_data(years, qualifications, directions))
            files[JOURNALS_FILE_NAME], files[STUDENTS_FILE_NAME], files[GRADES_FILE_NAME], files[RATINGS_FILE_NAME], files[EGE_FILE_NAME] = data
            return html.Div("Данные успешно получены")
        else: 
            return html.Div("Заполнены не все поля")
    

@app.callback(Output('download-data-zip', 'data'),
              Input('download-data-button', 'n_clicks'),
              State('filename-input', 'value'))
def download_data(n_clicks, filename):
    """
    Функция обработчик для скачивания данных в формате ZIP.

    Параметры:
    - n_clicks (int): Количество кликов на кнопку скачивания данных.
    - filename (str): Имя файла для сохранения.

    Возвращает:
    - Данные для скачивания ZIP-файла.
    """ 
    if n_clicks > 0 and filename:
        _, missing_files = check_files()
        if len(missing_files) == 0:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zf:
                zf.writestr(JOURNALS_FILE_NAME, files[JOURNALS_FILE_NAME].to_json())
                zf.writestr(STUDENTS_FILE_NAME, files[STUDENTS_FILE_NAME].to_json())
                zf.writestr(GRADES_FILE_NAME, files[GRADES_FILE_NAME].to_json())
                zf.writestr(RATINGS_FILE_NAME, files[RATINGS_FILE_NAME].to_json())
                zf.writestr(EGE_FILE_NAME, files[EGE_FILE_NAME].to_json())
            zip_data = zip_buffer.getvalue()
            return dcc.send_bytes(zip_data, filename=filename + ".zip", mimetype='application/zip')
        

if __name__ == '__main__':
    app.run_server(debug=True)