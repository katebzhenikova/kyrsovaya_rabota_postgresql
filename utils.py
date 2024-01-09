import requests
import psycopg2
from typing import Any
import os


def get_employers(employer_ids):
    employers_list = []
    for employer_id in employer_ids:
        employers_url = f'https://api.hh.ru/employers/{employer_id}'
        params = {
            'area': 1,
            'per_page': 1,
        }
        response = requests.get(employers_url, params=params)
        if response.status_code == 200:
            employers_data = response.json()

            employers = {
                'Работодатель_id': employers_data['id'],
                'Работодатель': employers_data['name'],
            }
            employers_list.append(employers)
        else:
            print('Ошибка обработки данных')
    return employers_list



def get_vacancies(employer_ids):
    vacancies_list = []
    for employer_id in employer_ids:
        employers_url = f'https://api.hh.ru/employers/{employer_id}'
        params = {
            'area': 1,
            'per_page': 1,
        }
        response = requests.get(employers_url, params=params).json()

        vacancies_response = requests.get(response['vacancies_url'])
        if vacancies_response.status_code == 200:
            vacancies_data = vacancies_response.json()
            for v in vacancies_data['items']:
                vacancies = {
                    'Работодатель_id': employer_id,
                    'Вакансия_id': v['id'],
                    'Название_вакансии': v['name'],
                    'Зарплата': v['salary']['from'] if v['salary'] else None,
                    'Описание': v['snippet']['requirement'],
                    'Вакансия_url': v['alternate_url'],
                }
                if vacancies['Зарплата'] is not None:
                    vacancies_list.append(vacancies)
        else:
            print('Ошибка при получении данных о вакансиях:', vacancies_response.status_code)
    return vacancies_list



def create_database(database_name, params):
    '''Создание БД и таблиц для сохранения данных'''
    conn = psycopg2.connect(**params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")
    cur.close()
    conn.close()

def create_tables(database_name, params):
    hh_params = params
    hh_params['database'] = database_name
    new_conn = psycopg2.connect(**hh_params)
    new_conn.autocommit = True

    with new_conn.cursor() as new_cur:
        # Создаем таблицу employers
        new_cur.execute('''
                CREATE TABLE IF NOT EXISTS employers (
                    Работодатель_id INTEGER PRIMARY KEY,
                    Работодатель VARCHAR(255) NOT NULL,
                    Открытых_вакансий INTEGER
                )        
            ''')

        # Создаем таблицу vacancies
        new_cur.execute("""
                CREATE TABLE IF NOT EXISTS vacancies (
                    Вакансия_id SERIAL PRIMARY KEY,
                    Работодатель_id INT REFERENCES employers(Работодатель_id),
                    Название_вакансии TEXT NOT NULL,
                    Зарплата INTEGER,
                    Описание TEXT,
                    Вакансия_url TEXT
                )
            """)
    new_conn.close()


def save_data_to_database(database_name, employer_ids: list, params):
    '''Сохранение данных о работодателях и вакансиях'''
    db_params = params
    db_params['database'] = database_name
    conn = psycopg2.connect(**db_params)
    try:
        with conn:
            with conn.cursor() as cur:
                data_employers = get_employers(employer_ids)
                for i in data_employers:
                    cur.execute('''
                                    INSERT INTO employers (Работодатель_id, Работодатель)
                                    VALUES (%s, %s)                                   
                                    ''', (i['Работодатель_id'], i['Работодатель']))

                data_vacancies = get_vacancies(employer_ids)
                for i in data_vacancies:
                    cur.execute('''
                                    INSERT INTO vacancies (Вакансия_id, Работодатель_id, Название_вакансии, Зарплата, Описание, Вакансия_url)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ''',
                                (i['Вакансия_id'], i['Работодатель_id'], i['Название_вакансии'], i['Зарплата'],
                                 i['Описание'],i['Вакансия_url']))

    except psycopg2.Error as e:
        print(f'An error occurred: {e.pgerror}')
        conn.rollback()
    finally:
        conn.close()

