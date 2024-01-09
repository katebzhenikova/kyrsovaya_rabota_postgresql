import psycopg2
from config import *
import os

class DBManager:
    '''Класс для подключения к БД'''

    def get_companies_and_vacancies_count(self):
        '''Метод получает список всех компаний и
        количество вакансий у каждой компании'''
        hh_params = config()
        hh_params['database'] = 'hh'
        conn = psycopg2.connect(**hh_params)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute('''SELECT Работодатель, COUNT(Название_вакансии) AS Всего_вакансий
                        FROM employers 
                        JOIN vacancies USING (Работодатель_id) 
                        GROUP BY employers.Работодатель
                        ''')
            result = cur.fetchall()
        conn.commit()
        return result


    def get_all_vacancies(self):
        '''Метод получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию'''
        hh_params = config()
        hh_params['database'] = 'hh'
        conn = psycopg2.connect(**hh_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute('''SELECT employers.Работодатель, vacancies.Название_вакансии, 
                        vacancies.Зарплата, Вакансия_url FROM employers 
                        JOIN vacancies USING (Работодатель_id)
                        ''')
            result = cur.fetchall()
        conn.commit()
        return result


    def get_avg_salary(self):
        '''Метод получает среднюю зарплату по вакансиям'''
        hh_params = config()
        hh_params['database'] = 'hh'
        conn = psycopg2.connect(**hh_params)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(f"SELECT AVG(Зарплата) as Всего_вакансий FROM vacancies ")
            result = cur.fetchall()
        conn.commit()
        return result


    def get_vacancies_with_higher_salary(self):
        '''Метод получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям'''
        hh_params = config()
        hh_params['database'] = 'hh'
        conn = psycopg2.connect(**hh_params)
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM vacancies "
                        f"WHERE Зарплата > (SELECT AVG(Зарплата) FROM vacancies) ")
            result = cur.fetchall()
        conn.commit()
        return result


    def get_vacancies_with_keyword(self, keyword):
        '''Метод получает список всех вакансий,
        в названии которых содержатся переданные в метод слова'''
        hh_params = config()
        hh_params['database'] = 'hh'
        conn = psycopg2.connect(**hh_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM vacancies "
                        f"WHERE lower(Название_вакансии) LIKE '%{keyword}%' "
                        f"OR lower(Название_вакансии) LIKE '%{keyword}'"
                        f"OR lower(Название_вакансии) LIKE '{keyword}%';")
            result = cur.fetchall()
        conn.commit()
        return result