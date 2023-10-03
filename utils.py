import json

import requests

import psycopg2


def get_vacancies(url):
    response = requests.get(url)
    return response.json()["items"]


def insert(vacancies_json):
    with open('data.json', "w", encoding="utf-8") as file:
        json.dump(vacancies_json, file, indent=4, ensure_ascii=False)


def get_company(url):
    response = requests.get(url)
    return response.json()


def get_formatted_company(companies):
    formatted_companies = []
    for company in companies:
        formatted_company = {
            "id": company["id"],
            "name": company['name'],
            "city": company["area"]["name"],
            "url": company["url"]
        }

        formatted_companies.append(formatted_company)

    return formatted_companies


def get_formatted_vacancies(vacancies):
    """Форматирует данные о вакансиях"""
    formatted_vacancies = []
    for vacancy in vacancies:

        formatted_vacancy = {
            "id": vacancy["id"],
            "name": vacancy['name'],
            "salary_from": vacancy['salary']['from'] if isinstance(vacancy["salary"], dict) and vacancy["salary"][
                    'from'] else 0,
            "salary_to": vacancy["salary"]['to'] if isinstance(vacancy["salary"], dict) and vacancy["salary"][
                    'to'] else 0,
            "url": vacancy["url"],
            "company_id": vacancy["employer"]["id"]
        }

        formatted_vacancies.append(formatted_vacancy)

    return formatted_vacancies


def get_companies_by_ids(ids):
    """Получает данные о работодателях по id"""
    all_companies = []
    for cid in ids:
        url = "https://api.hh.ru/employers/" + str(cid)
        result = requests.get(url)
        result_data = result.json()
        all_companies.append({'id': str(cid), 'name': result_data['name'], 'city': result_data['area']['name'],
                              'url': result_data['alternate_url']})
    return all_companies


def get_vacancies_by_ids(ids):
    """Получает данные о вакансиях по id компаний"""
    all_vacancies = []
    for cid in ids:
        url = "https://api.hh.ru/vacancies?employer_id=" + str(cid)
        result = requests.get(url)
        result_data = result.json()
        all_vacancies.extend(result_data['items'])
    return all_vacancies


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")

    conn.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as sql:
        cur.execute(sql.read())


def create_vacancies_table(cur) -> None:
    """Создает таблицу vacancies."""
    cur.execute("""
                CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    vacancy_name VARCHAR(200) NOT NULL,
                    salary_from integer,
                    salary_to integer,
                    url VARCHAR(100)                    
                )
            """)


def create_company_table(cur) -> None:
    """Создает таблицу companies"""
    cur.execute("""
                CREATE TABLE companies (
                    company_id SERIAL PRIMARY KEY,
                    company_name VARCHAR(200) NOT NULL,
                    city  VARCHAR(30),
                    url VARCHAR(100)                                    
                )
            """)


def get_company_data(json_file: str) -> list[dict]:
    """Извлекает данные о компаниях из JSON-файла и возвращает список словарей с соответствующей информацией."""
    with open(json_file, "r") as file:
        company_data = json.loads(file.read())
        return [company['id'] for company in company_data]


def insert_data(cur, companies: list[dict], vacancies: list[dict]) -> None:
    """Добавляет данные из companies в таблицу companies."""
    for company in companies:
        cur.execute("""
                INSERT INTO companies (company_name, city, url)
                VALUES (%s, %s, %s)
                RETURNING company_id
                """,
                    (company['name'], company['city'], company['url']))
        company_id = cur.fetchone()[0]
        for vacanci in vacancies:
            if company['id'] == vacanci['company_id']:

                cur.execute("""
                        INSERT INTO vacancies (company_id, vacancy_name, salary_from, salary_to, url)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                            (company_id, vacanci['name'], vacanci['salary_from'], vacanci['salary_to'],
                             vacanci['url']))


def add_foreign_keys(cur) -> None:
    """Добавляет foreign key со ссылкой на company_id в таблицу vacancies"""
    cur.execute("ALTER TABLE vacancies ADD COLUMN company_id int")
    cur.execute("ALTER TABLE vacancies ADD CONSTRAINT fk_vacancies_companies FOREIGN KEY(company_id) "
                "REFERENCES companies (company_id)")
