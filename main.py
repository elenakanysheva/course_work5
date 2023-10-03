from dbmanager import DBManager

import psycopg2

from config import config

from utils import insert, get_formatted_vacancies, \
    execute_sql_script, create_vacancies_table, create_company_table, add_foreign_keys, create_database, \
    get_company_data, get_companies_by_ids, get_vacancies_by_ids, insert_data


def main(company_data=None):
    script_file = 'fill_db.sql'
    db_name = 'vacancies_db'
    company_ids = get_company_data('company.json')
    #print(company_ids)

    companies = get_companies_by_ids(company_ids)
    #print(companies)

    vacancies = get_vacancies_by_ids(company_ids)
    #print(vacancies[0])

    formatted_vacancies = get_formatted_vacancies(vacancies)
    #print(formatted_vacancies)

    insert(vacancies_json=formatted_vacancies)

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_vacancies_table(cur)
                print("Таблица vacancies успешно создана")

                create_company_table(cur)
                print("Таблица company успешно создана")

                add_foreign_keys(cur)
                print(f"FOREIGN KEY успешно добавлен")

                insert_data(cur, companies, formatted_vacancies)
                print("Данные в таблицы успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    dbmanager = DBManager(**params)

    print(dbmanager.get_all_vacancies())
    print(dbmanager.get_avg_salary())
    print(dbmanager.get_vacancies_with_keyword("Python"))
    print(dbmanager.get_companies_and_vacancies_count())
    print(dbmanager.get_vacancies_with_higher_salary())


if __name__ == '__main__':
    main()
