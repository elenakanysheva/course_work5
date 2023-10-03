import psycopg2


class DBManager:

    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: str = '5432'):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        self.cur = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        '''получает список всех компаний и количество вакансий у каждой компании'''
        with self.conn:
            self.cur.execute("""
                            select company_name, count(1) 
                            from vacancies INNER JOIN companies USING (company_id)
                            GROUP BY company_name
                            ORDER BY company_name """
                             )
            return self.cur.fetchall()

    def get_all_vacancies(self):
        '''получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию'''
        with self.conn:
            self.cur.execute("""
                        select vacancy_name, companies.company_name, salary_from, salary_to, vacancies.url
                        from vacancies 
                        INNER JOIN  companies USING (company_id)                        
                        ORDER BY vacancy_name """
                         )
            return self.cur.fetchall()

    def get_avg_salary(self):
        '''получает среднюю зарплату по вакансиям'''
        with self.conn:
            self.cur.execute("""
                        select round(avg(salary_from))
                        from vacancies"""
                         )
            return self.cur.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        '''получает список всех вакансий, у которых зарплата выше средней по всем вакансиям'''
        avg_salary = self.get_avg_salary()
        with self.conn:
            self.cur.execute(f"""select vacancy_id, vacancy_name, salary_from from vacancies
                            where salary_from > {avg_salary}
                            order by salary_from desc
                            """
                          )
            return self.cur.fetchall()

    def get_vacancies_with_keyword(self, word: str):
        '''получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python'''
        with self.conn:
            self.cur.execute(f"""select *
                                from vacancies
                                where vacancy_name like '%{word}%'
                            """
                             )
            return self.cur.fetchall()

