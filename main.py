from utils import *
from config import *
from bd_manager import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    employer_ids = [78638, 84585, 3529, 633069, 1740, 1375441, 1272486, 2324020, 1122462, 15478]
    params = config()
    create_database('hh', params)
    create_tables('hh', params)
    get_employers(employer_ids)
    get_vacancies(employer_ids)
    save_data_to_database('hh', employer_ids, params)
    db_manager = DBManager()
    db_manager.get_companies_and_vacancies_count()
    db_manager.get_all_vacancies()
    db_manager.get_avg_salary()
    db_manager.get_vacancies_with_keyword('python')


