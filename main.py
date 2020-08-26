import lib_main
from statistics import mean


if __name__ == '__main__':
    df = lib_main.getFreshData('findcsystem')
    scored_employees = lib_main.score_employees(df, 63, 7)
    unified_employee_scores = lib_main.unify_employee_scores(scored_employees, mean)

    lib_main.insert_data_into_gbq(scored_employees, 'findcsystem', 'xsolla_summer_school', 'score_result_status')
    lib_main.insert_data_into_gbq(unified_employee_scores, 'findcsystem', 'xsolla_summer_school', 'score_result_total')
