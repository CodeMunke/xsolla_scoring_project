from google.oauth2 import service_account
import pandas_gbq

import numpy as np
import pandas as pd
import math as mt
import datetime as dt

from statistics import mean


"""[summary]
Funtion for getting fresh data from BigQuery for workload scoring model
[description]
Credentials - google service account object with credentials data for project
SqlQuery - string, sql query for BigQeury database
"""
def getFreshData(Credentials,ProjectId):
    bigquery_sql = " ".join(["SELECT id, DATE(CAST(created_at AS DATETIME)) AS created, DATE(CAST(updated_at AS DATETIME)) AS updated, status, assignee_id",
                             "FROM `xsolla_summer_school.customer_support`",
                             "WHERE status IN ('closed','solved')",
                             "ORDER BY updated_at"])

    dataframe = pandas_gbq.read_gbq(bigquery_sql,project_id=ProjectId, credentials=Credentials, dialect="standard")

    return dataframe


"""[summary]
Function for scoring workload by statuses (In Progress and Done) for one employee, NumOfAllDays = 63, NumOfIntervalDays = 7
[description]
Data - pandas dataframe object, with hist data for customer support agent
NumOfAllDays - integer, number of days for all hist data
NumOfIntervalDays - integer, number of days for weekly calculating interval
"""


def workloadScoringByStatuses(Data, NumOfAllDays, NumOfIntervalDays):
    assignee_id = np.unique(Data.assignee_id)
    assignee_id = assignee_id[0]

    # splitting by status
    statuses = np.unique(Data.status)
    assignee_id_list = []
    status_list = []
    avg_num_of_task_per_week_list = []
    ste_list = []
    num_tasks_per_current_week_list = []
    score_for_status_list = []

    for status in statuses:
        dataframe_status = Data[(Data.status == str(status))][:]

        # time borders params
        curr_date = dt.datetime.strptime(str('2017-04-01'), '%Y-%m-%d')
        curr_date = curr_date.date()
        delta = dt.timedelta(days=NumOfAllDays)
        first_date = curr_date - delta

        # time interval params
        delta_interval = dt.timedelta(days=NumOfIntervalDays)
        first_interval = first_date + delta_interval

        num_of_intervals = int(NumOfAllDays / NumOfIntervalDays)
        num_tasks_per_week = []
        for i in range(0, num_of_intervals):
            interval = dataframe_status[(dataframe_status.updated >= str(first_date)) & (
                        dataframe_status.updated <= str(first_interval))][:]
            first_date = first_date + delta_interval
            first_interval = first_interval + delta_interval

            if i != (num_of_intervals - 1):
                num_of_tasks = len(np.unique(interval['id']))
                num_tasks_per_week.append(num_of_tasks)  # history number of tasks
            else:
                num_tasks_per_current_week = len(np.unique(interval['id']))  # currently number of tasks

        avg_num_of_task_per_week = round(np.mean(num_tasks_per_week), 2)

        # squared deviations
        x_values = []
        for num in num_tasks_per_week:
            x = round((num - avg_num_of_task_per_week) ** 2, 2)
            x_values.append(x)

        # data sampling statistics
        x_sum = round(sum(x_values), 2)  # sum of squared deviations
        dispersion = round(x_sum / (num_of_intervals - 1), 2)  # dispersion
        std = round(mt.sqrt(dispersion), 2)  # standart deviation for sample
        ste = round(std / mt.sqrt(num_of_intervals), 2)  # standart error for sample

        # confidence interval
        left_border = int(avg_num_of_task_per_week - ste)
        right_border = int(avg_num_of_task_per_week + ste)

        # workload scoring for status
        score_for_status = workloadScoreStatuses(left_border, right_border, num_tasks_per_current_week)
        assignee_id_list.append(assignee_id)
        status_list.append(status)
        avg_num_of_task_per_week_list.append(avg_num_of_task_per_week)
        ste_list.append(ste)
        num_tasks_per_current_week_list.append(num_tasks_per_current_week)
        score_for_status_list.append(score_for_status)

    score_data = {"assignee_id": assignee_id_list, "status": status_list,
                  "count_last_period": num_tasks_per_current_week_list,
                  "count_mean_calc_period": avg_num_of_task_per_week_list, "count_sem_calc_period": ste_list,
                  "score_value": score_for_status_list}
    scores = pd.DataFrame(data=score_data)

    return scores


"""[summary]
Function for scoring workload for current status
[description]
LeftBoard - float, left boarder for confidence interval
RightBoard - float right boarder for confidence interval
CurrentNumOfTasks - integer, number of customer support agent tasks for current interval (7 days)
[example]
Input: LeftBoard = 187
       RightBoard = 206
       CurrentNumOfTasks = 196
Output: 1
"""


def workloadScoreStatuses(LeftBoard, RightBoard, CurrentNumOfTasks):
    if (LeftBoard == 0) & (CurrentNumOfTasks == 0) & (RightBoard == 0):
        score = 0
    elif (CurrentNumOfTasks >= 0) & (CurrentNumOfTasks < LeftBoard):
        score = 0
    elif (CurrentNumOfTasks >= LeftBoard) & (CurrentNumOfTasks <= RightBoard):
        score = 1
    else:
        score = 2

    return score


def score_employees(employee_df):
    unique_ids = employee_df['assignee_id'].unique()

    temp = employee_df[employee_df.assignee_id == 12604869947][:]
    temp.reset_index(inplace=True, drop=True)
    temp_status = workloadScoringByStatuses(temp, 63, 7)

    result = temp_status.copy()
    result.drop(result.index, inplace=True)
    for assignee_id in unique_ids:
        user = employee_df[employee_df.assignee_id == assignee_id][:]
        user.reset_index(inplace=True, drop=True)
        user_status = workloadScoringByStatuses(user, 63, 7)
        result = result.append(user_status)

    result.reset_index(inplace=True, drop=True)
    return result


def unify_employee_scores(user_status_df):
    unique_user_ids = user_status_df['assignee_id'].unique()
    assignee_col = []
    score_value_col = []

    for user_id in unique_user_ids:
        user_df = user_status_df[user_status_df.assignee_id == user_id][['assignee_id', 'score_value']]
        score_values = user_df[:]['score_value']
        mean_score = mean(score_values)

        assignee_col.append(user_id)
        score_value_col.append(mean_score)

    #         result.append({'assignee_id': user_id, 'score_value': mean_score})
    result = pd.DataFrame({'assignee_id': assignee_col, 'score_value': score_value_col})
    return result


def insert_data_into_gbq(InsertData: pd.DataFrame, ProjectId, DatasetId, TableId):
    destination_table = f"{DatasetId}.{TableId}"

    res_df = pd.DataFrame()
    if 'status' in InsertData.columns:
        res_df['assignee_id'] = InsertData['assignee_id'].astype('int')
        res_df['status'] = InsertData['status'].astype('str')
        res_df['count_last_period'] = InsertData['count_last_period'].astype('int')
        res_df['count_mean_calc_period'] = InsertData['count_mean_calc_period'].astype('float')
        res_df['count_sem_calc_period'] = InsertData['count_sem_calc_period'].astype('float')
        res_df['score_value'] = InsertData['score_value'].astype('int')
    else:
        res_df['assignee_id'] = InsertData['assignee_id'].astype('int')
        res_df['score_value'] = InsertData['score_value'].astype('float')

    res_df['developer'] = 'dennis.shablonov'
    res_df['developer'] = res_df['developer'].astype('str')

    pandas_gbq.to_gbq(res_df, destination_table=destination_table, project_id=ProjectId, if_exists='append')