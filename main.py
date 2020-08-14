from google.oauth2 import service_account
import pandas_gbq

import numpy as np
import pandas as pd
import math as mt
import datetime as dt

import lib_main

CREDENTIALS = service_account.Credentials.from_service_account_info({
  "type": "service_account",
  "project_id": "findcsystem",
  "private_key_id": "36721f70db39fbb81267c346c2668f10ff0cf5db",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDrna55UYM+/6kh\nGJnQ28+QWLv2Uv9D7jpSLyENeBFJMWAAWh1ZAlGctc/VgLr1vp5CU/w+2jONawMk\nFqQFoxcAAUIJT6KHHZqLU8zh7mDccpwUlfn7ZaQUbiR0J6ZH0nHmfQtk1FOs1TZU\nzJGtyNzUnpePsYJOdpsiZ+Ofh7i3kVubkOoIf/EdNdECOx2bbv7uKkERjIMYCJyh\nCRXgq2c/19CODEETmvEpUKjYCn9LSspBjTHiUUbT1JQfIAr73r1pXl52dATHIay9\nf4hNXAr8HUy0DpbiUCUdvzF5N+a8cl5tIHewNfXdv0pWMUmCJetKWk4FKIkMOlDQ\ny7Of3XdjAgMBAAECggEAGtUcww0w5LJJ6Q57qej2hOpOEZnXHz3Yn3ZljbqgQVUh\nqTiaAgJ9F9suvZJI+GaKcGRPJDtGRrMqquf3KvstSd9azWOnzzpkWLyk5w/2MPTO\naJvB2crz+i2m8iF30tMZDJYt/4AcvWCUrOiVKJTo7T/YY9Fj1Rq1xR/OsOLMev8X\nXbzU+ofW7G8gG/1FFb/qI1FsF0rz7O3zJD79OjJ8jZcz5V4FzCwg8yFumPbhDzlK\ne2oou3k55Owo2GKdRZUgmlpUSL8s3uk65q9vOOngXwp7qQMQwWueUfx608aIgeT9\n7OXBuMQ8mXbUkU7lYAWippvwejKQU0BQWpjttisanQKBgQD4kgM+BvV9R5froojE\niBaXfRf82bJpP9AH0tRWk86kaFhyGR3boioyVENkbMw54ZJVVA4zKukslzrasFUf\n+bM2595+0mtqMRWw+tiApm3ONwevOPxwn/ijTPJiqKQ96/24aA93oZcfZ9X38/4l\niFTYvMAG5YjHIsCE7zXJDRHg3QKBgQDyqIukCTIurPyHZ4EOUUZDWY50JTm0mCKg\nbTNOvM+h9lvnKHYrDrFG9gkb7lbzCZCcJJybtEUEki2D1kuJ4Wqu1/kI2ZPkp5Ie\n/ViJcxCzCj0fReo2+yOIsHHQ0AQADzmkHrMZYxYWUh8agPOB28Tw/8SZ9Dc1oVrN\nceTu+kkVPwKBgDgt6AWw1PMHp1JeXcLtbw21/CHtoeEfxwi9obgfl+iYnMTM4G6v\nbBIL8V9VJ6M9VDFs2fi+jgzB8U9T4yli6hpStXq8XAKYLWrehugstUySK25y1rst\nrKhbz7x0mQpVt/Zhrn0/TESQ108/GgWplmOV5WCpqAw50oE4/1L9XTkRAoGAD0Ir\n7beqUScNhhIrGlRf/7Is8/63PzTl0IKtXEEhKUUNiF6R96kn2pd0AS6ehw/N6ROg\nSWYvhNcQR579BwGGrNHl1fmghBtJY+t4WsRCg4+cQlAqJyTpmhnGPmQmLD7I2Boa\nFvmVFPg6/nanWT4Rhzn+CdRCeHvZ8ts7kw9n8w8CgYA3NBW4vaWh0ggxbI9dbG06\nZ6cY+b6guDlky/2HKXgZrCj+6DFVr/8XSdPw7s8ICrbywCbEZVXW+HOFb4fkZr5r\ni3VO9ihyCf9PzwgQ0hviL0fyJH1Ao3myySh5SyCxgcBoXEJhXG43aY70tQPTNLdP\n4SiylfHm6gEBJudzYov/iw==\n-----END PRIVATE KEY-----\n",
  "client_email": "xsolla-ss-2020-students@findcsystem.iam.gserviceaccount.com",
  "client_id": "105043181870281932141",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xsolla-ss-2020-students%40findcsystem.iam.gserviceaccount.com"
})

if __name__ == '__main__':
    df = lib_main.getFreshData(CREDENTIALS, 'findcsystem')
    scored_employees = lib_main.score_employees(df)
    unified_employee_scores = lib_main.unify_employee_scores(scored_employees)

    lib_main.insert_data_into_gbq(scored_employees, 'findcsystem', 'xsolla_summer_school', 'score_result_status')
    lib_main.insert_data_into_gbq(unified_employee_scores, 'findcsystem', 'xsolla_summer_school', 'score_result_total')
    # print(scored_employees)
    # print()
    # print(unified_employee_scores)
