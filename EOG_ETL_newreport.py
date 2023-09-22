from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Дефолтные параметры для @task
default_args = {
    'owner': 'e-ogo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 7),
}

# Интервал запуска DAG
schedule_interval = '0 9 * * *'

# коннекшены к датасету с обновлениями и до тестовой базы
connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'}

connection_in = {
                'host': 'https://clickhouse.lab.karpov.courses',
                'password': '656e2b0c9c',
                'user': 'student-rw',
                'database': 'test'}    

# основной даг
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_eog():
    
    #  читаем таблицу 1
    @task()
    def read_table1():  
        query = """SELECT user_id, age , gender, os, 
                        countIf(post_id, action = 'like') as likes, 
                        countIf(post_id, action = 'view') as views,
                        toDate(time) as event_date
                    FROM simulator_20230820.feed_actions
                    WHERE date(time) = today() - 1
                    GROUP BY user_id, age , gender, os, event_date"""

        table1 = pandahouse.read_clickhouse(query, connection=connection)
        return table1

    #  читаем таблицу 2
    @task()
    def read_table2():
        query = """SELECT distinct user_id, 
                        count(reciever_id) over (partition by user_id) as messages_sent,
                        count(distinct reciever_id) over (partition by user_id) as users_sent,
                        messages_received,
                        users_received,
                        toDate(time) as event_date
                FROM simulator_20230820.message_actions
                    join 
                    (SELECT reciever_id, count(user_id) as messages_received, count(distinct user_id) as users_received
                    FROM simulator_20230820.message_actions
                    GROUP BY reciever_id) as t1
                    on simulator_20230820.message_actions.user_id = t1.reciever_id
                WHERE date(time) = today()-1"""
        
        table2 = pandahouse.read_clickhouse(query, connection=connection)
        return table2
        
    @task() 
#     обьединяем 2 таблицы по user_id и дате
    def join_tables(table1, table2):
        table3 = pd.merge(table1, table2, how = "outer", on = ("user_id", "event_date"))
        return table3
    
    @task()
#     делаем три группировки, к каждой переименовываем столбца для последующего мержда
    def groupby_os(table3):
        group_os = table3.groupby(by = ["os", "event_date"], as_index = False)\
            [["likes", "views", "messages_sent","messages_received", "users_sent", "users_received"]]\
            .sum()
        group_os["dimension"] = "os"
        group_os = group_os.rename(columns={"os": "dimension_value"}) 
        return group_os 
    
    @task()
    def groupby_age(table3):
        group_age = table3.groupby(by = ["age", "event_date"], as_index = False)\
            [["likes", "views", "messages_sent","messages_received", "users_sent", "users_received"]]\
            .sum()
        group_age["dimension"] = "age"
        group_age = group_age.rename(columns={"age": "dimension_value"}) 
        return group_age
    
    @task()
    def groupby_gender(table3):
        group_gender = table3.groupby(by = ["gender", "event_date"], as_index = False)\
            [["likes", "views", "messages_sent","messages_received", "users_sent", "users_received"]]\
            .sum()
        group_gender["dimension"] = "gender"
        group_gender = group_gender.rename(columns={"gender": "dimension_value"}) 
        return group_gender

#     соединяем 3 тпблицы, грузим получившийся дф в тестовую базу
    @task()
    def save(group_os, group_age, group_gender):
        df = pd.concat([group_os, group_age, group_gender]) \
                .astype({'views':'int64',
                        'likes':'int64',
                        'messages_received':'int64',
                        'messages_sent':'int64',
                        'users_received':'int64',
                        'users_sent':'int64'})   

        pandahouse.to_clickhouse(df = df, table = 'eog14', index=False, connection=connection_in)


    table1 = read_table1()
    table2 = read_table2()
    table3 = join_tables(table1, table2)
    group_os = groupby_os(table3)
    group_age = groupby_age(table3)
    group_gender = groupby_gender(table3)
    save(group_os, group_age, group_gender)

dag_eog = dag_eog()