import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import datetime, timedelta
import io
import sys
import os
import requests
from airflow.decorators import dag, task

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-ogo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 22),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def eog_alerts_public():
    @task()
    def get_data():
        # выгружаем данные
        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'}

        query = ''' SELECT *
                    FROM 
                        (SELECT toStartOfFifteenMinutes(time) as ts, toDate(ts) as date , formatDateTime(ts, '%R') as hm,
                                count(distinct user_id) as active_users_feed, 
                                countIf(post_id, action = 'like') as likes, 
                                countIf(post_id, action = 'view') as views,
                                (likes/views)*100 as ctr    
                        FROM simulator_20230820.feed_actions
                        WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm) feed
                    LEFT JOIN 
                        (SELECT toStartOfFifteenMinutes(time) as ts, count(distinct user_id) as active_users_messages, count(user_id) as messages
                        FROM simulator_20230820.message_actions
                        WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts) mess
                    using (ts)
                    ORDER BY ts '''

        data = pandahouse.read_clickhouse(query, connection=connection)

        #         список метрик и параметров для них (подбирали вручную)
        metric_list = ["active_users_feed", "likes", "views", "ctr", "active_users_messages", "messages"]
        n_list = [5, 7, 7, 11, 7, 5]
        a_list = [3, 3, 3, 2, 3, 3]

        #       запускаем цикл, который будет проверять все метрики по очереди. для каждого делаем копию дф и выбираем нужные параметры
        for i in range(len(metric_list)):

            df = data[['ts', "date", "hm", metric_list[i]]].copy()
            metric = metric_list[i]
            n = n_list[i]
            a = a_list[i]

            #         считаем отклонение при помощи межквартильного размаха (а - количество размахов, n - периоды сглаживания)
            df["q25"] = df.iloc[:, 3].shift(1).rolling(n).quantile(0.25)
            df["q75"] = df.iloc[:, 3].shift(1).rolling(n).quantile(0.75)
            df["iqr"] = df.q75 - df.q25

            df["up"] = df.q75 + a * df.iqr
            df["down"] = df.q25 - a * df.iqr

            df["up"] = df.up.rolling(n, center=True, min_periods=1).mean()
            df["down"] = df.down.rolling(n, center=True, min_periods=1).mean()

            #          расчитываем параметры для текстового сообщения
            current = float(df.iloc[-1, 3])
            diff = float((current / ((df.up.iloc[-1] + df.down.iloc[-1]) / 2) - 1) * 100)
            time = df.hm.iloc[-1]
            print(metric, current, diff, time)

            #            метрика должна показать показать выброс на 20% от рядом стоящих данных
            prev = float(abs(df.iloc[-1, 3] / ((df.iloc[-1, 3] + df.iloc[-3, 3]) / 2) - 1))

            #          проверяем, есть ли алерт
            if current < float(df.down.iloc[-1]) or current > float(df.up.iloc[-1]) or prev > 0.2:
                print("has alert")
                run_alerts(metric, current, diff, time, df)
            else:
                print("no alerts")

    # если еть алерт, запускаем отправку алерта по данной метрике
    @task()
    def run_alerts(metric, current, diff, time, df):
        chat_id = 149279733
        # ид чата с алертами -968261538
        # ид моего чата 149279733
        bot = telegram.Bot(token='6510694941:AAFP4GtMAvWswONBpI2m9g2iNN4ueT25Ko4')

        msg = '''Alert: Метрика {metric} по состоянию на {time}.\nТекущее значение {current:.2f}. Отклонение от ожидаемого значения {diff:.2f}%.\n\
Подробнее: http://superset.lab.karpov.courses/r/4439\n@ogogorelkova, нужен анализ ситуации.''' \
            .format(metric=str(metric), time=time, current=float(current), diff=float(diff))
        bot.sendMessage(chat_id=chat_id, text=msg)

        sns.lineplot(x=df["ts"], y=df[metric], label=metric)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    metric = get_data()
    current = get_data()
    diff = get_data()
    time = get_data()
    df = get_data()
    run_alerts(metric, current, diff, time, df)


eog_alerts_public = eog_alerts_public()