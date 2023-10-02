from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import requests
import pandahouse
import telegram
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-ogo',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 14),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def eog_mainmetrics1():
    
    @task()
    def read():
#   читаем бд за последнюю неделю
        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'}

        query = """SELECT toDate(time) as date,
                                count(distinct user_id)/1000 as dau,  
                                countIf(post_id, action = 'like')/1000 as likes, 
                                countIf(post_id, action = 'view')/1000 as views,
                                (likes/views)*100 as ctr
                            FROM simulator_20230820.feed_actions
                            WHERE date(time) between (today() - 7) AND (today() - 1)
                            GROUP BY date"""

        df = pandahouse.read_clickhouse(query, connection=connection)
        
#       создаем столбец с датой без года (нужен будет в графиках)
        df["day"] = ""
        for i in range(len(df["date"])):
            df.at[i, "day"] = df.at[i, "date"].strftime('%m-%d')
        return df
    
    @task()   
    def text_report(df):
        # создаем текстовый отчет
        date = df.iat[6, 0].strftime("%d/%m/%y")
        dau = round(df.iat[6, 1], 2)
        likes = round(df.iat[6, 2], 2)
        views = round(df.iat[6, 3], 2)
        ctr = round(df.iat[6, 4], 2)
        report_message = f"Метрики за {date}: \n  DAU: {dau} тыс. \n  Лайки: {likes} тыс.\n  Просмотры: {views} тыс.\n  CTR: {ctr}%"
        return report_message
    
    @task()  
    def graph_report(df):
#       создаем графический отчет
        figure, axes = plt.subplots(4, 1, sharex=True, figsize=(10,10))
        figure.suptitle('Показатели за неделю')


        axes[0].set_title('DAU, тыс')
        axes[1].set_title('Просмотры, тыс')
        axes[2].set_title('Лайки, тыс')
        axes[3].set_title('CTR, %')

        sns.lineplot(ax=axes[0], data=df, x='day', y='dau')
        sns.lineplot(ax=axes[1], data=df, x='day', y='views')
        sns.lineplot(ax=axes[2], data=df, x='day', y='likes')
        sns.lineplot(ax=axes[3], data=df, x='day', y='ctr')
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        
        return plot_object
    
    @task()    
    def send(report_message, plot_object):
        
#       настройки бота
        my_token = '6510694941:AAFP4GtMAvWswONBpI2m9g2iNN4ueT25Ko4' 
        bot = telegram.Bot(token=my_token)
        chat_id = -928988566 #сюда подставить идгруппы
        
#         запуск бота
        bot.sendMessage(chat_id=chat_id, text=report_message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df = read()
    report_message = text_report(df)
    plot_object = graph_report(df)
    send(report_message, plot_object)
    
eog_mainmetrics1 = eog_mainmetrics1()