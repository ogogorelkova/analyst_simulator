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

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def eog_allmetrics():
    
    @task()
    def read1():    
        query1 = """SELECT t2.date as date, DAU, new_users, left_users
                    FROM 
                        (SELECT (last_date + Interval 15 day) as date, count(distinct user_id) as left_users
                        FROM 
                            (SELECT max(toDate(time)) as last_date, user_id
                                FROM simulator_20230820.feed_actions
                                GROUP BY user_id) t1
                        GROUP BY date) t4
                    JOIN 
                        (SELECT date, count(user_id) as new_users
                        FROM (SELECT min(toDate(time)) as date,
                                        user_id
                                FROM simulator_20230820.feed_actions
                                GROUP BY user_id
                                ) t5
                        GROUP BY date) t2
                    on t4.date = t2.date
                    JOIN 
                        (SELECT toDate(time) as date, count(distinct user_id) as DAU
                        FROM simulator_20230820.feed_actions
                        GROUP BY date) t3
                    on t4.date = t3.date
                    WHERE t2.date between (today()-14) and (today()-1)"""

        users = pandahouse.read_clickhouse(query1, connection=connection)
        
        return users
        
    @task()
    def read2():
        query2 =  """SELECT date, likes, views, ctr, likes_per_user, views_per_user, messages/users as messages_per_user
                    FROM 
                        (SELECT toDate(time) as date, 
                                countIf(user_id, action = 'like') as likes,
                                countIf(user_id, action = 'view') as views,
                                countIf(user_id, action = 'like')/countIf(user_id, action = 'view')*100 as ctr,
                                countIf(user_id, action = 'like')/count(distinct user_id) as likes_per_user,
                                countIf(user_id, action = 'view')/count(distinct user_id) as views_per_user,
                                count(distinct user_id) as users
                        FROM simulator_20230820.feed_actions
                        GROUP BY date) f
                    left join 
                        (SELECT toDate(time) as date, 
                                count(reciever_id) as messages
                        FROM simulator_20230820.message_actions
                        GROUP BY date) m
                    using date
                    WHERE date between (today()-14) and (today()-1)"""

        metrics = pandahouse.read_clickhouse(query2, connection=connection) 
        
        return metrics
        
    @task()
    def read3():
        query3 =  """SELECT toDate(time) as date, post_id, countIf(user_id, action = 'like') as likes,countIf(user_id, action = 'view') as views
                FROM simulator_20230820.feed_actions
                WHERE date = today()-1
                GROUP BY date, post_id
                ORDER BY views desc
                LIMIT 1"""

        posts = pandahouse.read_clickhouse(query3, connection=connection)
        
        return posts

    @task()
    def text_message(users, metrics, posts):
        date = users.iat[13,0].strftime("%d/%m/%y")
        dau = users.iat[13,1]
        dau_dtd = round((dau/users.iat[12,1]-1)*100,2)
        wau = users.DAU[-7:].sum()
        wau_wtw = round((wau/users.DAU[:8].sum()-1)*100,2)
        new_users = users.iat[13,2]
        new_users_dtd = round((new_users/users.iat[12,2]-1)*100,2)
        left_users = users.iat[13,3]
        left_users_dtd = round((left_users/users.iat[12,3]-1)*100,2)
        ctr = round(metrics.iat[13,3],2)
        ctr_dtd = round((ctr/metrics.iat[12,3]-1)*100,2)
        lpu = round(metrics.iat[13,4],2)
        lpu_dtd = round((lpu/metrics.iat[12,4]-1)*100,2)
        vpu = round(metrics.iat[13,5],2)
        vpu_dtd = round((vpu/metrics.iat[12,5]-1)*100,2)
        mpu = round(metrics.iat[13,6],2)
        mpu_dtd = round((mpu/metrics.iat[12,6]-1)*100,2)
        postid = posts.iat[0,1]
        pl = posts.iat[0,2]
        pv = posts.iat[0,3]
        
        report_message = f"Метрики за {date}: \n\n  --Динамика пользователей-- \n DAU: {dau} ({dau_dtd}% dtd)\n WAU: {wau} ({wau_wtw}% wtw) \n \
Новых пользователей: {new_users} ({new_users_dtd}% dtd) \n Неактивных пользователей (нет активности 2 недели): {left_users} ({left_users_dtd}% dtd) \n\n\
--Пользовательская активность--\n CTR: {ctr} ({ctr_dtd}% dtd) \n В среднем на пользователя: \n   {lpu} лайков ({lpu_dtd}% dtd), \n\
   {vpu} просмотров ({vpu_dtd}% dtd),\n   {mpu} сообщений ({mpu_dtd}% dtd). \n\n\
Самый популярный пост: id{postid} ({pl} лайков, {pv} просмотров.)"
        return report_message
    
    @task()
    def graph_message(users, metrics):
        users["cdate"] = users.date
        users.cdate[:7] = users.date[7:]
        users["week"] = "current week"
        users.week[:7] = "previous week"
        users["ctr"] = metrics.ctr
        
        figure, axes = plt.subplots(4, 1, sharex=True, figsize=(10,10))
        figure.suptitle('Основные показатели приложения')
        i = ["current week", "previous week"]

        axes[0].set_title('DAU')
        axes[1].set_title('Новые пользователи')
        axes[2].set_title('Дезактивированные пользователи')
        axes[3].set_title('CTR, %')

        sns.lineplot(ax=axes[0], data=users, x='cdate', y='DAU', style = "week", style_order = i, legend = "full")
        sns.lineplot(ax=axes[1], data=users, x='cdate', y='new_users', style = "week", style_order = i, legend = False)
        sns.lineplot(ax=axes[2], data=users, x='cdate', y='left_users', style = "week", style_order = i,  legend = False)
        sns.lineplot(ax=axes[3], data=users, x='cdate', y='ctr', style = "week", style_order = i, legend = False)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        
        return plot_object
        
    @task()
    def bot(report_message, plot_object):    
        #настройки бота
        my_token = '6510694941:AAFP4GtMAvWswONBpI2m9g2iNN4ueT25Ko4' 
        bot = telegram.Bot(token=my_token)
        chat_id = -928988566
        # #сюда подставить идгруппы -928988566 
        
        #запуск бота
        bot.sendMessage(chat_id=chat_id, text=report_message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    users = read1()
    metrics = read2()
    posts = read3()
    report_message = text_message(users, metrics, posts)
    plot_object = graph_message(users, metrics)
    bot(report_message, plot_object)
    
eog_allmetrics = eog_allmetrics()    