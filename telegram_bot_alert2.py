import pandahouse as ph
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230120'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'r-gilmedinova-15',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 2, 12),
}

schedule_interval = '0 11 * * *' # отчет приходит каждый день в 11 утра

import telegram
my_token = '0'

bot = telegram.Bot(token=my_token)

chat_id = -0

@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False)
def task7_renata_feed_report():
    
    # загрузка данных за предыдущий день
    @task()
    def extract_yesterday():
        q = """
        SELECT max(toDate(time)) as day, 
        count(DISTINCT user_id) as DAU, 
        sum(action = 'like') as likes,
        sum(action = 'view') as views, 
        round(likes/views*100,2) as CTR
        FROM simulator_20230120.feed_actions
        WHERE toDate(time) = yesterday()
        """
        report_last_day = ph.read_clickhouse(q, connection=connection)
        return report_last_day
    
    #Загрузка данных за прошлую неделю
    @task()
    def extract_last_week():
        q_2 = """
        SELECT toDate(time) as day, 
        count(DISTINCT user_id) as DAU, 
        sum(action = 'like') as likes,
        sum(action = 'view') as views, 
        round(likes/views*100,2) as CTR
        FROM simulator_20230120.feed_actions
        WHERE toDate(time) > today()-8 AND toDate(time) < today()
        GROUP BY day
        """

        report_week = ph.read_clickhouse(q_2, connection=connection)
        return report_week
    

    
    @task()
    def load_report(report_last_day, report_week):
        df1 = report_last_day.iloc[-1]

        report_msg = "Дата: " + df1['day'].strftime("%d.%m.%Y")+\
                    "\nDAU: " + str(df1['DAU']) +\
                    "\nLikes: " + str(df1['likes']) +\
                    "\nViews: " + str(df1['views']) +\
                    "\nCTR: " + str(df1['CTR']) + "%"
    
        bot.send_message(chat_id=chat_id, text=report_msg)
        
        #графики

        sns.set(rc={'figure.figsize':(15,5)})
        sns.lineplot(data=report_week, x='day', y='DAU').set(title='DAU')
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot1.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        sns.set_style("whitegrid")
        sns.set(rc={'figure.figsize':(18,10)})
        plt.xlabel("date", size=15, labelpad = 10)
        plt.ylabel("Количество", size=15, labelpad = 10)
        plt.title("Количество просмотров и лайков", size=15)
        color_map = ['#FFB6C1', '#4169E1']
        plt.stackplot(report_week.day, report_week.likes, report_week.views, labels = ['likes', 'views'], alpha=0.5, colors = color_map)
        plt.legend()
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot2.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        sns.set_style("whitegrid")
        sns.lineplot(data=report_week, x='day', y='CTR').set(title='CTR')


        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot3.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    report_last_day = extract_yesterday()
    report_week = extract_last_week()
    sen_messages = load_report(report_last_day, report_week)
    
run_task = task7_renata_feed_report()
