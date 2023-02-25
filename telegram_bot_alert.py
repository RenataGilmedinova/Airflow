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
    'start_date': datetime(2023, 2, 13),
}

schedule_interval = '0 11 * * *' # отчет приходит каждый день в 11 утра

import telegram
my_token = '5918290671:AAEgWZFAZYdlBcooZjnEVbbvwZmtS05cSmI'

bot = telegram.Bot(token=my_token)

chat_id = -677113209

@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False)
def task7_2_renata_feed_report():
    
    # загрузка данных из feed_action
    @task()
    def extract_feed():
        q = """
        select toStartOfDay(time) as day,
            count(DISTINCT user_id) as DAU_feed_actions,
            countIf(user_id, action = 'view') as views,
            countIf(user_id, action = 'like') as likes,
            countIf(user_id, action = 'view')/countIf(DISTINCT user_id, action = 'view') as views_per_user,
            countIf(user_id, action = 'like')/countIf(DISTINCT user_id, action = 'like') as likes_per_user,
            countIf(user_id, action = 'like')/countIf(user_id, action = 'view') as ctr
        from {db}.feed_actions
        where toDate(time) <= today() - 1 and toDate(time) >= today() - 8
        GROUP BY day
        """
        df1 = ph.read_clickhouse(q, connection=connection)
        return df1
    
    #Загрузка данных по сообщениям
    @task()
    def extract_messege():
        q_2 = """
        select toStartOfDay(time) as day,
            count(user_id) as messages,
            count(DISTINCT user_id) as DAU_message_sender,
            count(user_id)/count(DISTINCT user_id) as messages_per_user        
        from simulator_20230120.message_actions
        where toDate(time) <= today() - 1 and toDate(time) >= today() - 8
        GROUP BY day
        """

        df2 = ph.read_clickhouse(q_2, connection=connection)
        return df2
    
    @task()
    def transfrom_join(df1, df2):
        df_all = df1.merge(df2, how='inner', on=['day', 'day']).sort_values(by='day')
        return df_all
    
    @task()
    def load_report(df_all):

        report_msg = f'''
                Привет! Данные за последнюю неделю ниже'''
    
        bot.send_message(chat_id=chat_id, text=report_msg)
        
        #графики

        sns.set_style("whitegrid")
        fig, axes = plt.subplots(nrows= 2 , ncols= 2 , figsize=(22, 20))

        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=15)

        sns.lineplot(ax = axes[0,0], data = df_all, x = 'day', y = 'views_per_user', label = 'views_per_user')
        sns.lineplot(ax = axes[0,0], data = df_all, x = 'day', y = 'likes_per_user', label = 'likes_per_user')
        axes[0,0].set_xlabel(' ')
        axes[0,0].set_ylabel(' ')
        axes[0,0].legend()
        axes[0,0].set_title('События на 1 пользователя', fontsize=15)
        axes[0,0].grid()

        sns.lineplot(ax = axes[0,1], data = df_all, x = 'day', y = 'views', label = 'Просмотры')
        sns.lineplot(ax = axes[0,1], data = df_all, x = 'day', y = 'likes', label = 'Лайки')
        sns.lineplot(ax = axes[0,1], data = df_all, x = 'day', y = 'messages', label = 'Сообщения')
        axes[0,1].set_xlabel(' ')
        axes[0,1].set_ylabel(' ')
        axes[0,1].legend()
        axes[0,1].set_title('Все события', fontsize=15)
        axes[0,1].grid()


        sns.lineplot(ax = axes[1,0], data = df_all, x = 'day', y = 'views_per_user', label = 'Просмотры')
        sns.lineplot(ax = axes[1,0], data = df_all, x = 'day', y = 'likes_per_user', label = 'Лайки')
        sns.lineplot(ax = axes[1,0], data = df_all, x = 'day', y = 'messages_per_user', label = 'Сообщения')
        axes[1,0].set_xlabel(' ')
        axes[1,0].set_ylabel(' ')
        axes[1,0].legend()
        axes[1,0].set_title('События на 1 пользователя', fontsize=15)
        axes[1,0].grid()


        sns.lineplot(ax = axes[1,1], data = df_all, x = 'day', y = 'ctr')
        axes[1,1].set_title('CTR ленты новостей', fontsize=15)
        axes[1,1].set_xlabel(' ')
        axes[1,1].set_ylabel(' ')
        axes[1,1].grid()
        
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plot1.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df1 = extract_feed()
    df2 = extract_messege()
    df_all = transfrom_join(df1, df2)
    load_report_ = load_report(df_all)
    
run_task2 = task7_2_renata_feed_report()