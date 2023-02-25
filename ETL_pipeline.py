import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import requests
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student',
              password='dpo_python_2020'):
    r = requests.post(host, data=query.encode('utf-8'), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


# параметры подключения для записи в БД
connection_write = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'}

query = """select user_id, 
            countIf(post_id, action='like') as likes, 
            countIf(post_id, action='view') as view
        from simulator_20230120.feed_actions
            group by user_id"""

# дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'r.gilmedinova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 30)
}

# интервал запуска DAG

schedule_interval = '0 07 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_renata():
    @task()
    def extract1():
        query = """select 
                    user_id, 
                    countIf(post_id, action='like') as likes, 
                    countIf(post_id, action='view') as views
                from simulator_20230120.feed_actions
                group by user_id
                format TSVWithNames"""
        df1 = ch_get_df(query=query)
        return df1

    @task()
    def extract2():
        query = """select event_date, user_id, os, gender, age, t1.messages_received, t1.messages_sent, t2.users_received, t2.users_sent from
            (select toDate(time) as event_date , user_id,os, gender, age, count(reciever_id) as messages_received, count(distinct (reciever_id)) as messages_sent
            from simulator_20230120.message_actions
            where toDate(time) = today() - 1
            group by user_id,os, gender, age, time) t1
            inner join
            (select toDate(time) as event_date, reciever_id, count(user_id) as users_received, count(distinct (user_id)) as users_sent
            from simulator_20230120.message_actions
            where toDate(time) = today() - 1
            group by time, reciever_id) t2
            on t1.user_id = t2.reciever_id
            format TSVWithNames"""
        df2 = ch_get_df(query=query)
        return df2

    @task()
    def transform_join_table(df_cube1, df_cube2):
        df_cubes_merged = df_cube1.merge(df_cube2, on='user_id')
        return df_cubes_merged

    @task()
    def transform_os(df_cubes):
        df_cubes_os = df_cubes.groupby(['event_date', 'os']).sum().reset_index()
        df_cubes_os['dimension'] = 'os'
        df_cubes_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        return df_cubes_os

    @task()
    def transform_age(df_cubes):
        df_cubes_age = df_cubes.groupby(['event_date', 'age']).sum().reset_index()
        df_cubes_age['dimension'] = 'age'
        df_cubes_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        return df_cubes_age

    @task()
    def transform_gender(df_cubes):
        df_cubes_gender = df_cubes.groupby(['event_date','gender']).sum().reset_index()
        df_cubes_gender['dimension'] = 'gender'
        df_cubes_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        return df_cubes_gender

    @task()
    def total_table(df_cubes_os, df_cubes_age, df_cubes_gender):
        df_total = pd.concat([df_cubes_os, df_cubes_age, df_cubes_gender], sort=False)
        df_total = df_total.drop(['user_id', 'age', 'gender'], axis=1)
        return df_total

    @task()
    def load(total_table):
        table_name = 'renata_gilmedinova_lesson6'
        query_create = f'''
        create table if not exists test.{table_name} 
        (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt32,
            likes UInt32,
            messages_received UInt32,
            messages_sent UInt32,
            users_received UInt32,
            users_sent UInt32
        )
        ENGINE = MergeTree
        order by event_date
        partition by toYYYYMMDD(event_date)
        '''
        pandahouse.execute(query_create, connection_write)
        n = pandahouse.to_clickhouse(total_table, table_name, index=False, connection=connection_write)
        print(f'count of rows: {n}')

    df_cube1 = extract1()
    df_cube2 = extract2()
    df_cubes = transform_join_table(df_cube1, df_cube2)
    df_cubes_os = transform_os(df_cubes)
    df_cubes_gender = transform_gender(df_cubes)
    df_cubes_age = transform_age(df_cubes)
    df_total = total_table(df_cubes_os, df_cubes_gender, df_cubes_age)
    load = load(df_total)


dag_renata = dag_renata()