# -*- coding:utf-8 -*-
# __author__ = majing

import datetime
from sqlalchemy.sql import func
from sqlalchemy import desc

from airflow.models import TaskInstance
from airflow.settings import Session, smtp_mail_to
from airflow.utils.email import send_email_smtp


def report_template(*args):
    html = u"""

    执行任务数：%s
    错误任务数：%s
    任务正确率：%s

    平均任务时长：%s
    最慢任务:
    %s

    错误任务列表：
    %s
    """ % (args[0], args[1], args[2], args[3], args[4], args[5], args[6])
    return html


def gen_task_statistics(report_time):
    """
    获取任务统计
    :param report_time: '2017/12/20' 
    :return: 
    """
    if isinstance(report_time, str):
        if '/' in report_time:
            _format = '%Y/%m/%d'
        elif '-' in report_time:
            _format  = '%Y-%m-%d'
        elif '_' in report_time:
            _format = '%Y_%m_%d'
        else:
            _format = '%Y-%m-%d'
        try:
            report_time = datetime.datetime.strptime(report_time, _format)
        except Exception as exc:
            raise ValueError('str to datetime error: %s' % exc.message)

    # change time to utc
    filter_start_time = (datetime.datetime(report_time.year, report_time.month, report_time.day) - \
                        datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    filter_end_time = (datetime.datetime(report_time.year, report_time.month, report_time.day, 23, 59, 59) - \
                      datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

    session = Session()
    total_tasks = session.query(TaskInstance).filter(TaskInstance.start_date.between(filter_start_time, filter_end_time)).\
        filter(TaskInstance.end_date.between(filter_start_time, filter_end_time)).count()

    error_tasks = session.query(TaskInstance).filter(TaskInstance.start_date.between(filter_start_time, filter_end_time)).\
        filter(TaskInstance.end_date.between(filter_start_time, filter_end_time)).filter(TaskInstance.state == 'failed')

    error_task_num = error_tasks.count()
    correct_rate = (total_tasks - error_task_num) / total_tasks

    total_execute_time = session.query(TaskInstance, func.sum(TaskInstance.duration)).filter(TaskInstance.start_date.between(filter_start_time, filter_end_time)).\
        filter(TaskInstance.end_date.between(filter_start_time, filter_end_time))[0][0]

    execute_time_max = session.query(TaskInstance).filter(TaskInstance.start_date.between(filter_start_time, filter_end_time)).\
        filter(TaskInstance.end_date.between(filter_start_time, filter_end_time)).order_by(desc(TaskInstance.duration))[0]

    execute_time_max_html = """
      dag_id              task_id                duration
      %s                   %s                      %s      
    
    """ % (execute_time_max.dag_id, execute_time_max.task_id, execute_time_max.duration)

    error_task_html = ' dag_id                  task_id \n'
    for task in error_tasks:
        error_task_html += " %s                     %s\n" %(task.dag_id, task.task_id)

    report_html = report_template(total_tasks, error_task_num, correct_rate, round(total_execute_time/total_tasks, 3),
                                  execute_time_max_html, error_task_html)
    session.expunge_all()
    session.commit()
    session.close()
    return report_html


def send_task_report_email(subject, html_content, report_time):
    """
    发送邮件
    :param to: 
    :param subject: 
    :param html_content: 
    :return: 
    """
    html_content = gen_task_statistics(report_time)
    send_email_smtp(smtp_mail_to, subject='send task report', html_content=html_content)

