# -*- coding:utf-8 -*-
# __author__ = majing

import datetime
from prettytable import PrettyTable

from airflow.models import TaskInstance
from airflow.settings import Session
from airflow.utils.email import send_email_smtp
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


def report_template(*args):
    html = u"""
    <html>

        <body>
        <p>执行任务数：%s</p>
        <p>错误任务数：%s</p>  
        <p>任务正确率：%s</p> 
            
        <p>平均任务时长：%s</p>
        <p> 最慢任务: </p>
        <p>    %s </p>
            
            
        <p>错误任务列表: </p>
        <p>    %s </p>
        </body>
    
    </html>

    
    """ % args
    return html


def parse_time(ptime):
    """
    格式化时间
    :param ptime: 
    :return: 
    """
    if isinstance(ptime, str):
        if '/' in ptime:
            _format = '%Y/%m/%d'
        elif '-' in ptime:
            _format = '%Y-%m-%d'
        elif '_' in ptime:
            _format = '%Y_%m_%d'
        else:
            _format = '%Y-%m-%d'
        try:
            ptime = datetime.datetime.strptime(ptime, _format)
        except Exception as exc:
            raise ValueError(u'str to datetime error: %s' % exc.message)
    return ptime


def print_string_to_table(columns, instances):
    """
    输出一个table
    columns: []
    instances: [] 
    """
    if not columns:
        raise ValueError(u'参数columns不能为空')

    values = []
    _table = PrettyTable()
    _table.field_names = columns

    log.info("instances: %s   type(instances): %s" % (instances, type(instances)))

    try:
        for task in instances:
            _tmp = []
            for key in columns:
                _tmp.append(getattr(task, key))
            values.append(_tmp)
    except Exception as exc:
        raise ValueError(exc.message)

    for item in values:
        _table.add_row(item)

    return _table.get_html_string()


def gen_task_statistics(report_time):
    """
    获取任务统计
    :param report_time: '2017/12/20' 
    :return: 
    """
    report_time = parse_time(report_time)

    # change time to utc
    filter_start_time = (datetime.datetime(report_time.year, report_time.month, report_time.day) - \
                        datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    filter_end_time = (datetime.datetime(report_time.year, report_time.month, report_time.day, 23, 59, 59) - \
                      datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

    session = Session()
    total_tasks = session.query(TaskInstance).filter(TaskInstance.start_date.between(filter_start_time, filter_end_time)).\
        filter(TaskInstance.end_date.between(filter_start_time, filter_end_time))

    total_tasks_num = total_tasks.count()
    error_task_num = 0
    total_execute_time = 0.0
    execute_time_max = None
    error_tasks = []

    for task in total_tasks:
        duration = task.duration
        total_execute_time += duration
        if not execute_time_max:
            execute_time_max = task
        if duration > execute_time_max.duration:
            execute_time_max = task

        if not task.is_success:
            error_task_num += 1
            error_tasks.append(task)

    if execute_time_max:
        columns = ['dag_id', 'task_id', 'duration']
        execute_time_max_html = print_string_to_table(columns, [execute_time_max])
    else:
        execute_time_max_html = u''

    if error_tasks:
        columns = ['dag_id', 'task_id', 'execution_date', 'start_date']
        error_task_html = print_string_to_table(columns, error_tasks)
    else:
        error_task_html = u''

    if total_tasks_num:
        correct_rate = u"%s" % round(((total_tasks_num - error_task_num) * 100)/float(total_tasks_num), 2) + u'%'
        average_task_time = u" %s s" % round(total_execute_time / total_tasks_num, 4)
    else:
        correct_rate = u'0'
        average_task_time = u'0.00'

    report_html = report_template(total_tasks_num, error_task_num, correct_rate, average_task_time,
                                  execute_time_max_html, error_task_html)
    session.expunge_all()
    session.commit()
    session.close()
    return report_html


def send_task_report_email(report_time):
    """
    发送邮件
    :param to: 
    :param subject: 
    :param html_content: 
    :return: 
    """
    try:
        html_content = gen_task_statistics(report_time)
        log.info('send_task_report_email: html_content: %s ' % html_content)
        if not isinstance(html_content, unicode):
            html_content = unicode(html_content, 'utf-8')
        send_email_smtp(subject=u'任务统计表', html_content=html_content)
        return True, ''
    except Exception as exc:
        return False, exc.message

