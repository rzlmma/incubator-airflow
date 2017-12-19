# -*- coding:utf-8 -*-
# __author__ = majing

import datetime

from odps import ODPS
from odps.df import DataFrame


from airflow.utils.log.logging_mixin import LoggingMixin

MIN_AND_MAX = []

class ConnOdps(object):
    def __init__(self, access_id, access_key, project):
        self.access_id = access_id
        self.access_key = access_key
        self.project = project
        self.__initMaxCompute()

    def __initMaxCompute(self):
        self.odps = ODPS(self.access_id, self.access_key, self.project)


class MaxComputeAPI(ConnOdps, LoggingMixin):
    """
    MaxCompute api
    """
    def get_result_number(self, sql):
        instance = self.odps.execute_sql(sql)
        with instance.open_reader() as reader:
            count = reader.count
        self.log.info("get_result_number: %s" % count)
        return count

    def check_data_consistency(self, src_table, dest_table, join_table):
        """
        检查数据一致性
        src_table: string  源数据表
        dest_table: string  处理后的数据表
        join_table: string 关联表
        :return: 
        """
        if not src_table or not dest_table:
            raise ValueError(u"参数不能为空")

        if not self.odps.exist_table(src_table):
            raise ValueError(u'table [%s] is not in project [%s]' % (src_table, self.project))

        if not self.odps.exist_table(dest_table):
            raise ValueError(u'table [%s] is not in project [%s]' % (src_table, self.project))

        filter_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")

        src_df = DataFrame(self.odps.get_table(src_table))
        dest_df = DataFrame(self.odps.get_table(dest_table))
        join_df = DataFrame(self.odps.get_table(join_table))

        dest_num_1 = dest_df.filter(dest_df.day == filter_day, dest_df.only != 1).revenue_prop.sum()
        src_num_1 = src_df.left_join(join_df, on=[src_df.institution_id == join_df.i_id]).\
            filter(src_df.end_time > datetime.datetime.now(), src_df.only != 1, join_df.i_institution_type_code == '0',
                   join_df.i_cooperate_state_code > 0).count()
        self.log.info("check_data_consistency==> dest_num_1:%s  src_num_1: %s" % (dest_num_1, src_num_1))

        dest_num_2 = dest_df.filter(dest_df.day == filter_day, dest_df.only == 1, dest_df.status == 1).revenue_prop.sum()
        src_num_2 = src_df.left_join(join_df, on=[src_df.institution_id == join_df.i_id]).\
            filter(src_df.end_time > datetime.datetime.now(), src_df.only == 1, src_df.status == 1,
                   join_df.i_institution_type_code == '0',
                   join_df.i_cooperate_state_code > 0).count()
        self.log.info("check_data_consistency==> dest_num_2:%s  src_num_2: %s" % (dest_num_2, src_num_2))

        if dest_num_1 == src_num_1 and dest_num_2 == src_num_2:
            return True, "dest_num_1: %s  src_num_1:%s | dest_num_2:%s src_num_2:%s" % (dest_num_1, src_num_1,
                                                                                        dest_num_2, src_num_2)
        return False, "dest_num_1: %s  src_num_1:%s | dest_num_2:%s src_num_2:%s" % (dest_num_1, src_num_1,
                                                                                        dest_num_2, src_num_2)

    def check_key_indicators(self, table, keyword, columns):
        """
        检查数据指标
        sql: 
        keyword: null|repeat
        column: 检查的列名
        """
        rest = dict()
        if keyword not in ['null', 'repeat']:
            raise ValueError(u"参数错误：keyword in null or repeat")

        if not isinstance(columns, list):
            raise TypeError(u'colums need a list')

        instance = DataFrame(self.odps.get_table(table))
        all_columns = [t.name for t in instance.dtypes]
        if not set(columns).issubset(set(all_columns)):
            raise ValueError(u"some colum in %s is not in table %s" %(columns, table))

        self.log.info(u"check_key_indicators==> keyword: %s columns: %s " %(keyword, columns))
        total_num = instance.count()
        self.log.info(u"check_key_indicators ==> table_name: %s  total_num: %s " %(table, total_num))
        if keyword == 'null':
            for col in columns:
                num = instance.filter(lambda x: getattr(x, col) == 'NULL').count()
                self.log.info(u"check_key_indicators ==> column_name: %s  num:%s " %(col, num))
                rest[col] = round(num/float(total_num), 4)
        else:
            for col in columns:
                num = instance.distinct(col).count()
                self.log.info(u"check_key_indicators ==> column_name: %s  distinct_num:%s " % (col, num))
                rest[col] = round((total_num - num)/float(total_num), 4)
        return rest

    def _parse_partion(self, partion):
        """
        解析分区名字，得到分区日期
        :param partion: 
        :return: 
        """
        rest = {}
        partion_day = None
        if ',' in partion:
            new = partion.split(',')
        else:
            new = [partion]
        for item in new:
            tmp = item.split('=')
            rest[tmp[0]] = rest[tmp[1]]
        if 'day' in rest:
            day_time = rest.get('day')
            if '_' in day_time:
                elements = day_time.split('_')
                partion_day = datetime.datetime(elements[0], elements[1], elements[2])
            else:
                partion_day = datetime.datetime(day_time[0:4], day_time[4:6], day_time[6:])
        return partion_day

    def _get_histroy_inscrement_data(self, table):
        """
        获取历史增量
        :param table: 
        :return: 
        """
        global MIN_AND_MAX
        histroy_day = datetime.datetime.today() - datetime.timedelta(days=1)
        _table = self.odps.get_table(table)
        for pt in _table.partitions:
            pt_name = pt.name
            partion_day = self._parse_partion(pt_name)
            if partion_day < histroy_day:
                with _table.open_reader(partition=pt_name) as reader:
                    count = reader.count
                    if MIN_AND_MAX:
                        min_d = MIN_AND_MAX[0][1]
                        max_d = MIN_AND_MAX[1][1]
                        if count < min_d:
                            MIN_AND_MAX[0] = (pt_name, count)

                        if count > max_d:
                            MIN_AND_MAX[1] = (pt_name, count)
                    else:
                        MIN_AND_MAX = [(pt_name, count), (pt_name, count)]

    def get_inscrement_data(self, table):
        """
        获取数据的增量
        :return: 
        """
        if not table:
            raise ValueError(u'the param table is None')
        if not self.odps.exist_table(table):
            raise ValueError(u'table [%s] 不存在' % table)

        self.log.info(u'get_inscrement_data==> MIN_AND_MAX: %s' % MIN_AND_MAX)
        if not MIN_AND_MAX:
            self._get_histroy_inscrement_data(table)

        today = datetime.datetime.today() - datetime.timedelta(days=1)
        _table = self.odps.get_table(table)

        need_partions = [pt.name for pt in _table.partitions if self._parse_partion(pt.name) >= today]
        self.log.info(u'get_inscrement_data==> need_partions: %s' % need_partions)
        for pt in need_partions:
            with _table.open_reader(pt) as reader:
                count = reader.count
                self.log.info("get_inscrement_data==> partion: %s num:%s " % (pt, count))
                min_d = MIN_AND_MAX[0][1]
                max_d = MIN_AND_MAX[1][1]
                if count < min_d:
                    MIN_AND_MAX[0] = (pt, count)

                if count > max_d:
                    MIN_AND_MAX[1] = (pt, count)

        self.log.info(u'get_inscrement_data==> MIN_AND_MAX: %s' % MIN_AND_MAX)

if __name__ == '__main__':
    ACCESS_ID = 'LTAIhNwQk5AplazR'
    ACCESS_KEY = 'kxvb9Hh1S7deJgyYRg79F0rvSf9vL5'
    ODPS_PROJECT = 'dpdefault_126990'
    sql = """
    SELECT
      path,
      thread
    FROM
        basic_data_service_logstore
    LIMIT
        10
    """
    od = MaxComputeAPI(ACCESS_ID, ACCESS_KEY, ODPS_PROJECT)




