# -*- coding:utf-8 -*-
# __author__ = majing

import datetime

from odps.df import DataFrame

from airflow.utils.ali_odps import ConnOdps
from airflow.models import IncrementRecord
from airflow.settings import MIN_AND_MAX


class MaxComputeAPI(ConnOdps):
    """
    MaxCompute api
    """
    def get_result_number(self, sql):
        instance = self.odps.execute_sql(sql)
        with instance.open_reader() as reader:
            count = reader.count
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
        today = datetime.datetime.now() - datetime.timedelta(days=1)

        src_df = DataFrame(self.odps.get_table(src_table))
        dest_df = DataFrame(self.odps.get_table(dest_table))
        join_df = DataFrame(self.odps.get_table(join_table))

        dest_num_1 = dest_df.filter(dest_df.day == filter_day, dest_df.only != 1).revenue_prop.sum().execute()
        src_num_1 = src_df.left_join(join_df, on=[src_df.institution_id == join_df.i_id]).\
            filter(src_df.end_time > datetime.datetime.now(), src_df.start_time < datetime.datetime(today.year, today.month, today.day),
                   src_df.only != 1, join_df.i_institution_type_code == '0',
                   join_df.i_cooperate_state_code > 0).count().execute()

        dest_num_2 = dest_df.filter(dest_df.day == filter_day, dest_df.only == 1, dest_df.status == 1).revenue_prop.\
            sum().execute()
        src_num_2 = src_df.left_join(join_df, on=[src_df.institution_id == join_df.i_id]).\
            filter(src_df.end_time > datetime.datetime.now(), src_df.start_time < datetime.datetime(today.year, today.month, today.day),
                   src_df.only == 1, src_df.status == 1,
                   join_df.i_institution_type_code == '0',
                   join_df.i_cooperate_state_code > 0).count().execute()
        # self.log.info("check_data_consistency==> dest_num_2:%s  src_num_2: %s" % (dest_num_2, src_num_2))

        if abs(dest_num_1 - src_num_1) <= 1 and abs(dest_num_2 - src_num_2) <= 1:
            return True, "dest_num_1: %s  src_num_1:%s | dest_num_2:%s src_num_2:%s" % (dest_num_1, src_num_1,
                                                                                        dest_num_2, src_num_2)
        return False, "dest_num_1: %s  src_num_1:%s | dest_num_2:%s src_num_2:%s" % (dest_num_1, src_num_1,
                                                                                        dest_num_2, src_num_2)

    # def check_key_indicators(self, table, keyword, columns):
    #     """
    #     检查数据指标
    #     sql:
    #     keyword: null|repeat
    #     column: 检查的列名
    #     """
    #     rest = dict()
    #     if keyword not in ['null', 'repeat']:
    #         raise ValueError(u"参数错误：keyword in null or repeat")
    #
    #     if not isinstance(columns, list):
    #         raise TypeError(u'colums need a list')
    #
    #     instance = DataFrame(self.odps.get_table(table))
    #     all_columns = [t.name for t in instance.dtypes]
    #     if not set(columns).issubset(set(all_columns)):
    #         raise ValueError(u"some colum in %s is not in table %s" %(columns, table))
    #
    #     self.log.info(u"check_key_indicators==> keyword: %s columns: %s " %(keyword, columns))
    #     total_num = instance.count()
    #     self.log.info(u"check_key_indicators ==> table_name: %s  total_num: %s " %(table, total_num))
    #     if keyword == 'null':
    #         for col in columns:
    #             num = instance.filter(lambda x: getattr(x, col) == 'NULL').count()
    #             self.log.info(u"check_key_indicators ==> column_name: %s  num:%s " %(col, num))
    #             rest[col] = round(num/float(total_num), 4)
    #     else:
    #         for col in columns:
    #             num = instance.distinct(col).count()
    #             self.log.info(u"check_key_indicators ==> column_name: %s  distinct_num:%s " % (col, num))
    #             rest[col] = round((total_num - num)/float(total_num), 4)
    #     return rest
    #
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

    def __get_all_days(self, start_time, end_time):
        all_days = []
        months = dict()
        start_year = start_time.year
        end_year = end_time.year
        start_month = start_time.month
        end_month = end_time.month

        if start_year == end_year:
            months[start_year] = xrange(start_month, end_month+1)
        else:
            months[start_year] = xrange(start_month, 13)
            months[end_year] = xrange(1, end_month+1)

        for year, values in months.iteritems():
            for month in values:
                if month in [1, 3, 5, 7, 8, 10, 12]:
                    days = 31
                elif month == 2:
                    if year % 4 == 0 or year % 400 == 0:
                        days = 29
                    else:
                        days = 28
                else:
                    days = 30
                if month == start_month:
                    start_day = start_time.day
                    end_day = days
                elif month == end_month:
                    start_day = 1
                    end_day = end_time.day
                else:
                    start_day = 1
                    end_day = days
                for day in xrange(start_day, end_day + 1):
                    all_days.append((year, month, day))

        return all_days

    @staticmethod
    def __get_min_and_max(year, num):
        if not MIN_AND_MAX:
            MIN_AND_MAX.append((year, num))
            MIN_AND_MAX.append((year, num))
        else:
            _min = MIN_AND_MAX[0][1]
            _max = MIN_AND_MAX[1][1]
            if num < _min:
                MIN_AND_MAX[0] = (year, num)
            if num > _max:
                MIN_AND_MAX[1] = (year, num)

    def _get_histroy_inscrement_data(self, table):
        """
        获取历史增量
        :param table:
        :return:
        """
        end_day = datetime.date.today() - datetime.timedelta(days=2)
        _table = self.odps.get_table(table)
        creation_time = _table.creation_time
        df = DataFrame(_table)
        start_day = datetime.date(creation_time.year, creation_time.month, creation_time.day)
        all_days = self.__get_all_days(start_day, end_day)
        for item in all_days:
            record_day = datetime.date(item[0], item[1], item[2])
            tmp = record_day.strftime("%Y_%m_%d")
            record = IncrementRecord.find(table_name=table, record_date=record_day)
            if not record:
                num = df.filter(df.day == tmp).count().execute()
                num = int(num)
                IncrementRecord.create(table_name=table, record_date=record_day, numbers=num)
            else:
                num = record.numbers
            self.__get_min_and_max(tmp, num)

    def get_inscrement_data(self, table):
        """
        获取数据的增量
        :return:
        """
        try:
            if not table:
                raise ValueError(u'the param table is None')
            if not self.odps.exist_table(table):
                raise ValueError(u'table [%s] 不存在' % table)

            pre_day = datetime.date.today() - datetime.timedelta(days=1)
            pre_day_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")
            histroy_day = datetime.date.today() - datetime.timedelta(days=2)
            if not IncrementRecord.find(table_name=table, record_date=histroy_day) or not MIN_AND_MAX:
                self._get_histroy_inscrement_data(table)

            _table = self.odps.get_table(table)

            if not IncrementRecord.find(table_name=table, record_date=pre_day):
                df = DataFrame(_table)
                num = df.filter(df.day == pre_day_str).count().execute()
                num = int(num)
                self.__get_min_and_max(pre_day_str, num)
                IncrementRecord.create(table_name=table, record_date=pre_day, numbers=num)
            return True, MIN_AND_MAX
        except Exception as exc:
            return False, exc.message


if __name__ == '__main__':
    ACCESS_ID = ''
    ACCESS_KEY = ''
    ODPS_PROJECT = ''
    od = MaxComputeAPI(ACCESS_ID, ACCESS_KEY, ODPS_PROJECT)
    od.check_data_consistency('dw_account_info', 'dm_account_subject_product', 'dim_institution')

