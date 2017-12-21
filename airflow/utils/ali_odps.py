# -*- coding:utf-8 -*-
# __author__ = majing

from odps import ODPS


class ConnOdps(object):
    """
    connect to odps
    """
    aa = ''
    def __init__(self, access_id, access_key, project):
        self.access_id = access_id
        self.access_key = access_key
        self.project = project
        self.__initMaxCompute()

    def __initMaxCompute(self):
        self.odps = ODPS(self.access_id, self.access_key, self.project)





