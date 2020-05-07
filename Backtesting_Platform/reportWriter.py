#!/usr/bin/env Python
# -*- coding:utf-8 -*-
# author: Yanggang Fang
'''
因子检测报告图表写入模块
'''
import datetime
from cpa.utils import logger
from cpa.config import pathSelector
from cpa.config import const
from cpa.factorProcessor import factorTest


class ReportWriter:
    '''
    通过调用factorTest下的TestReportGenerator类来实现图表的计算和存储
    一种方式是通过传入factorTest下的DefaultFactorTest类对象，调用其下的几个panel
    还有一种是通过传入h5BatchPanelReader，通过读取h5文件来获取panel
    '''

    logger = logger.getLogger("ReportWriter")

    def __init__(self, factorName, defaultFactorTest=None, h5BatchPanelReader=None):
        '''
        param factorName: 因子名
        param defaultFactorTest: 因子检测类对象
        param h5BatchPanelReader: h5文件读取类对象
        '''
        self.factorName = factorName
        self.defaultFactorTest = defaultFactorTest
        self.h5BatchPanelReader = h5BatchPanelReader
        self.frequency = defaultFactorTest.frequency if defaultFactorTest\
                           else h5BatchPanelReader.frequency
        if self.defaultFactorTest and self.h5BatchPanelReader:
            self.logger.info("Either defaultFactorTest or h5BatchPanelReader must be None")
            return
        self.testReportGenerator = factorTest.TestReportGenerator(self.defaultFactorTest,
                                                                  self.h5BatchPanelReader)

    def write(self):
        '''
        将分层收益图和分层统计量分别写入对于的图和表文件
        '''
        currentDT = datetime.datetime.now()
        # 储存分层收益图
        figName = self.factorName + '_Report_' +\
                  const.DataFrequency.freq2lable(self.frequency) + '.png'
                  # currentDT.strftime("_%Y%m%d_%H%M") + '.png'
        path = pathSelector.PathSelector.getFactorFilePath(factorName=self.factorName,  # 因子计算数据的文件路径
                                                           factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                                           fileName=figName)
        self.testReportGenerator.plotGroupret(_show=False, path=path)
        # 储存分层统计量
        statisticFileName = self.factorName + '_Statistic_' +\
                            const.DataFrequency.freq2lable(self.frequency) + '.xls'
                            # currentDT.strftime("_%Y%m%d_%H%M") + '.xls'
        self.testReportGenerator.statistic(path=pathSelector.PathSelector.getFactorFilePath(
                                         factorName=self.factorName,  # 因子计算数据的文件路径
                                         factorFrequency=const.DataFrequency.freq2lable(self.frequency),
                                         fileName=statisticFileName))
