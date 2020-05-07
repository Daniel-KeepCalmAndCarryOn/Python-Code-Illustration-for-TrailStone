#!/usr/bin/env Python
# -*- coding:utf-8 -*-
# author: Yanggang Fang

'''
factorUpdate.py
描述：运行更新factora下因子数据到fatoterData下
'''

import os
import shutil
import importlib

import pandas as pd
import numpy as np

from cpa.io import h5Writer, h5Reader, csvReader
from cpa.io.reportWriter import ReportWriter
from cpa.io.csvReader import CSVPanelReader
from cpa.utils import logger, bar, series
from cpa.config import pathSelector, const
from cpa.factorModel import factorBase
from cpa.indicators.panelIndicators import returns
from cpa.factorProcessor.factorTest import DefaultFactorTest
from cpa.feed.feedFactory import DataFeedFactory
from cpa.resample.resampled import ResampledPanelFeed
from cpa.feed.baseFeed import AdvancedFeed

class FactorUpdate:
    '''因子检测数据写入及更新'''

    logger = logger.getLogger("factorUpdate")

    def __init__(self, instruments, market=bar.Market.STOCK, start=None, end=None,
                 testFreq=None, isRelReturn=False, fee=0.003, lag=1):
        '''
        初始化因子检测参数
        param instruments: 代码 "SZ50", "HS300", or "ZZ500"
        param market: 市场 bar.Market.STOCK, or bar.Market.FUTURES
        param frequency: 数据频率 bar.Frequency.MINUTE or bar.Frequency.HOUR
        param start: 因子检测开始时间，当为空值时将使用H5DataReader的默认开始时间
        param end: 因子检测结束时间，当为空值时将使用H5DataReader的默认结束时间
        param testFreq: 测试的resample频率
        param isRelReturn: True为计算相对收益， False为计算绝对收益
        param fee: 开仓手续费，用于计算交易成本
        '''
        self.instruments = instruments
        self.market = market
        self.start = start
        self.end = end
        self.newFactor = []
        self.factorDefPath = pathSelector.PathSelector.getFactorDefPath()
        self.factorDataPath = pathSelector.PathSelector.getFactorFilePath()
        self.fee = fee
        self.isRelReturn = isRelReturn
        self.lag = lag
        #设置要回测的时间频率，默认测试 5，30, 60, 120分钟的
        self.resampleFreqNum = [bar.Frequency.MINUTE5,
                                bar.Frequency.MINUTE30,
                                bar.Frequency.HOUR,
                                bar.Frequency.HOUR2] if not testFreq else testFreq
        self.resampleFreqStr = [const.DataFrequency.freq2lable(freq) for freq in self.resampleFreqNum]

        # 存储resample相关对象的字典
        self.reasampleFeedDict = {}
        self._return_Dict = {}
        self.rawFactorDict = {}
        self.factorTesterDict = {}
        self.dictOldResultDict = {}
        self.dictFilePathDict = {}

    def getPanelFeed(self):
        '''获取一个新的panelFeed'''
        panelFeed = DataFeedFactory.getHistFeed(instruments=self.instruments,
                                                market=self.market,
                                                frequency=bar.Frequency.MINUTE,
                                                start=self.start,
                                                end=self.end)
        return panelFeed

    def getBenchPanel(self):
        '''获取基准指数panel'''
        benchNameDict = {"SZ50": "IH.CCFX.csv", "HS300": "IF.CCFX.csv", "ZZ500": "IC.CCFX.csv"}
        if self.instruments in ["SZ50", "HS300", "ZZ500"]:
            fileName = benchNameDict[self.instruments]
        else:
            self.logger.info("The input instruments do not have benchmark. Please re-input.")
            return
        filePath = pathSelector.PathSelector.getDataFilePath(market=const.DataMarket.FUTURES, types=const.DataType.OHLCV,
                                                frequency=const.DataFrequency.MINUTE, fileName=fileName)
        indexReader = CSVPanelReader(filePath=filePath,
                                     fields=['open', 'high', 'low', 'close', 'volume'],
                                     frequency=bar.Frequency.MINUTE,
                                     isInstrumentCol=False,
                                     start=self.start)
        indexReader.loads()
        benchPanel = series.SequenceDataPanel.from_reader(indexReader)
        return benchPanel

    def newFactorList(self):
        '''获取新增的因子列表'''
        allFactors = [factor.split('.')[0] for factor in os.listdir(self.factorDefPath) \
                      if factor not in ['__init__.py', '__pycache__']]
        # self.logger.info("All factors defined: {}".format(allFactors))
        self.newFactor = sorted(list(set(allFactors) - set(os.listdir(self.factorDataPath))))
        if self.newFactor:
            self.logger.info("The new factors:{}".format(self.newFactor))
        else:
            self.logger.info("No new factors seen, the factor updating process will end soon")

    def writeNewFactor(self):
        '''
        存储数据文件
        '''
        self.newFactorList()
        if self.newFactor:  # 仅在有新增因子的情况下才进行后续的因子计算、检验及存储
            for factor in self.newFactor:  # 对新增因子列表里的因子进行计算和数据存储
                if factor == 'broker':
                    continue

                self.logger.info(
                    "****************** Writing FactorData for {} ******************".format(factor))
                modulePath = "cpa.factorPool.factors.{}".format(factor)  # 因子模块路径
                module = importlib.import_module(modulePath)  # 导入模块
                factorObject = getattr(module, 'Factor')  # 获取因子对象的名称 e.g. cpa.factorPool.factors.dmaEwv.Factor
                panelFeed = self.getPanelFeed()  # 为新的因子匹配一个新的panelFeed

                # 计算绝对收益
                if self.isRelReturn is False:
                    # 对各resample周期创建相应的格模块类
                    for freqNum, freqStr in zip(self.resampleFreqNum, self.resampleFreqStr):
                        self.reasampleFeedDict[freqStr] = ResampledPanelFeed(panelFeed, freqNum)
                        self._return_Dict[freqStr] = returns.Returns(self.reasampleFeedDict[freqStr],
                                                                     lag=self.lag,
                                                                     maxLen=1024)
                        self.rawFactorDict[freqStr] = factorBase.FactorPanel(self.reasampleFeedDict[freqStr], factorObject)
                        self.factorTesterDict[freqStr] = DefaultFactorTest(self.reasampleFeedDict[freqStr],
                                                                           self.rawFactorDict[freqStr],
                                                                           self._return_Dict[freqStr],
                                                                           indicators=['IC', 'rankIC', 'beta', 'gpIC',
                                                                                       'tbdf', 'turn', 'groupRet'],
                                                                           lag=self.lag,
                                                                           cut=0.1,
                                                                           fee=self.fee)
                    panelFeed.run(_print=True)  # 由panelFeed同时驱动各resampleFeed

                # 计算相对收益
                elif self.isRelReturn is True:
                    # 生成一个存放resampleFeed的字典
                    for freqNum, freqStr in zip(self.resampleFreqNum, self.resampleFreqStr):
                        self.reasampleFeedDict[freqStr] = ResampledPanelFeed(panelFeed, freqNum)
                    baseFeedDict = {"base": panelFeed}  # panelFeed字典
                    combinedDict = {**baseFeedDict, **self.reasampleFeedDict}  #合并字典
                    benchPanel = self.getBenchPanel()  # 基准指数panel
                    advFeed = AdvancedFeed(feedDict=combinedDict, panelDict={'bench': benchPanel})

                    for freqStr in self.resampleFreqStr:
                        # 对各resample周期创建相应的格模块类
                        self._return_Dict[freqStr] = returns.RelativeReturns(advFeed,
                                                                             isResample=True,
                                                                             resampleType=freqStr,
                                                                             lag=self.lag,
                                                                             maxLen=1024)
                        self.rawFactorDict[freqStr] = factorBase.FactorPanel(self.reasampleFeedDict[freqStr],
                                                                             factorObject)
                        # self.rawFactorDict[freqStr] = factorBase.FactorPanel(advFeed,
                        #                                                      factorObject,
                        #                                                      isResample=True,
                        #                                                      resampleType=freqStr)
                        self.factorTesterDict[freqStr] = DefaultFactorTest(advFeed,
                                                                           self.rawFactorDict[freqStr],
                                                                           self._return_Dict[freqStr],
                                                                           isResample = True,
                                                                           resampleType = freqStr,
                                                                           indicators = ['IC', 'rankIC', 'beta', 'gpIC',
                                                                                       'tbdf', 'turn', 'groupRet'],
                                                                           lag=self.lag,
                                                                           cut=0.1,
                                                                           fee=self.fee)
                    advFeed.run(_print=True)  # 由advancedFeed同时驱动各resampleFeed

                # 若数据长度不符合因子检验标准，则不存储
                if len(self._return_Dict[self.resampleFreqStr[0]]) <= 2 * self.lag:
                    self.logger.warning(
                        "The length of the return panel <= 2 * the required lag. Data will not be saved.")
                    return

                # 写h5文件和图表
                for freqStr in self.resampleFreqStr:
                    h5PanelWriter = h5Writer.H5PanelWriter(factor, self.factorTesterDict[freqStr])
                    h5PanelWriter.write(mode="new")
                    reportWriter = ReportWriter(factorName=factor,
                                                defaultFactorTest=self.factorTesterDict[freqStr])
                    reportWriter.write()

    def updateFactor(self, factor, nBizDaysAhead=30):
        '''
        续写一个因子文件夹下的所有文件
        param factor: 因子名
        param nBizDaysAhead: 以旧数据结束日期提前n个工作日开始计算新数据，根据策略需要调整
                             例如使用MA20的策略，对于2h的数据，至少要提前10个工作日
        '''
        self.logger.info("****************** Updating FactorData for {} ******************".format(factor))

        factorReader = h5Reader.H5BatchPanelReader(factorName=factor, frequency=None, allFolders=True)
        factorReader.prepareOutputData()
        dateRangeDict = factorReader.getDateRange()  # 获取存放首尾数据日期的字典
        endDateList = sorted([range[1] for range in dateRangeDict.values()])  # 取所有的数据结束日期， 并排序
        endDate = endDateList[-1].to_pydatetime()  # 取所有数据结束日期中最晚的一个
        timeDiff = pd.tseries.offsets.BusinessDay(n=nBizDaysAhead)  # 比结束日期提前n个工作日开始计算新数据
        self.start = endDate - timeDiff  # 计算新数据所开始的时间
        self.logger.info("The end time in the original data is {}\n"
                         "The input time difference is {}\n"
                         "The start time for calculating the new data is {}\n"
                         "The end time for calculating the new data is {}\n"
                         .format(endDate, timeDiff, self.start, self.end))
        panelFeed = self.getPanelFeed()  # 以新的start获取一个新的panelFeed

        modulePath = "cpa.factorPool.factors.{}".format(factor)  # 因子模块路径
        module = importlib.import_module(modulePath)  # 导入模块
        factorObject = getattr(module, 'Factor')  # 获取因子对象的名称 e.g. cpa.factorPool.factors.dmaEwv.Factor


        for freqNum, freqStr in zip(self.resampleFreqNum, self.resampleFreqStr):
            folderPath = pathSelector.PathSelector.getFactorFilePath(factorName=factor, factorFrequency=freqStr)
            # 读取因子检测的参数值
            csvFileName = [name for name in os.listdir(folderPath) if name.endswith(".csv")][0]
            csvFilePath = os.path.join(folderPath, csvFileName)
            fields = ["frequency", "lag", "nGroup", "cut", "fee", "poolNum"]
            settingReader = csvReader.CSVPanelReader(filePath=csvFilePath,
                                                     fields=fields,
                                                     frequency=freqNum,
                                                     isInstrumentCol=False)
            settingReader.loads()

            # 读取不同周期的h5文件
            freqReader = h5Reader.H5BatchPanelReader(factorName=factor,
                                                     frequency=freqNum,
                                                     allFolders=False)
            freqReader.prepareOutputData()  # 存入相应的字典中
            oldResultDict = freqReader.to_frame()  # 获取存放dataframe数据的字典
            filePathDict = freqReader.getFilePath()  # 获取原来H5文件的路径

            # 对各resample周期创建相应的模块类
            self.dictOldResultDict[freqStr] = oldResultDict
            self.dictFilePathDict[freqStr] = filePathDict
            self.reasampleFeedDict[freqStr] = ResampledPanelFeed(panelFeed, freqNum)
            self._return_Dict[freqStr] = returns.Returns(self.reasampleFeedDict[freqStr], lag=self.lag, maxLen=1024)
            self.rawFactorDict[freqStr] = factorBase.FactorPanel(self.reasampleFeedDict[freqStr], factorObject)
            self.factorTesterDict[freqStr] = DefaultFactorTest(feed=self.reasampleFeedDict[freqStr],
                                                               factorPanel=self.rawFactorDict[freqStr],
                                                               returnPanel=self._return_Dict[freqStr],
                                                               indicators=['IC', 'rankIC', 'beta', 'gpIC',
                                                                           'tbdf', 'turn', 'groupRet'],
                                                               lag=self.lag,
                                                               cut=0.1,
                                                               fee=self.fee)

        panelFeed.run(_print=True)  # 由panelFeed同时驱动各resampleFeed


        for freqStr, oldResultDict in self.dictOldResultDict.items():
            # 将旧的文件移入以时间命名的文件夹
            oldDateTime = list(self.dictOldResultDict[freqStr].keys())[0][-16:-3]
            freqFolderPath = pathSelector.PathSelector.getFactorFilePath(factorName=factor, factorFrequency=freqStr)
            destFolderPath = os.path.join(freqFolderPath, oldDateTime)
            if not os.path.exists(destFolderPath):
                os.mkdir(destFolderPath)
            fileList = [name for name in os.listdir(freqFolderPath) if
                              os.path.isfile(os.path.join(freqFolderPath, name))]
            for file in fileList:
                sourceFilePath = os.path.join(freqFolderPath, file)
                shutil.move(sourceFilePath, destFolderPath)

            # 写新的h5文件
            h5PanelWriter = h5Writer.H5PanelWriter(factorName=factor,
                                                   defaultFactorTest=self.factorTesterDict[freqStr])
            h5PanelWriter.write(mode="append", oldResultDict=oldResultDict)  # 使用append模式写入

        for freqNum in self.resampleFreqNum:
            # 写新的图表文件
            secondReader = h5Reader.H5BatchPanelReader(factorName=factor,
                                                       frequency=freqNum)
            secondReader.prepareOutputData()
            reportWriter = ReportWriter(factorName=factor,
                                        h5BatchPanelReader=secondReader,
                                        csvPanelReader=settingReader)
            reportWriter.write()

    def updateFactorPool(self, nBizDaysAhead=30):
        '''
        续写factorData下所有的因子文件夹
        param nBizDaysAhead: 以旧数据结束日期提前n个工作日开始计算新数据，根据策略需要调整
                             例如使用MA20的策略，对于2h的数据，至少要提前10个工作日
        '''
        factorNameList = [name for name in os.listdir(self.factorDataPath) if  # 取factorData文件下的子文件夹名
                          os.path.isdir(os.path.join(self.factorDataPath, name))]
        for factor in factorNameList:
            self.updateFactor(factor, nBizDaysAhead=nBizDaysAhead)

if __name__ == "__main__":


    "写新的因子检测数据"
    # 写factorData中不存在，但是factors中存在的因子
    # 如果要重新写某个因子，需将factorData下原来的因子文件夹删除或者重命名
    factorUpdate = FactorUpdate(instruments="SZ50", start="20150701", end="20150731", isRelReturn=True)
    factorUpdate.writeNewFactor()

    '''续写功能，仅在原有数据非常长的情况下使用，使用前建议咨询项目组成员'''
    # 续写factorData下某一个因子
    # factorUpdate = FactorUpdate(instruments="SZ50", end="20151031", isRelReturn=False)
    # factorUpdate.updateFactor("maPanelFactor", nBizDaysAhead=30)

    # 续写factorData下所有的因子
    # factorUpdate.updateFactorPool()
