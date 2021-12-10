#!/usr/bin/env /home/jcm/env/sidxfa/bin/python

from mydataframe import dataframe
import pandas as pd

path = '/home/jcm/projects/SIDxFARanInventory/LocalFiles/List Report - CELL 20191210.xlsx'
test = dataframe()
test.setSourceFile(path)
test.createExcelDataframe('Sheet1', skiprows=2)

inputPath = test.showSourceFile()
print("Input path: {}".format(inputPath))

print(test.df)
