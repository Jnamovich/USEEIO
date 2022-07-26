import pymrio
import pandas as pd
import pickle as pkl
import numpy as np
from pathlib import Path
import time
dataPath = Path(__file__).parent

def runCode():
    sourceScheme, targetScheme, translationScheme1, translationScheme2 = importIndustryClassificationFiles()
    unifiedSchema = combineSchema(sourceScheme,targetScheme, translationScheme1, translationScheme2) 
    MVector,sectors = extractMVectorPlusSectors()
    downloadMRIO = True
    if downloadMRIO is True:
        downloadAndStoreMRIO()
    MVector = extractMVector()
    
    return(unifiedSchema)

def importIndustryClassificationFiles():
    try:
        sourceScheme = pd.read_csv('NAICS2017-EXIOBASE.csv',dtype=str)
    except FileNotFoundError:
        print("Your Source Industry Classification Scheme .csv file was not found")
    try:
        targetScheme = pd.read_csv('NAICS-BEA&USEEIO', dtype=str)
    except FileNotFoundError:
        print("Your Target Industry Classification Scheme .csv file was not found")
    try:
        translationScheme1 = pd.read_csv('NAICS2017_NAICS2012.csv',dtype=str)
    except FileNotFoundError:
        print("Your Translsation Industry Classification Scheme 1 .csv file was not found")
    try:
        translationScheme2 = None #pd.read_csv('NAICS-ISIC4_Code_Mapping.csv', dtype=str)
    except FileNotFoundError:
        print("Your Translsation Industry Classification Scheme 2 .csv file was not found")
    return(sourceScheme, targetScheme, translationScheme1, translationScheme2)

def combineSchema(sourceScheme, targetScheme, translationScheme1, translationScheme2):
    if translationScheme1 is not None and translationScheme2 is not None:
        tdf1 = sourceScheme.merge(translationScheme1, on='HSCPC', how='outer')
        tdf2 = tdf1.merge(translationScheme2, on='ISIC 4', how='outer')
        df = tdf2.merge(targetScheme, on='NAICS', how='outer')
    elif translationScheme1 is None and translationScheme2 is not None:
        tdf = sourceScheme.merge(translationScheme2, on='ISIC 4', how='outer')
        df = tdf.merge(targetScheme, on='NAICS', how='outer')
    elif translationScheme1 is not None and translationScheme2 is None:
        tdf = sourceScheme.merge(translationScheme1, on='NAICS2017', how='outer')
        df = tdf.merge(targetScheme, on='NAICS2012', how='outer')
    else:
        df = sourceScheme.merge(targetScheme, on='ISIC 4', how='outer')
    df = df['EXIOBASE','BEA_Summary']    
    return(df)

def downloadAndStoreMRIO():
    exio3 = pymrio.parse_exiobase3('IOT_2022_pxp.zip')
    pkl.dump(exio3,open(dataPath/'exio3.pkl', 'wb'))
    
def extractMVectorPlusSectors():
    exio3 = pkl.load(open(dataPath/'exio3.pkl','rb'))
    impactsPerUCurrency = exio3.impacts.M
    sectors = exio3.get_sectors()
    return(impactsPerUCurrency,sectors)

def importCoefficients():
    year = '2020'
    tradePartners = ['ROW','Canada','Mexico','China','Europe']
    removeRows=[0,1,2,3,4,5,6,8]
    regionalImports = {}
    for partner in tradePartners:
        partnerURL = 'https://www.bea.gov/system/files/2021-12/Import%20Matrix%20'+ partner +'.xlsx'
        partnerDF = pd.read_excel(partnerURL,sheet_name=year, skiprows=removeRows,index_col=0).rename(columns={'Unnamed: 0':'Industry/Commodity Code:','F050':partner})
        if regionalImports.empty:
            regionalImports = partnerDF
        else:
            regionalImports = pd.concat([regionalImports,partnerDF], axis=1)

    rowURL = r'https://www.bea.gov/system/files/2021-12/Import%20Matrix%20ROW.xlsx'
    caURL = r'https://www.bea.gov/system/files/2021-12/Import%20Matrix%20Canada.xlsx'
    mxURL = r'https://www.bea.gov/system/files/2021-12/Import%20Matrix%20Mexico.xlsx'
    cnURL = r'https://www.bea.gov/system/files/2021-12/Import%20Matrix%20China.xlsx'
    euURL = r'https://www.bea.gov/system/files/2021-12/Import%20Matrix%20Europe.xlsx'
    removeRows=[0,1,2,3,4,5,6,8]
    rowDF = pd.read_excel(rowURL,sheet_name=year, skiprows=removeRows,index_col=0).rename(columns={'Unnamed: 0':'Industry/Commodity Code:','F050':'ROW'})
    caDF = pd.read_excel(caURL,sheet_name=year, skiprows=removeRows,index_col=0).rename(columns={'Unnamed: 0':'Industry/Commodity Code:','F050':'CA'})
    mxDF = pd.read_excel(mxURL,sheet_name=year, skiprows=removeRows,index_col=0).rename(columns={'Unnamed: 0':'Industry/Commodity Code:','F050':'MX'})
    cnDF = pd.read_excel(cnURL,sheet_name=year, skiprows=removeRows,index_col=0).rename(columns={'Unnamed: 0':'Industry/Commodity Code:','F050':'CN'})
    euDF = pd.read_excel(euURL,sheet_name=year, skiprows=removeRows,index_col=0).rename(columns={'Unnamed: 0':'Industry/Commodity Code:','F050':'EU'})
    rowImports = rowDF[['ROW']]
    caImports = caDF[['CA']]
    mxImports = mxDF[['MX']]
    cnImports = cnDF[['CN']]
    euImports = euDF[['EU']]
    regionalImports = pd.concat([rowImports,caImports,mxImports,cnImports,euImports], axis=1)
    importCoefficients = regionalImports.div(regionalImports.sum(axis=1),axis=0)
    
    return(importCoefficients)
    
def regionalAveraging():
    exio3 = pkl.load(open(dataPath/'exio3.pkl','rb'))
    exio3x = exio3.x
    exio3x = exio3x.rename(columns={'region':'TiVA Region'}).reset_index()
    regions = pd.read_csv('countryRegionDF.csv').rename(columns={'ISO 3166-alpha-2':'region'})
    regionTaxonomy = regions[["TiVA Region","region"]]
    regionOutputs = regionTaxonomy.merge(exio3x, on='region', how='outer')
    regionOutputs['indout_subtotal'] = regionOutputs[['TiVA Region','sector','indout']].groupby(['TiVA Region','sector']).transform('sum')
    regionOutputs['contributions'] = regionOutputs['indout']/regionOutputs['indout_subtotal']
    regionOutputs = regionOutputs.fillna(0)
    
    exio3MMatrix = exio3.impacts.M
    exio3EmissionsMatrix = exio3MMatrix[37:40]
    exio3EmissionsMatrix = exio3EmissionsMatrix.transpose().reset_index().rename(columns={'Carbon dioxide (CO2) IPCC categories 1 to 4 and 6 to 7 (excl land use, land use change and forestry)':'Carbon Dioxide (CO2)','Methane (CH4) IPCC categories 1 to 4 and 6 to 7 (excl land use, land use change and forestry)':'Methane (CH4)','Nitrous Oxide (N2O) IPCC categories 1 to 4 and 6 to 7 (excl land use, land use change and forestry)':'Nitrous Oxide (N2O)'})
    
    outputEmissionsDF = regionOutputs.merge(exio3EmissionsMatrix, how='left', left_on=['region','sector'],right_on=['region','sector'])
    outputEmissionsDF['(Weighted) Carbon Dioxide (CO2)'] = outputEmissionsDF['Carbon Dioxide (CO2)']*outputEmissionsDF['contributions']
    outputEmissionsDF['(Weighted) Methane (CH4)'] = outputEmissionsDF['Methane (CH4)']*outputEmissionsDF['contributions']
    outputEmissionsDF['(Weighted) Nitrous Oxide (N2O)'] = outputEmissionsDF['Nitrous Oxide (N2O)']*outputEmissionsDF['contributions']
    
    outputEmissionsDF = outputEmissionsDF.drop(['Carbon Dioxide (CO2)','Methane (CH4)','Nitrous Oxide (N2O)','indout','contributions'], axis=1)
    weightedEmissionsCollapsed = outputEmissionsDF.groupby(['TiVA Region','sector','indout_subtotal']).agg({'(Weighted) Carbon Dioxide (CO2)': 'sum', '(Weighted) Methane (CH4)': 'sum','(Weighted) Nitrous Oxide (N2O)': 'sum'}).reset_index().rename(columns={'sector':'EXIOBASE'})
    useeioExiobase = pd.read_csv('USEEIO_EXIOBASE.csv', dtype=str)
    useeioExioImports = weightedEmissionsCollapsed.merge(useeioExiobase, on='EXIOBASE', how='left')

    return(useeioExioImports)

runCode()

    