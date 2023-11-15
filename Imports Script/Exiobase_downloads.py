import pymrio
from pathlib import Path
import pickle as pkl
import yaml
import pandas as pd

dataPath = Path(__file__).parent / 'Data'
e_Path = Path(__file__).parent / 'Exiobase Models'
m_Path = Path(__file__).parent / 'Exiobase M-Arrays'
ef_Path = Path(__file__).parent / 'Exiobase EF-Arrays'
bt_Path = Path(__file__).parent / 'Exiobase Bilateral Trade'
tt_Path = Path(__file__).parent / 'Exiobase Trade Totals'
ear_Path = Path(__file__).parent / 'Exiobase All Resources'
m_t = 'pxp' #model type

with open(dataPath.parent / "Data" / "exio_config.yml", "r") as file:
    config = yaml.safe_load(file)

def run_All(Year_Start=1997, Year_End=2023, download=False):
    years = years_List(Year_Start, Year_End)
    if download == True:
        download_And_Store_Exiobase(years)
    all_dict = {}
    for y in years:
        d = {}
        e = open_Exiobase_Model(y)
        m = extract_M(e,y)
        t, b = e_Trade(e,y) 
        ef = extract_EFs(y)
        d['M'] = m
        d['Trade Total'] = t
        d['Bilateral Trade'] = b
        d['EFs'] = ef
        all_pickling(d,y)
        all_dict[y] = d
        
    
    return all_dict
            
def years_List(Year_Start, Year_End):
    '''
    A function to set the range of years the user desires to download exiobase
    models for, or to extract components of those models.
    '''
    Year_End += 1 
    years = list(range(Year_Start,Year_End))
    return years

def download_And_Store_Exiobase(years):
    '''
    A function to download
    '''
    exio3 = pymrio.download_exiobase3(storage_folder=e_Path,
                                      system=m_t,years=years)

def open_Exiobase_Model(year):
    '''
    A function to open the downloaded exiobase model for a given year
    '''
    file_n = 'IOT_'+str(year)+'_pxp.zip' 
    file = e_Path / file_n
    e = pymrio.parse_exiobase3(file)
    return e
        

def extract_M(e,year):
    exio_m = e.impacts.M                                                  
    file_n = 'exio3_multipliers_'+str(year)+'.pkl'                                      
    pkl.dump(exio_m, open(m_Path / file_n, 'wb'))
    return exio_m

def extract_EFs(year):
    file_n = 'exio3_multipliers_'+str(year)+'.pkl' 
    file = m_Path/file_n
    M_df = pkl.load(open(file,'rb'))
        
    fields = {**config['fields'], **config['flows']}
        
    M_df = M_df.loc[M_df.index.isin(fields.keys())]
    M_df = (M_df
            .transpose()
            .reset_index()
            .rename(columns=fields))
    M_df = M_df.assign(Year = str(year))
    file_n = 'exio3_EFs_'+str(year)+'.pkl'                                      
    pkl.dump(M_df, open(ef_Path / file_n, 'wb'))
    return M_df

def e_Trade(e, year, region='US'):
    t_file_n = 'exio_total_trade_'+str(year)+'.pkl'
    t_file = tt_Path/t_file_n
    b_file_n = 'exio_bilateral_trade_'+str(year)+'.pkl'
    b_file = bt_Path/b_file_n
    trade = pymrio.IOSystem.get_gross_trade(e)
    totals = trade[1]
    # ^^ df with gross total imports and exports per sector and region
    bilat = trade[0]
    bilat = bilat[region]
    # ^^ df with rows: exporting country and sector, columns: importing countries
    pkl.dump(totals, open(t_file, 'wb'))
    pkl.dump(bilat, open(b_file, 'wb'))
    
    return(totals, bilat)

def all_pickling(d,year):
    a_file_n = 'exio_all_resources_'+str(year)+'.pkl'
    a_file = ear_Path/a_file_n
    pkl.dump(d, open(a_file, 'wb'))
    

