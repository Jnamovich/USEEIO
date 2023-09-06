import pymrio
from pathlib import Path
import pickle as pkl
import yaml
import pandas as pd

dataPath = Path(__file__).parent / 'Data'
e_Path = Path(__file__).parent / 'Exiobase Models'
m_Path = Path(__file__).parent / 'Exiobase M-Arrays'
ef_Path = Path(__file__).parent / 'Exiobase EF-Arrays'
m_t = 'pxp' #model type

with open(dataPath.parent / "Data" / "exio_config.yml", "r") as file:
    config = yaml.safe_load(file)
    
years = []
for i in range(1997,2023,1):
    years.append(i)


def download_And_Store_Exiobase():
    exio3 = pymrio.download_exiobase3(storage_folder=e_Path,system=m_t,years=years)

def extract_M(years):
    for i in years:
        file_n = 'IOT_'+str(i)+'_pxp.zip' 
        file = e_Path / file_n
        e = pymrio.parse_exiobase3(file)
        exio_m = e.impacts.M                                                   
        file_n = 'exio3_multipliers_'+str(i)+'.pkl'                                      
        pkl.dump(exio_m, open(m_Path / file_n, 'wb'))

def extract_EFs():
    for i in years:
        file_n = 'exio3_multipliers_'+str(i)+'.pkl' 
        file = m_Path/file_n
        M_df = pkl.load(open(file,'rb'))
        
        fields = {**config['fields'], **config['flows']}
        
        M_df = M_df.loc[M_df.index.isin(fields.keys())]
        M_df = (M_df
                .transpose()
                .reset_index()
                .rename(columns=fields))
        M_df = M_df.assign(Year = str(i))
        file_n = 'exio3_EFs_'+str(i)+'.pkl'                                      
        pkl.dump(M_df, open(ef_Path / file_n, 'wb'))

def create_Time_Series_EF_DF():
    df = []
    for i in years:
        file_n = 'exio3_EFs_'+str(i)+'.pkl' 
        file = ef_Path/file_n
        e_df = pkl.load(open(file,'rb'))
        df.append(e_df)
    df = pd.concat(df)
    return(df)

# file_n = 'exio3_EFs_2022.pkl' 
# file = ef_Path/file_n
# e_df = pkl.load(open(file,'rb'))


# download_And_Store_Exiobase()
# extract_M(years)
# extract_EFs()
df = create_Time_Series_EF_DF()
df.to_csv('exiobase_multipliers_ts.csv', index=False)