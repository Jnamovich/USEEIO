import pandas as pd
import yaml
import urllib
import json
from pathlib import Path

apiPath = Path(__file__).parents[1] / 'API'
dataPath = Path(__file__).parents[1] / 'Data'
conPath = Path(__file__).parents[1] / 'Concordances'
  
    
    
#%%

f_n = 'Import Matrix, __region__, After Redefinitions.csv'
r = ['Canada', 'China', 'Europe', 'Japan', 'Mexico', 
     'Rest of Asia and Pacific', 'Rest of World']
ri_df = pd.DataFrame()
for region in r:
    r_path = f_n.replace('__region__',region)
    df = pd.read_csv(dataPath / r_path,skiprows=3, index_col=(0)).drop(
        ['IOCode']).drop(['Commodities/Industries'], axis=1)
    df = df.apply(pd.to_numeric)
    df[region] = df[list(df.columns)].sum(axis=1)
    df = df.reset_index(inplace=False)
    ri_r = df[['IOCode',region]]
    if ri_df.empty:
        ri_df = ri_r
    else:
        ri_df = pd.merge(ri_df, ri_r, how='outer',on='IOCode')
    ri_df = ri_df.iloc[:-3]
    

#%%

def get_URL_Components(file):
    with open(apiPath / file) as f:
        try:
            print('Successfully Loaded',file[:-8],'URL Components')
            m = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            print(exc)
    return m

def get_CTY_CODE(file='country.txt'):
    l = []
    with open(conPath / file) as f:
        for line in f:
            a = line.split('|')
            l2 = []
            for item in a:
                l2.append(item.strip())
            if len(l2)>=3:
                l.append(l2)
    headers = l[0]
    df = pd.DataFrame(l, columns=headers)
    df = df.iloc[1:,:]
    df = df.rename(columns={'Code':'Census Code'})
    return(df)

def get_country_schema(cty,t_e='exio_tiva_concordance.csv'):
    df = pd.read_csv(conPath / t_e)
    df = df.rename(columns={'ISO 3166-alpha-2':'ISO Code', 
                            'BEA_AREAORCOUNTRY':'BEA'})
    e_t = df[['ISO Code', 'TiVA Region']]
    b_c = df[['ISO Code','BEA']].dropna(axis='index',how='any')
    b_d = b_c.set_index('ISO Code')['BEA'].to_dict()
    c_c = df[['ISO Code']].dropna(axis='index',how='any')
    c_c = pd.merge(cty,c_c,how='left',on='ISO Code').drop(columns='Name')
    c_d = c_c.set_index('ISO Code')['Census Code'].to_dict()
    return (b_d, c_d, e_t)

def create_Reqs(file,d):
    components = get_URL_Components(file)
    reqs = {}
    for year in components['years']:
        year = str(year)
        comp = components['url']
        req_url = comp['base_url']
        try: 
            a = comp['api_path']
            req_url += a
        except KeyError:
            pass
        for key,value in comp['url_params'].items():
            string = key+'='+value+'&'
            req_url += string
        req_url = req_url[:-1]
        req_url = req_url.replace('__year__',year)
        reqs[year] = req_url
    reqs = complete_URLs(reqs,d)
    print('Successfully Created All',file[:-8], 'Request URLs')
    return reqs

def complete_URLs(reqs,d):
    ctys = [value for key, value in d.items()]
    l = {}
    for cty in ctys:
        try:
            cty = str(cty)
        except ValueError:
            pass
        for key,value in reqs.items():
            url = value.replace('__areaorcountry__',cty)
            new_key = key+'_'+cty
            l[new_key]=url
    reqs = l
    return reqs

def make_reqs(reqs):
    d = {}
    for key, value in reqs.items():
        response = urllib.request.urlopen(value)
        data = json.loads(response.read())
        d[key] = data
    return d
        
cty=get_CTY_CODE()
b_d, c_d, e_t = get_country_schema(cty)
b_reqs = create_Reqs('BEA_API.yml', b_d)
c_reqs = create_Reqs('Census_API.yml', c_d)
