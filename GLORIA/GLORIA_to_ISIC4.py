import pandas as pd

def run_script():
    gloria_to_hscpc_concordance = get_gloria_hscpc_concordance()
    hscpc_to_isic4_concordance = get_hscpc_isic4_concordance()
    isic4_to_naics2012_concordance = get_isic4_naics2012_concordance()
    naics_to_bea_concordance = get_naics_bea_concordance()
    gloria_hscpc_isic4_naics_bea = combine_concordances(
        gloria_to_hscpc_concordance, hscpc_to_isic4_concordance,
        isic4_to_naics2012_concordance,naics_to_bea_concordance)
    gloria_bea_concordance = isolcate_gloria_bea_concordance(
        gloria_hscpc_isic4_naics_bea)
    
    return(gloria_bea_concordance)

def get_gloria_hscpc_concordance():
    gloria_to_hscpc_binary = pd.read_csv("GLORIA_HSCPC_Headers.csv", dtype=str)
    gloria_to_hscpc_binary = gloria_to_hscpc_binary.drop(
        ['Code','GLORIA','Checksum'], axis=1)
    gloria_to_hscpc_binary = gloria_to_hscpc_binary.drop(index=[0,1], axis=0)
    gloria_to_hscpc_long = pd.melt(gloria_to_hscpc_binary, 
                                   id_vars=['RowIndex'])
    gloria_to_hscpc_concordance = (gloria_to_hscpc_long
                            .loc[gloria_to_hscpc_long['value']== '1']
                            .rename(columns={'variable':'GLORIA Sector',
                                             'RowIndex':'HSCPC Sector'}))
    gloria_to_hscpc_concordance = (
        gloria_to_hscpc_concordance[['GLORIA Sector','HSCPC Sector']]
        )
    return gloria_to_hscpc_concordance

def get_hscpc_isic4_concordance():
    hscpc_to_isic4_binary = pd.read_csv("HSCPC_ISIC4_Headers.csv", dtype=str, )
    hscpc_to_isic4_binary = hscpc_to_isic4_binary.drop(
        ['Code','HSCPC description'], axis=1)
    hscpc_to_isic4_binary = hscpc_to_isic4_binary.drop(index=[0], axis=0)
    hscpc_to_isic4_long = pd.melt(hscpc_to_isic4_binary, 
                                   id_vars=['HSCPC_RowIndex'])
    # hscpc_to_isic4_long.to_csv('hscpc_to_isic4_long.csv',index = False)
    
    hscpc_to_isic4_concordance = (hscpc_to_isic4_long
                            .loc[hscpc_to_isic4_long['value'] == '1']
                            .rename(columns={'variable':'ISIC4 Sector',
                                             'HSCPC_RowIndex':'HSCPC Sector'}))
    hscpc_to_isic4_concordance = (
        hscpc_to_isic4_concordance[['ISIC4 Sector','HSCPC Sector']]
        )
    return hscpc_to_isic4_concordance


def get_isic4_naics2012_concordance():
    isic4_to_naics2012_concordance = pd.read_csv("ISIC4_NAICS2012US.csv", 
                                                 dtype=str)
    isic4_to_naics2012_concordance = (
        isic4_to_naics2012_concordance[['ISIC4Code','NAICS2012Code']]
        .rename(columns={'ISIC4Code':'ISIC4 Sector',
                         'NAICS2012Code':'NAICS 2012 Sector'})
        )
    isic4_to_naics2012_concordance['ISIC4 Sector'] = (
        isic4_to_naics2012_concordance['ISIC4 Sector']
        .str.replace(r'^(0+)', '', regex=True).fillna('0')
        )
    return isic4_to_naics2012_concordance

def get_naics_bea_concordance():
    useeio_concordances = pd.read_csv('useeio_internal_concordance.csv', 
                                      dtype=str)
    naics_to_bea_concordance = (
        useeio_concordances[['BEA_Summary','NAICS2012']]
        .rename(columns={'BEA_Summary':'BEA Summary',
                         'NAICS2012':'NAICS 2012 Sector'})
        )
    return naics_to_bea_concordance

def combine_concordances(gloria_to_hscpc_concordance,
                         hscpc_to_isic4_concordance,
                         isic4_to_naics2012_concordance,
                         naics_to_bea_concordance):
    gloria_hscpc_isic4 = gloria_to_hscpc_concordance.merge(
        hscpc_to_isic4_concordance, on='HSCPC Sector', how='left')
    gloria_hscpc_isic4_naics2012 = gloria_hscpc_isic4.merge(
        isic4_to_naics2012_concordance, on='ISIC4 Sector', how='left')
    gloria_hscpc_isic4_naics_bea = gloria_hscpc_isic4_naics2012.merge(
        naics_to_bea_concordance, on='NAICS 2012 Sector', how='left')
    return gloria_hscpc_isic4_naics_bea

def isolcate_gloria_bea_concordance(gloria_hscpc_isic4_naics_bea):
    gloria_bea = gloria_hscpc_isic4_naics_bea[['GLORIA Sector',
                                               'BEA Summary']]
    gloria_bea_concordance = gloria_bea.drop_duplicates()
    
    return gloria_bea_concordance

result = run_script()

result.to_csv('gloria_to_bea_concordance.csv', index=False)