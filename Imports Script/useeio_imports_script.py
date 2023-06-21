import pandas as pd
import pymrio
import pickle as pkl
import yaml
import statistics
#from currency_converter import CurrencyConverter
from datetime import date
from pathlib import Path
from API_Imports_Data_Script import get_imports_data
#%%
''' 
VARIABLES:
path = data path, set to parent directory
t_df = dataframe of tiva region imports data
e = complete exiobase model
e_m = extracts m vector (containing emission factors per unit currency)
i_d = imports data
t_e = region mappings from BEA TiVA to exiobase countries
t_c = BEA TiVA import contributions coefficients, by BEA naics category for 
      available region datasets
e_u_b = exiobase to detail useeio concordance, binary format, from exiobase team
e_u_l = exiobase to detail useeio concordance, converted to long format
e_u = exiobase to detail useeio concordance, condensed long format
u_cc = complete useeio internal concordance
u_c = useeio detail to summary code concordance
r_i = imports, by NAICS category, from countries aggregated in 
      TiVA regions (ROW, EU, APAC)
p_d = dataframe prepared for final factor calculation
t_r_i = Import quantities, by Exiobase sector, mapped to TiVA-mapped Exiobase
        countries
t_r_i_u = Import quantities, by Exiobase sector and BEA sector,
          mapped to TiVA-mapped Exiobase countries
c_d = Contribution coefficient matrix
e_d = Exiobase emission factors per unit currency
'''

#%%
dataPath = Path(__file__).parent / 'Data'
conPath = Path(__file__).parent / 'Concordances'

#%%

with open(dataPath.parent / "Data" / "exio_config.yml", "r") as file:
    config = yaml.safe_load(file)


def run_script(io_level='Summary', year=2021):
    '''
    Runs through script to produce emission factors for U.S. imports.
    '''
    
    t_df = get_tiva_data(year=year)
    t_c = calc_tiva_coefficients(t_df)
    t_e = get_tiva_to_exio_concordance()
    e_u = get_exio_to_useeio_concordance()
    sr_i = get_subregion_imports()

    if io_level == 'Summary':
        u_c = get_detail_to_summary_useeio_concordance()
        sr_i = (sr_i.merge(u_c, how='left', on='BEA Detail', validate='m:1')
                  .drop_duplicates()
                  )
        t_c = t_c.rename(columns={'BEA Summary': 'BEA Summary'})
    else: # Detail
        print('ERROR: not yet implemented')
        sr_i = sr_i.rename(columns={'BEA Detail': 'BEA'})
        ## TODO adjust t_c


    #t_r_i_u = sr_i.merge(e_u, on='BEA Detail', how='left')

    p_d = sr_i.copy()
    p_d = p_d[['TiVA Region','CountryCode','BEA Summary','BEA Detail','Import Quantity']]
    # TODO WARNING ^^ this is creating some duplicates where an exiobase sector
    # maps to multiple detail sectors but still a single summary sector
    c_d = calc_contribution_coefficients(p_d)
    c_de = c_d.merge(e_u, on='BEA Detail', how='left')
    c_de = c_de[['TiVA Region','CountryCode','BEA Summary','BEA Detail',
                 'Exiobase Sector','Subregion Contribution to Summary',
                 'Subregion Contribution to Detail']]
    e_d = pull_exiobase_multipliers()
    
    multiplier_df = c_de.merge(e_d, how='left',
                              on=['CountryCode', 'Exiobase Sector'])
    multiplier_df = multiplier_df.melt(
        id_vars = [c for c in multiplier_df if c not in 
                   config['flows'].values()],
        var_name = 'Flow',
        value_name = 'EF')
    weighted_multipliers_bea_detail, weighted_multipliers_bea_summary = (
        calculate_specific_emission_factors(multiplier_df))
    weighted_multipliers_all = (
        calculate_emission_factors(multiplier_df))
    imports_multipliers = (calculateWeightedEFsImportsData(
        weighted_multipliers_all, t_c))

    imports_multipliers = (
        imports_multipliers
        # .assign(Compartment='air')
        .rename(columns={'Weighted_Import_EF': 'Amount'})
        .assign(Unit='kg / Euro')
        .assign(CurrencyYear=str(year))
        .assign(DataYear=str(year)) # Emissions year
        .assign(PriceType='Producer')
        )
    
    # Currency adjustment
    # c = CurrencyConverter(fallback_on_missing_rate=True)
    # exch = statistics.mean([c.convert(1, 'EUR', 'USD', date=date(year, 1, 1)),
    #                         c.convert(1, 'EUR', 'USD', date=date(year, 12, 30))])
    # imports_multipliers = (
    #     imports_multipliers
    #     .assign(Amount=lambda x: x['Amount']/exch)
    #     .assign(Unit='kg / USD')
    #     )
    #TODO Pricetype adjustment
    #TODO Flow Mapping
    
    return (p_d, imports_multipliers, weighted_multipliers_bea_detail, 
            weighted_multipliers_bea_summary,weighted_multipliers_all)


# TODO reflect the year of the data in the csv file
def get_tiva_data(year='2020'):
    '''
    Iteratively pulls BEA imports data matricies from stored csv file,
    extracts the Total Imports columns by region, and consolidates 
    into one dataframe. 
    
    https://apps.bea.gov/iTable/?reqid=157&step=1
    '''

    f_n = 'Import Matrix, __region__, After Redefinitions.csv'
    regions = {'Canada': 'CA',
               'China': 'CN', 
               'Europe': 'EU',
               'Japan': 'JP',
               'Mexico': 'MX', 
               'Rest of Asia and Pacific': 'APAC',
               'Rest of World': 'ROW',
               }
    ri_df = pd.DataFrame()
    for region, abbv in regions.items():
        r_path = f_n.replace('__region__', region)
        df = (pd.read_csv(dataPath / r_path, skiprows=3, index_col=0)
                 .drop(['IOCode'])
                 .drop(['Commodities/Industries'], axis=1)
                 .dropna()
                 .apply(pd.to_numeric)
                 )
        df[abbv] = df[list(df.columns)].sum(axis=1)
        df = df.reset_index(inplace=False)
        ri_r = df[['IOCode', abbv]]
        if ri_df.empty:
            ri_df = ri_r
        else:
            ri_df = pd.merge(ri_df, ri_r, how='outer', on='IOCode')
        ri_df = ri_df.iloc[:-3]
    ri_df = ri_df.set_index('IOCode')

    return ri_df


def calc_tiva_coefficients(t_df):
    '''
    Calculate the fractional contributions, by TiVA region, to total imports
    by BEA-summary sector. Resulting dataframe is long format. 
    '''
    corr = (pd.read_csv(conPath / 'bea_imports_corr.csv',
                        usecols=['BEA Imports', 'BEA Summary'])
            .drop_duplicates())
    # ^^ requires mapping of import codes to summary codes. These codes are 
    # between detail and summary.

    t_c = (t_df
           .reset_index()
           .rename(columns={'IOCode': 'BEA Imports'})
           .merge(corr, on='BEA Imports', how='left', validate='one_to_many')
           .groupby('BEA Summary').agg('sum')
           )

    t_c = (t_c.div(t_c.sum(axis=1), axis=0).fillna(0)
              .reset_index())

    if not round(t_c.drop(columns='BEA Summary')
                    .sum(axis=1),5).isin([0,1]).all():
        print('WARNING: error calculating import shares.')

    t_c = t_c.melt(id_vars=['BEA Summary'], var_name='TiVA Region',
                   value_name='region_contributions_imports')

    return t_c


def download_and_store_mrio():
    '''
    If MRIO object not already present in directory, downloads MRIO object.
    '''
    file = dataPath / 'IOT_2022_pxp.zip'
    if not file.exists():
        exio3 = pymrio.download_exiobase3(storage_folder=dataPath,
                                          system='pxp',
                                          years=[2022])
    e = pymrio.parse_exiobase3(file)
    exio_m = e.impacts.M                                                   
    pkl.dump(exio_m, open(dataPath / 'exio3_multipliers.pkl', 'wb'))


def remove_exports(dataframe):
    '''Function filters data for positive (export) values and replaces them with 
    a value of 0.
    '''
    dataframe_values = dataframe._get_numeric_data()
    dataframe_values[dataframe_values>0] = 0
    return dataframe


def get_tiva_to_exio_concordance():
    '''
    Opens concordance dataframe of TiVA regions to exiobase countries.
    '''
    path = conPath / 'exio_tiva_concordance.csv'
    t_e = (pd.read_csv(path)
             .rename(columns={'ISO 3166-alpha-2': 'CountryCode'}))
    t_e = t_e[["TiVA Region","CountryCode"]]
    return t_e


def get_exio_to_useeio_concordance():
    '''
    Opens Exiobase to USEEIO binary concordance.
    Transforms wide-form Exiobase to USEEIO concordance into long form, 
    extracts all mappings to create new, two column concordance consisting of 
    USEEIO detail and mappings to Exiobase.
    '''
    path = conPath / "exio_to_bea_commodity_concordance.csv"
    e_u_b = (pd.read_csv(path, dtype=str)
               .rename(columns={'Unnamed: 0':'BEA Detail'}))
    e_u_b = e_u_b.iloc[:,:-4]
    e_u_l = pd.melt(e_u_b, id_vars=['BEA Detail'], var_name='Exiobase Sector')
    e_u = (e_u_l.query('value == "1"')
                .reset_index(drop=True))
    e_u = (e_u[['BEA Detail','Exiobase Sector']])
    return e_u


def get_detail_to_summary_useeio_concordance():
    '''
    Opens crosswalk between BEA (summary & detail) and USEEIO (with and 
    without waste disaggregation) sectors. USEEIO Detail with Waste Disagg 
    and corresponding summary-level codes. 
    '''
    path = conPath / 'useeio_internal_concordance.csv'
    u_cc = (pd.read_csv(path, dtype=str)
              .rename(columns={'BEA_Detail_Waste_Disagg': 'BEA Detail',
                               'BEA_Summary': 'BEA Summary'})
              )
    u_c = u_cc[['BEA Detail','BEA Summary']]
    u_c = u_c.drop_duplicates()
    return u_c


def get_subregion_imports(): #TO-DO: Reconstruct using census and BEA data
    '''
    Extracts industry output vector from exiobase pkl file.
    '''
    sr_i = get_imports_data(False)
    sr_i = sr_i[['BEA Sector','CountryCode','Import Quantity']]
    path = conPath / 'exio_tiva_concordance.csv'
    regions = (pd.read_csv(path, dtype=str, usecols=['ISO 3166-alpha-2',
                                                     'TiVA Region'])
                              .rename(columns={'ISO 3166-alpha-2': 'CountryCode'}))
    sr_i = (sr_i.merge(regions, on='CountryCode', how='left')
            .rename(columns={'BEA Sector':'BEA Detail'}))
    # sr_i['Subregion Contribution'] = sr_i['Import Quantity']/sr_i.groupby('BEA Sector')['Import Quantity'].transform('sum')
    # sr_i = sr_i.fillna(0).drop(columns={'Import Quantity'}).rename(columns={'BEA Sector':'BEA Detail'})
    return sr_i


def pull_exiobase_multipliers():
    '''
    Extracts multiplier matrix from stored Exiobase model.
    '''
    
    file = dataPath/'exio3_multipliers.pkl'
    if not file.exists():
        download_and_store_mrio()
    M_df = pkl.load(open(file,'rb'))

    fields = {**config['fields'], **config['flows']}

    M_df = M_df.loc[M_df.index.isin(fields.keys())]
    M_df = (M_df
            .transpose()
            .reset_index()
            .rename(columns=fields)
            # .melt(value_vars = [c for c in renamed_categories.values() if c not
            #                 in ['Exiobase Sector', 'Country']],
            #       id_vars = ['Exiobase Sector', 'Country'],
            #       var_name = 'Flow',
            #       value_name = 'EF')
            )
    return M_df


def calc_contribution_coefficients(p_d):
    '''
    Appends contribution coefficients to prepared dataframe.
    '''
    
    df = calc_coefficients_bea_summary(p_d)
    df = calc_coefficients_bea_detail(df)

    df = df[['TiVA Region','CountryCode','BEA Summary','BEA Detail',
             'Subregion Contribution to Summary',
             'Subregion Contribution to Detail']]
    return df


def calc_coefficients_bea_summary(df):
    '''
    Calculate the fractional contributions, by sector, of each Exiobase 
    country to the TiVA region they are assigned. This creates 2 new columns:
    1) 'TiVA_indout_subtotal, where industry outputs are summed according to
    TiVA-sector pairings; 2) 'region_contributions_TiVA, where each 
    Exiobase country's industry outputs are divided by their corresponding
    TiVA_indout_subtotals to create the fractional contribution coefficients.
    '''
    
    df['Subregion Contribution to Summary'] = (df['Import Quantity']/
                                               df.groupby(['TiVA Region',
                                                           'BEA Summary'])
                                               ['Import Quantity']
                                               .transform('sum'))
    return df


def calc_coefficients_bea_detail(df):
    '''
    Calculate the fractional contributions, by sector, of each Exiobase 
    country to their corresponding USEEIO summary-level sector(s). These
    concordances were based on Exiobase sector --> USEEIO Detail-level 
    sector, and USEEIO detail-level sector --> USEEIO summary-level sector
    mappins. The function creates 2 new columns: 1) 'USEEIO_indout_subtotal, 
    where industry outputs are summed according to
    TiVA-Exiobase sector-USEEIO summary sector combinations; 
    2) 'regional_contributions_USEEIO, where each 
    Exiobase country's industry outputs are divided by their corresponding
    USEEIO_indout_subtotals to create the fractional contribution 
    coefficients to each USEEIO category. 
    '''
    
    df['Subregion Contribution to Detail'] = (df['Import Quantity']/
                                              df.groupby(['TiVA Region',
                                                          'BEA Detail'])
                                              ['Import Quantity']
                                              .transform('sum'))
    return df


def calculate_specific_emission_factors(multiplier_df):
    '''
    Calculates TiVA-exiobase sector and TiVA-bea summary sector emission
    multipliers.
    '''
    
    multiplier_df = (multiplier_df
                     .assign(Weighted_BEA_Detail = (multiplier_df['EF'] *
                             multiplier_df['Subregion Contribution to Detail']))
                     .assign(Weighted_BEA_Summary = (multiplier_df['EF'] *
                             multiplier_df['Subregion Contribution to Summary']))
                     )

    weighted_multipliers_bea_detail = (multiplier_df
        .groupby(['TiVA Region','BEA Detail', 'Flow'])
        .agg({'Weighted_BEA_Detail': 'sum'}).reset_index())
    weighted_multipliers_bea_summary = (multiplier_df
        .groupby(['TiVA Region','BEA Summary', 'Flow'])
        .agg({'Weighted_BEA_Summary': 'sum'}).reset_index())
    return(weighted_multipliers_bea_detail, weighted_multipliers_bea_summary)


def calculate_emission_factors(multiplier_df):
    '''
    Merges emission multipliers on country and exiobase sector. Each gas 
    multiplier is multiplied by both the TiVA and USEEIO contribution 
    coefficients to produce multipliers for each Exiobase country-sector 
    and gas combination. These are stored in new 'Weighted (insert 
    multiplier category)' columns. Subsequently, unnecessary columns, such as 
    unweighted gas multipliers and used contribution factors, are dropped 
    from the dataframe. Other than weighted burden columns, the output 
    dataframe also continues to include 'TiVA Region', 'Exiobase Sector', 
    and 'USEEIO Summary'.
    '''
    
    multiplier_df = (multiplier_df
                     .assign(Weighted_TiVA_BEA = (multiplier_df['EF'] *
                             multiplier_df['Subregion Contribution to Detail'] *
                             multiplier_df['Subregion Contribution to Summary'])
                             )
                     )

    weighted_multipliers_exiobase = (multiplier_df
        .groupby(['TiVA Region','BEA Summary', 'Flow'])
        .agg({'Weighted_TiVA_BEA': 'sum'}).reset_index()
        )
    return weighted_multipliers_exiobase


def calculateWeightedEFsImportsData(weighted_multipliers,
                                    import_contribution_coeffs):
    '''
    Merges import contribution coefficients with weighted exiobase 
    multiplier dataframe. Import coefficients are then multiplied by the 
    weighted exiobase multipliers to produce weighted multipliers that 
    incorporate imports data. These are stored in new 'Weighted-Imports 
    (insert multiplier category)' columns. Subsequently, unnecessary columns, 
    such as unweighted Exiobase multipliers and used contribution factors, 
    are dropped from the dataframe. Other than weighted burden columns, the 
    output dataframe only continues to include 'USEEIO Summary' codes.
    '''
    print("")
    weighted_df_imports = (
        weighted_multipliers
        .merge(import_contribution_coeffs, how='left', validate='m:1',
               on=['TiVA Region','BEA Summary'])
        .assign(region_contributions_imports=lambda x:
                x['region_contributions_imports'].fillna(0))
            )

    weighted_df_imports = (
        weighted_df_imports.assign(Weighted_Import_EF=lambda x:
                                   x['Weighted_TiVA_BEA'] * 
                                   x['region_contributions_imports'])
        )

    imports_multipliers = (
        weighted_df_imports
        .groupby(['BEA Summary', 'Flow'])
        .agg({'Weighted_Import_EF': 'sum'})
        .reset_index()
        )

    return imports_multipliers


#%%
if __name__ == '__main__':
    (prepared_dataframe, imports_multipliers, weighted_multipliers_bea_detail, 
            weighted_multipliers_bea_summary,weighted_multipliers_all) = run_script()

    imports_multipliers.to_csv('imports_multipliers.csv', index=False)
    # weighted_multipliers_bea.to_csv('weighted_multipliers_bea.csv', index=False)
    # weighted_multipliers_exio.to_csv('weighted_multipliers_exio.csv', index=False)
