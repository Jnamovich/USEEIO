import pandas as pd
import pymrio
import pickle as pkl
import yaml
import statistics
from currency_converter import CurrencyConverter
from datetime import date
from pathlib import Path
from API_Imports_Data_Script import get_imports_data
from esupy.dqi import get_weighted_average
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

flow_cols = ('Flow', 'Compartment', 'Unit',
             'CurrencyYear', 'EmissionYear', 'PriceType',
             'Flowable', 'Context', 'FlowUUID')

#%%

with open(dataPath.parent / "Data" / "exio_config.yml", "r") as file:
    config = yaml.safe_load(file)


def run_script(io_level='Summary', year=2021):
    '''
    Runs through script to produce emission factors for U.S. imports.
    '''
    # Country imports by detail sector
    sr_i = get_subregion_imports(year)
    if len(sr_i.query('`Import Quantity` <0')) > 0:
        print('WARNING: negative import values...')

    if io_level == 'Summary':
        u_c = get_detail_to_summary_useeio_concordance()
        sr_i = (sr_i.merge(u_c, how='left', on='BEA Detail', validate='m:1'))

    else: # Detail
        print('ERROR: not yet implemented')
        sr_i = sr_i.rename(columns={'BEA Detail': 'BEA'})

    p_d = sr_i.copy()
    p_d = p_d[['TiVA Region', 'CountryCode', 'BEA Summary',
               'BEA Detail', 'Import Quantity']]
    c_d = calc_contribution_coefficients(p_d)

    if sum(c_d.duplicated(['CountryCode', 'BEA Detail'])) > 0:
        print('Error calculating country coefficients by detail sector')

    e_u = get_exio_to_useeio_concordance()
    e_d = pull_exiobase_multipliers(year)
    e_out = pull_exiobase_output(year)
    check = e_d.query('`Carbon dioxide` >= 100')
    e_d = e_d.query('`Carbon dioxide` < 100') # Drop Outliers
    ## TODO consider an alternate approach here

    e_d = (e_d.merge(e_out, how='left')
              .merge(e_u, on='Exiobase Sector', how='left')
              )
    agg = e_d.groupby(['BEA Detail', 'CountryCode']).agg('sum')
    for c in [c for c in agg.columns if c not in ['indout']]:
        agg[c] = get_weighted_average(e_d, c, 'indout', ['BEA Detail', 'CountryCode'])

    multiplier_df = c_d.merge(agg.reset_index().drop(columns='indout'),
                              how='left',
                              on=['CountryCode', 'BEA Detail'])
    multiplier_df = multiplier_df.melt(
        id_vars = [c for c in multiplier_df if c not in 
                   config['flows'].values()],
        var_name = 'Flow',
        value_name = 'EF')

    multiplier_df = (
        multiplier_df
        .assign(Compartment='emission/air')
        .assign(Unit='kg')
        .assign(ReferenceCurrency='Euro')
        .assign(CurrencyYear=str(year))
        .assign(EmissionYear='2019' if year > 2019 else str(year))
        # ^^ GHG data stops at 2019
        .assign(PriceType='Basic')
        )

    import fedelemflowlist as fedelem
    fl = (fedelem.get_flows()
          .query('Flowable in @multiplier_df.Flow')
          .filter(['Flowable', 'Context', 'Flow UUID'])
          )
    multiplier_df = (
        multiplier_df
        .merge(fl, how='left',
               left_on=['Flow', 'Compartment'],
               right_on=['Flowable', 'Context'],
               )
        .drop(columns=['Flow', 'Compartment'])
        .rename(columns={'Flow UUID': 'FlowUUID'})
        )

    weighted_multipliers_bea_detail, weighted_multipliers_bea_summary = (
        calculate_specific_emission_factors(multiplier_df))

    # Aggregate by TiVa Region
    t_c = calc_tiva_coefficients(year)
    imports_multipliers = (calculateWeightedEFsImportsData(
        weighted_multipliers_bea_summary, t_c))

    # Currency adjustment
    c = CurrencyConverter(fallback_on_missing_rate=True)
    exch = statistics.mean([c.convert(1, 'EUR', 'USD', date=date(year, 1, 1)),
                            c.convert(1, 'EUR', 'USD', date=date(year, 12, 30))])
    imports_multipliers = (
        imports_multipliers
        .assign(FlowAmount=lambda x: x['Amount']/exch)
        .drop(columns='Amount')
        .rename(columns={'BEA Summary': 'Sector'})
        .assign(Unit='kg')
        .assign(ReferenceCurrency='USD')
        .assign(BaseIOLevel='Summary')
        )

    return (sr_i, imports_multipliers, weighted_multipliers_bea_detail, 
            weighted_multipliers_bea_summary)


def get_tiva_data(year):
    '''
    Iteratively pulls BEA imports data matricies from stored csv file,
    extracts the Total Imports columns by region, and consolidates 
    into one dataframe. 
    
    https://apps.bea.gov/iTable/?reqid=157&step=1
    '''

    f_n = f'Import Matrix, __region__, After Redefinitions_{year}.csv'
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
        df = df.reset_index()
        ri_r = df[['IOCode', abbv]]
        if ri_df.empty:
            ri_df = ri_r
        else:
            ri_df = pd.merge(ri_df, ri_r, how='outer', on='IOCode')
        ri_df = ri_df.iloc[:-3]
    ri_df = ri_df.set_index('IOCode')

    return ri_df


def calc_tiva_coefficients(year):
    '''
    Calculate the fractional contributions, by TiVA region, to total imports
    by BEA-summary sector. Resulting dataframe is long format. 
    '''
    t_df = get_tiva_data(year)
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


def download_and_store_mrio(year):
    '''
    If MRIO object not already present in directory, downloads MRIO object.
    '''
    file = dataPath / f'IOT_{year}_pxp.zip'
    if not file.exists():
        exio3 = pymrio.download_exiobase3(storage_folder=dataPath,
                                          system='pxp',
                                          years=[year])
    e = pymrio.parse_exiobase3(file)
    exio = {}
    exio['M'] = e.impacts.M
    exio['x'] = e.x
    pkl.dump(exio, open(dataPath / f'exio3_multipliers_{year}.pkl', 'wb'))


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
    modified slightly from: https://ntnu.app.box.com/v/EXIOBASEconcordances/file/983477211189
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


def get_subregion_imports(year):
    '''
    Generates dataset of imports by country by sector from BEA and Census
    '''
    sr_i = get_imports_data(False, data_years=[year])
    path = conPath / 'exio_tiva_concordance.csv'
    regions = (pd.read_csv(path, dtype=str,
                           usecols=['ISO 3166-alpha-2', 'TiVA Region'])
               .rename(columns={'ISO 3166-alpha-2': 'CountryCode'})
               )
    sr_i = (sr_i.merge(regions, on='CountryCode', how='left', validate='m:1')
                .rename(columns={'BEA Sector':'BEA Detail'}))
    # sr_i['Subregion Contribution'] = sr_i['Import Quantity']/sr_i.groupby('BEA Sector')['Import Quantity'].transform('sum')
    # sr_i = sr_i.fillna(0).drop(columns={'Import Quantity'}).rename(columns={'BEA Sector':'BEA Detail'})
    return sr_i


def pull_exiobase_multipliers(year):
    '''
    Extracts multiplier matrix from stored Exiobase model.
    '''
    file = dataPath/f'exio3_multipliers_{year}.pkl'
    if not file.exists():
        download_and_store_mrio(year)
    exio = pkl.load(open(file,'rb'))
    M_df = exio['M']

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


def pull_exiobase_output(year):
    '''
    Extracts industry output vector from stored Exiobase model.
    '''
    file = dataPath/f'exio3_multipliers_{year}.pkl'
    if not file.exists():
        download_and_store_mrio(year)
    fields = {**config['fields'], **config['flows']}
    exio = pkl.load(open(file,'rb'))
    x_df = (exio['x']
            .reset_index()
            .rename(columns=fields)
            )
    return x_df


def calc_contribution_coefficients(p_d):
    '''
    Appends contribution coefficients to prepared dataframe.
    '''
    
    df = calc_coefficients_bea_summary(p_d)
    df = calc_coefficients_bea_detail(df)

    df = df[['TiVA Region','CountryCode','BEA Summary','BEA Detail',
             'Subregion Contribution to Summary',
             'Subregion Contribution to Detail']]
    if not(df['Subregion Contribution to Summary'].fillna(0).between(0,1).all() &
           df['Subregion Contribution to Detail'].fillna(0).between(0,1).all()):
        print('ERROR: Check contribution values outside of [0-1]')
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
                     .assign(Amount = (multiplier_df['EF'] *
                             multiplier_df['Subregion Contribution to Detail']))
                     .assign(Amount = (multiplier_df['EF'] *
                             multiplier_df['Subregion Contribution to Summary']))
                     )

    col = [c for c in multiplier_df if c in flow_cols]

    weighted_multipliers_bea_detail = (multiplier_df
        .groupby(['TiVA Region','BEA Detail'] + col)
        .agg({'Amount': 'sum'}).reset_index())
    weighted_multipliers_bea_summary = (multiplier_df
        .groupby(['TiVA Region','BEA Summary'] + col)
        .agg({'Amount': 'sum'}).reset_index())
    return(weighted_multipliers_bea_detail, weighted_multipliers_bea_summary)


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
    weighted_df_imports = (
        weighted_multipliers
        .merge(import_contribution_coeffs, how='left', validate='m:1',
               on=['TiVA Region','BEA Summary'])
        .assign(region_contributions_imports=lambda x:
                x['region_contributions_imports'].fillna(0))
            )

    weighted_df_imports = (
        weighted_df_imports.assign(Amount=lambda x:
                                   x['Amount'] *
                                   x['region_contributions_imports'])
        )

    col = [c for c in weighted_df_imports if c in flow_cols]

    imports_multipliers = (
        weighted_df_imports
        .groupby(['BEA Summary'] + col)
        .agg({'Amount': 'sum'})
        .reset_index()
        )

    return imports_multipliers


#%%
if __name__ == '__main__':
    (import_totals, imports_multipliers, weighted_multipliers_bea_detail, 
            weighted_multipliers_bea_summary) = run_script(year=2021)

    imports_multipliers.to_csv('imports_multipliers.csv', index=False)
