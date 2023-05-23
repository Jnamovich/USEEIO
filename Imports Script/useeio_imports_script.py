import pandas as pd
import pymrio
import pickle as pkl
import yaml
from pathlib import Path

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
dataPath = Path(__file__).parent / 'Data'
conPath = Path(__file__).parent / 'Concordances'

#%%

def run_script(io_level='Summary'):
    '''
    Runs through script to produce emission factors for U.S. imports.
    '''
    
    t_df = get_tiva_data()

    t_c = calc_tiva_coefficients(t_df)

    t_e = get_tiva_to_exio_concordance()
    e_u = get_exio_to_useeio_concordance()
    r_i = get_subregion_imports()
    # TODO ^^ Substitute with BEA and Census trade data

    t_r_i = (r_i.merge(t_e, on='region', how='left', validate='m:1')
                .rename(columns={'region':'Country',
                                 'sector':'Exiobase Sector'}))

    if io_level == 'Summary':
        u_c = get_detail_to_summary_useeio_concordance()
        e_u = (e_u.merge(u_c, how='left', on='BEA Detail', validate='m:1')
                  .drop(columns=['BEA Detail'])
                  .rename(columns={'BEA Summary': 'BEA'})
                  .drop_duplicates()
                  )
        t_c = t_c.rename(columns={'BEA Summary': 'BEA'})
    else: # Detail
        print('ERROR: not yet implemented')
        e_u = e_u.rename(columns={'BEA Detail': 'BEA'})
        ## TODO adjust t_c


    t_r_i_u = t_r_i.merge(e_u, on='Exiobase Sector', how='left')

    p_d = t_r_i_u.copy()
    # TODO WARNING ^^ this is creating some duplicates where an exiobase sector
    # maps to multiple detail sectors but still a single summary sector

    c_d = calc_contribution_coefficients(p_d)
    e_d = pull_exiobase_multipliers()
    multiplier_df = c_d.merge(e_d, how='left',
                              on=['Country', 'Exiobase Sector'])

    flows = ['Carbon Dioxide (CO2)',
             'Methane (CH4)',
             'Nitrous Oxide (N2O)']
    #TODO ^^ replace with names in metadata file

    multiplier_df = multiplier_df.melt(
        id_vars = [c for c in multiplier_df if c not in flows],
        var_name = 'Flow',
        value_name = 'EF')

    weighted_multipliers_bea, weighted_multipliers_exio = (
        calculate_specific_emission_factors(multiplier_df))
    weighted_multipliers_exiobase = (
        calculate_emission_factors(multiplier_df))
    imports_multipliers = (calculateWeightedEFsImportsData(
        weighted_multipliers_exiobase, t_c))

    imports_multipliers = (
        imports_multipliers
        .rename(columns={'Weighted_Import_EF': 'Amount'})
        .assign(Unit='kg / Euro')
        .assign(CurrencyYear='2021')
        .assign(DataYear='2021') # Emissions year
        .assign(PriceType='Producer')
        )
    
    #TODO Currency adjustment
    
    #TODO Price adjustment
    
    return (p_d, imports_multipliers, weighted_multipliers_bea, 
            weighted_multipliers_exio)


# TODO update this to use the API to obtain updated import data
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
    exio_indout = e.x                                                       
    pkl.dump(exio_indout, open(dataPath / 'exio3_indout.pkl', 'wb'))
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
             .rename(columns={'ISO 3166-alpha-2': 'region'}))
    t_e = t_e[["TiVA Region","region"]]
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
    file = dataPath/'exio3_indout.pkl'
    if not file.exists():
        download_and_store_mrio()

    exio_indout = pkl.load(open(dataPath/'exio3_indout.pkl','rb'))
    # exio_indout = (exio_indout.rename(columns={'region':'TiVA Region'})
    #                .reset_index())
    return exio_indout.reset_index()


def pull_exiobase_multipliers():
    # Extracts multiplier matrix from stored Exiobase model.
    
    with open(dataPath.parent / "Test" / "multipliers_renaming.yml", "r") as file:
        renamed_categories = yaml.safe_load(file)

    file = dataPath/'exio3_multipliers.pkl'
    if not file.exists():
        download_and_store_mrio()
    M_df = pkl.load(open(file,'rb'))

    M_df = M_df.loc[M_df.index.isin(renamed_categories.keys())]
    M_df = (M_df
            .transpose()
            .reset_index()
            .rename(columns=renamed_categories)
            # .melt(value_vars = [c for c in renamed_categories.values() if c not
            #                 in ['Exiobase Sector', 'Country']],
            #       id_vars = ['Exiobase Sector', 'Country'],
            #       var_name = 'Flow',
            #       value_name = 'EF')
            )

    return M_df


def calc_contribution_coefficients(p_d):
    # Appends contribution coefficients to prepared dataframe.
    
    df = calc_coefficients_tiva(p_d)
    df = calc_coefficients_useeio(df)

    df = df[['TiVA Region','Country','Exiobase Sector','BEA',
             'TiVA_indout_subtotal','BEA_indout_subtotal',
             'region_contributions_TiVA','region_contributions_BEA']]
    return df


def calc_coefficients_tiva(df):
    # Calculate the fractional contributions, by sector, of each Exiobase 
    # country to the TiVA region they are assigned. This creates 2 new columns:
    # 1) 'TiVA_indout_subtotal, where industry outputs are summed according to
    #TiVA-sector pairings; 2) 'region_contributions_TiVA, where each 
    # Exiobase country's industry outputs are divided by their corresponding
    # TiVA_indout_subtotals to create the fractional contribution coefficients.

    df = df.assign(TiVA_indout_subtotal =
                   df.groupby(['TiVA Region', 'Exiobase Sector'])
                   ['indout'].transform('sum'))
    df = df.assign(region_contributions_TiVA =
                   df['indout']/df['TiVA_indout_subtotal'])
    return df


def calc_coefficients_useeio(df):
    # Calculate the fractional contributions, by sector, of each Exiobase 
    # country to their corresponding USEEIO summary-level sector(s). These
    # concordances were based on Exiobase sector --> USEEIO Detail-level 
    # sector, and USEEIO detail-level sector --> USEEIO summary-level sector
    # mappins. The function creates 2 new columns: 1) 'USEEIO_indout_subtotal, 
    # where industry outputs are summed according to
    # TiVA-Exiobase sector-USEEIO summary sector combinations; 
    # 2) 'regional_contributions_USEEIO, where each 
    # Exiobase country's industry outputs are divided by their corresponding
    # USEEIO_indout_subtotals to create the fractional contribution 
    # coefficients to each USEEIO category. 
    
    df = df.assign(BEA_indout_subtotal =
                   df.groupby(['TiVA Region', 'BEA'])
                   ['indout'].transform('sum'))
    df = df.assign(region_contributions_BEA =
                   df['indout']/df['BEA_indout_subtotal'])

    return df


def calculate_specific_emission_factors(multiplier_df):
    # Calculates TiVA-exiobase sector and TiVA-bea summary sector emission
    # multipliers.
    
    multiplier_df = (multiplier_df
                     .assign(Weighted_exio = (multiplier_df['EF'] *
                             multiplier_df['region_contributions_TiVA']))
                     .assign(Weighted_BEA = (multiplier_df['EF'] *
                             multiplier_df['region_contributions_BEA']))
                     )

    weighted_multipliers_bea = (multiplier_df
        .groupby(['TiVA Region','BEA', 'Flow'])
        .agg({'Weighted_BEA': 'sum'}).reset_index())
    weighted_multipliers_exio = (multiplier_df
        .groupby(['TiVA Region','Exiobase Sector', 'Flow'])
        .agg({'Weighted_exio': 'sum'}).reset_index())
    return(weighted_multipliers_bea, weighted_multipliers_exio)


def calculate_emission_factors(multiplier_df):
    # Merges emission multipliers on country and exiobase sector. Each gas 
    # multiplier is multiplied by both the TiVA and USEEIO contribution 
    # coefficients to produce multipliers for each Exiobase country-sector 
    # and gas combination. These are stored in new 'Weighted (insert 
    # multiplier category)' columns. Subsequently, unnecessary columns, such as 
    # unweighted gas multipliers and used contribution factors, are dropped 
    # from the dataframe. Other than weighted burden columns, the output 
    # dataframe also continues to include 'TiVA Region', 'Exiobase Sector', 
    # and 'USEEIO Summary'.
 
    multiplier_df = (multiplier_df
                     .assign(Weighted_TiVA_BEA = (multiplier_df['EF'] *
                             multiplier_df['region_contributions_TiVA'] *
                             multiplier_df['region_contributions_BEA'])
                             )
                     )

    weighted_multipliers_exiobase = (multiplier_df
        .groupby(['TiVA Region','Exiobase Sector','BEA', 'Flow'])
        .agg({'Weighted_TiVA_BEA': 'sum'}).reset_index()
        )
    return weighted_multipliers_exiobase


def calculateWeightedEFsImportsData(weighted_multipliers_exiobase,
                                    import_contribution_coeffs):
    # Merges import contribution coefficients with weighted exiobase 
    # multiplier dataframe. Import coefficients are then multiplied by the 
    # weighted exiobase multipliers to produce weighted multipliers that 
    # incorporate imports data. These are stored in new 'Weighted-Imports 
    # (insert multiplier category)' columns. Subsequently, unnecessary columns, 
    # such as unweighted Exiobase multipliers and used contribution factors, 
    # are dropped from the dataframe. Other than weighted burden columns, the 
    # output dataframe only continues to include 'USEEIO Summary' codes.
    
    weighted_df_imports = (
        weighted_multipliers_exiobase
        .merge(import_contribution_coeffs, how='left', validate='m:1',
               on=['TiVA Region','BEA'])
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
        .groupby(['BEA', 'Flow'])
        .agg({'Weighted_Import_EF': 'sum'})
        .reset_index()
        )

    return imports_multipliers

#%%
if __name__ == '__main__':
    (prepared_dataframe, imports_multipliers,
     weighted_multipliers_bea, weighted_multipliers_exio) = run_script()

    imports_multipliers.to_csv('imports_multipliers.csv', index=False)
    # weighted_multipliers_bea.to_csv('weighted_multipliers_bea.csv', index=False)
    # weighted_multipliers_exio.to_csv('weighted_multipliers_exio.csv', index=False)
