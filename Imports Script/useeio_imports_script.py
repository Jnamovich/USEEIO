import pandas as pd
import pymrio
import pickle as pkl
from pathlib import Path

dataPath = Path(__file__).parent


def run_script():
    #Runs through script to produce emission factors for U.S. imports.
    
    import_contribution_coeffs = pull_and_process_imports_data() 
    # download_and_store_mrio() #Toggle if pkl file is not in working directory
    tiva_to_exio = open_tiva_region_concordance()
    exio3_to_useeio_binary = exiobase_to_useeio_concordance()
    exio3_to_useeio_concordance = (
        process_exio_to_useeio_concordance(exio3_to_useeio_binary)
        )
    useeio_detail_to_summary = pull_and_subset_useeio_crosswalk()
    exio_indout, exiobase_pkl = pull_exiobase_industry_output_vector()
    exiobase_emissions_multipliers_df = pull_exiobase_multipliers(exiobase_pkl)
    prepared_dataframe = prepare_for_calculations(tiva_to_exio,exio_indout,
                                                  exio3_to_useeio_concordance,
                                                  useeio_detail_to_summary)
    clean_coefficient_dataframe = (
        calculate_contribution_coefficients(prepared_dataframe)
        )
    
    
    weighted_multipliers_exiobase = (
        calculate_emission_factors(clean_coefficient_dataframe, 
                                    exiobase_emissions_multipliers_df)
        )
    imports_multipliers = (
        calculateWeightedEFsImportsData(weighted_multipliers_exiobase,
                                        import_contribution_coeffs)
        )
    return(imports_multipliers)
    
    
def pull_and_process_imports_data():
    # Iteratively pulls BEA imports data matricies from source URL, extracts 
    # the BEA NAICS and Total Imports columns, and consolidates all imports
    # stats into one dataframe. 
    
    imports_data_year = '2020'
    imports_data_url_stem = ('https://www.bea.gov/system/files/2021-12/Import'
                             '%20Matrix%20')
    bea_to_tiva_dict = {'ROW':'ROW','Canada':'CA','Mexico':'MX','China':'CN',
                        'Europe':'EU'} # key: Imports Region, value: TiVA Region 
    rows_to_skip=[0,1,2,3,4,5,6,8] # rows within the data sheets to omit
    regional_imports_df = pd.DataFrame() # empty dataframe to replace/populate
    for region in bea_to_tiva_dict:
        partner_url = (imports_data_url_stem + region +'.xlsx')
        partnerDF = pd.read_excel(partner_url,sheet_name=imports_data_year, 
                    skiprows=rows_to_skip,index_col=0).rename(
                    columns={'Unnamed: 0':'Industry/Commodity Code:',
                             'F050':bea_to_tiva_dict[region]})
        extracted_imports_column = partnerDF[bea_to_tiva_dict[region]]
        if regional_imports_df.empty == True:
            # dataframe to populate doesn't exist, becomes dataframe
            regional_imports_df = extracted_imports_column
        else:
            # dataframe exists, new columns added
            regional_imports_df = pd.concat([regional_imports_df, 
                                             extracted_imports_column], axis=1
                                            )                                   #VERIFY CONCAT MERGES ON INDEX
    regional_imports_df = remove_exports(regional_imports_df)
    import_contribution_coeffs = calculate_contribution_coefficients_imports(
        regional_imports_df)
    return(import_contribution_coeffs)


def download_and_store_mrio():
    # If MRIO object not already present in directory, downloads MRIO object.
    
    exio3 = pymrio.parse_exiobase3('IOT_2022_pxp.zip')
    pkl.dump(exio3,open(dataPath/'exio3.pkl', 'wb'))
    exiobase_pkl = pkl.load(open(dataPath/'exio3.pkl','rb'))
    return(exiobase_pkl)


def remove_exports(dataframe):
    # Function filters data for positive (export) values and replaces them with 
    # a value of 0.
    
    dataframe_values = dataframe._get_numeric_data()
    dataframe_values[dataframe_values>0] = 0
    return(dataframe)


def open_tiva_region_concordance():
    # Opens concordance dataframe of TiVA regions to exiobase countries.
    
    tiva_to_exio = (pd.read_csv('exio_tiva_concordance.csv')
        .rename(columns={'ISO 3166-alpha-2':'region'}))
    tiva_to_exio = tiva_to_exio[["TiVA Region","region"]]
    return(tiva_to_exio)


def exiobase_to_useeio_concordance():
    # Opens Exiobase to USEEIO binary concordance.
    
    exio3_to_useeio_binary = pd.read_csv(
        "useeio_exio_concordance_waste_disagg.csv",dtype=str)
    exio3_to_useeio_binary.rename(columns ={'Unnamed: 0':'USEEIO Detail'},
                                inplace=True)
    return(exio3_to_useeio_binary)


def process_exio_to_useeio_concordance(exio3_to_useeio_binary):
    # Transforms wide-form Exiobase to USEEIO concordance into long form, 
    # extracts all mappings to create new, two column concordance consisting of 
    # USEEIO detail and mappings to Exiobase.
    
    exio3_to_useeio_binary = exio3_to_useeio_binary.iloc[:,:-4]
    exio3_to_useeio_long = pd.melt(exio3_to_useeio_binary, 
                                   id_vars=['USEEIO Detail'])
    exio3_to_useeio_concordance = (exio3_to_useeio_long
                                   .loc[exio3_to_useeio_long['value'] == '1']
                                   .rename(columns={'variable':
                                                    'Exiobase Sector'}))
    exio3_to_useeio_concordance = (
        exio3_to_useeio_concordance[['USEEIO Detail','Exiobase Sector']]
        )
    return(exio3_to_useeio_concordance)


def pull_and_subset_useeio_crosswalk():
    # Opens crosswalk between BEA (summary & detail) and USEEIO (with and 
    # without waste disaggregation) sectors. USEEIO Detail with Waste Disagg 
    # and corresponding summary-level codes. 
    
    naics_bea_useeio_concordance = pd.read_csv(
        'useeio_internal_concordance.csv', dtype=str)
    naics_bea_useeio_concordance.rename(
        columns={'BEA_Detail_Waste_Disagg':'USEEIO Detail',
                 'BEA_Summary':'USEEIO Summary'},inplace=True)
    useeio_detail_to_summary = naics_bea_useeio_concordance[['USEEIO Detail',
                                                             'USEEIO Summary']]
    useeio_detail_to_summary = useeio_detail_to_summary.drop_duplicates()
    return(useeio_detail_to_summary)


def pull_exiobase_industry_output_vector():
    # Extracts industry output vector from exiobase pkl file.
    
    exiobase_pkl = pkl.load(open(dataPath/'exio3.pkl','rb'))
    exio_indout = exiobase_pkl.x
    exio_indout = (exio_indout.rename(columns={'region':'TiVA Region'})
                   .reset_index())
    return(exio_indout, exiobase_pkl)


def pull_exiobase_multipliers(exiobase_pkl):
    # Extracts multiplier matrix from stored Exiobase model.
    
    exiobase_multipliers_df = exiobase_pkl.impacts.M
    exiobase_emissions_multipliers_df = exiobase_multipliers_df[37:40]
    exiobase_emissions_multipliers_df = (exiobase_emissions_multipliers_df
        .transpose().reset_index()
        .rename(columns={'region':'Country','sector':'Exiobase Sector',
                        ('Carbon dioxide (CO2) IPCC categories 1 to 4 and 6 to'
                         ' 7 (excl land use, land use change and forestry)'):
                         'Carbon Dioxide (CO2)',('Methane (CH4) IPCC categori'
                         'es 1 to 4 and 6 to 7 (excl land use, land use change'
                         ' and forestry)'):'Methane (CH4)',('Nitrous Oxide (N'
                         '2O) IPCC categories 1 to 4 and 6 to 7 (excl land use'
                         ', land use change and forestry)'):'Nitrous Oxide (N'
                         '2O)'}))
    return(exiobase_emissions_multipliers_df)


def prepare_for_calculations(tiva_to_exio,exio_indout,
                             exio3_to_useeio_concordance,
                             useeio_detail_to_summary):
    # Combines TiVA to Exio concordance, Exio to USEEIO concordance, Exio 
    # industry outputs, and USEEIO detail to USEEIO summary concordance. 
    # Resultant dataframe has columns ordered TiVA Region, Exiobase Country, 
    # Exiobase commodity, USEEIO detail, USEEIO summary, industry output. 
    
    tiva_indout = (
        tiva_to_exio.merge(exio_indout, on='region', how='outer')
        .rename(columns={'region':'Country','sector':'Exiobase Sector'})
        )
    tiva_indout_useeio_detail = tiva_indout.merge(exio3_to_useeio_concordance, 
                                                  on='Exiobase Sector', 
                                                  how='left')
    tiva_indout_useeio_summary = (
        tiva_indout_useeio_detail.merge(useeio_detail_to_summary, 
                                        on='USEEIO Detail', how='left'))
    prepared_dataframe = (
        tiva_indout_useeio_summary[['TiVA Region','Country','Exiobase Sector',
                                    'USEEIO Detail','USEEIO Summary','indout']]
        )
    return(prepared_dataframe)


def calculate_contribution_coefficients(dataframe):
    # Appends contribution coefficients to prepared dataframe.
    
    get_tiva_coefficients = calculate_contribution_coefficients_tiva(dataframe)
    get_useeio_coefficients = (
        calculate_contribution_coefficients_useeio(get_tiva_coefficients)
        )
    cleaned_coefficients = clean_coefficient_dataframe(get_useeio_coefficients)
    return(cleaned_coefficients)


def calculate_contribution_coefficients_imports(dataframe):
    # Calculate the fractional contributions, by TiVA region used in BEA 
    # imports data, to total imports by USEEIO-summary sector. 
    
    import_contribution_coeffs = (dataframe.div(dataframe
                                                         .sum(axis=1),axis=0)
                                                         .fillna(0))
    import_contribution_coeffs = (import_contribution_coeffs
                                  .reset_index(level=0)
                                  .rename(columns={'index':'USEEIO Summary'}))
    import_contribution_coeffs = (import_contribution_coeffs
                                  .melt(id_vars=['USEEIO Summary'],
                                        var_name='TiVA Region',
                                        value_name=
                                        'region_contributions_imports'))
    return(import_contribution_coeffs)


def calculate_contribution_coefficients_tiva(dataframe):
    # Calculate the fractional contributions, by sector, of each Exiobase 
    # country to the TiVA region they are assigned. This creates 2 new columns:
    # 1) 'TiVA_indout_subtotal, where industry outputs are summed according to
    #TiVA-sector pairings; 2) 'regional_contributions_TiVA, where each 
    # Exiobase country's industry outputs are divided by their corresponding
    # TiVA_indout_subtotals to create the fractional contribution coefficients.

    dataframe['TiVA_indout_subtotal'] = (
        dataframe[['TiVA Region','Exiobase Sector','indout']]
        .groupby(['TiVA Region','Exiobase Sector']).transform('sum')
        )
    dataframe['region_contributions_TiVA'] = (
        dataframe['indout']/dataframe['TiVA_indout_subtotal'])
    return(dataframe)


def calculate_contribution_coefficients_useeio(dataframe):
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
    
    dataframe['USEEIO_indout_subtotal'] = (
        dataframe[['TiVA Region','USEEIO Summary','indout']]
        .groupby(['TiVA Region','USEEIO Summary']).transform('sum'))
    dataframe['region_contributions_USEEIO'] = (
        dataframe['indout']/dataframe[('USEEIO_indout_subtotal')])
    return(dataframe)


def clean_coefficient_dataframe(dataframe):
    # Removes unnecessary columns for final emission factor calculation
    dataframe = dataframe[['TiVA Region','Country','Exiobase Sector',
                                   'USEEIO Detail','USEEIO Summary',
                                   'region_contributions_TiVA',
                                   'region_contributions_USEEIO']]
    return(dataframe)


def calculate_emission_factors(dataframe, exiobase_emissions_multipliers_df):
    # Merges emission multipliers on country and exiobase sector. Each gas 
    # multiplier is multiplied by both the TiVA and USEEIO contribution 
    # coefficients to produce multipliers for each Exiobase country-sector 
    # and gas combination. These are stored in new 'Weighted (insert 
    # multiplier category)' columns. Subsequently, unnecessary columns, such as 
    # unweighted gas multipliers and used contribution factors, are dropped 
    # from the dataframe. Other than weighted burden columns, the output 
    # dataframe also continues to include 'TiVA Region', 'Exiobase Sector', 
    # and 'USEEIO Summary'.
    
    tiva_useeio_multiplier_df = (
        dataframe.merge(exiobase_emissions_multipliers_df,
                        how='left',
                        left_on=['Country','Exiobase Sector'],
                        right_on=['Country','Exiobase Sector'])
        )
    tiva_useeio_multiplier_df['(Weighted) Carbon Dioxide (CO2)'] = (
        tiva_useeio_multiplier_df['Carbon Dioxide (CO2)']
        *tiva_useeio_multiplier_df['region_contributions_TiVA']
        *tiva_useeio_multiplier_df['region_contributions_USEEIO']
        )
    tiva_useeio_multiplier_df['(Weighted) Methane (CH4)'] = (
        tiva_useeio_multiplier_df['Methane (CH4)']
        *tiva_useeio_multiplier_df['region_contributions_TiVA']
        *tiva_useeio_multiplier_df['region_contributions_USEEIO']
        )
    tiva_useeio_multiplier_df['(Weighted) Nitrous Oxide (N2O)'] = (
        tiva_useeio_multiplier_df['Nitrous Oxide (N2O)']
        *tiva_useeio_multiplier_df['region_contributions_TiVA']
        *tiva_useeio_multiplier_df['region_contributions_USEEIO']
        )
    tiva_useeio_multiplier_df = (tiva_useeio_multiplier_df
                         .drop(['Carbon Dioxide (CO2)','Methane (CH4)',
                                'Nitrous Oxide (N2O)',
                                'region_contributions_TiVA',
                                'region_contributions_USEEIO'], axis=1))
    weighted_multipliers_exiobase = (tiva_useeio_multiplier_df
        .groupby(['TiVA Region','Exiobase Sector','USEEIO Summary'])
        .agg({'(Weighted) Carbon Dioxide (CO2)': 'sum', 
              '(Weighted) Methane (CH4)': 'sum',
              '(Weighted) Nitrous Oxide (N2O)': 'sum'}).reset_index()
        )
    return(weighted_multipliers_exiobase)

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
    
    weighted_df_imports = pd.merge(weighted_multipliers_exiobase,
                                  import_contribution_coeffs, how='left',
                                  on=['TiVA Region','USEEIO Summary'])
    weighted_df_imports['region_contributions_imports'] = (
        weighted_df_imports['region_contributions_imports']
        .fillna(0)
        )
    weighted_df_imports['(Weighted-Imports) Carbon Dioxide (CO2)'] = (
        weighted_df_imports['(Weighted) Carbon Dioxide (CO2)']
        *weighted_df_imports['region_contributions_imports']
        )
    weighted_df_imports['(Weighted-Imports) Methane (CH4)'] = (
        weighted_df_imports['(Weighted) Methane (CH4)']
        *weighted_df_imports['region_contributions_imports']
        )
    weighted_df_imports['(Weighted-Imports) Nitrous Oxide (N2O)'] = (
        weighted_df_imports['(Weighted) Nitrous Oxide (N2O)']
        *weighted_df_imports['region_contributions_imports']
        )
    weighted_df_imports = (weighted_df_imports
                          .drop(['(Weighted) Carbon Dioxide (CO2)',
                                 '(Weighted) Methane (CH4)',
                                 '(Weighted) Nitrous Oxide (N2O)',
                                 'region_contributions_imports'], axis=1
                                ))
    imports_multipliers = (
        weighted_df_imports
        .groupby(['USEEIO Summary'])
        .agg({'(Weighted-Imports) Carbon Dioxide (CO2)': 'sum', 
              '(Weighted-Imports) Methane (CH4)': 'sum',
              '(Weighted-Imports) Nitrous Oxide (N2O)': 'sum'})
        .reset_index()
        )
    
    return(imports_multipliers)

imports_multipliers = run_script()