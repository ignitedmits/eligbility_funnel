import pandas as pd
import funnel 
import initdf


def cust_type(cust_num):
    if cust_num in range (1,3):
        return 'MBC'
    elif cust_num in range (3,10):
        return 'SME'
    elif cust_num in range (10,49):
        return 'Small Group'
    else:
        return 'Large Group'


def create_funnel(df):
    #for unit testing
    df.rename(columns ={'msn_count':'Count of MSN', 'Gain?':'New Sites'}, inplace = 'True')

    df['temp_count_col'] = df.groupby('cust1_cust_nm')['MPAN'].transform('count')

    df['Customer Type'] = df['temp_count_col'].apply(cust_type)

    df['Single/Multi Site'] = df['temp_count_col'].apply(lambda x: 'Single Site' if x == 1 else 'Multiple Site')

    #CT/WC
    mt_df = initdf.open_meter_type()
    mt_df.rename(columns={mt_df.columns[0]:'mtr_typ', mt_df.columns[1]:'CT/WC'}, inplace = True)
    df = pd.merge(df, mt_df, how='left', on=['mtr_typ', 'mtr_typ'])
    del mt_df

    #CT Ratio
    df['CT Ratio'] = df['CT/WC'].apply(lambda x: 'NA' if x =='WC' else 'Unknown')

    #Supply Type
    model_df = initdf.open_meter_model()
    model_df.rename(columns={model_df.columns[0]:'mtr_mod', model_df.columns[1]:'Supply Type'}, inplace = True)
    df = pd.merge(df, model_df, how='left', on=['mtr_mod', 'mtr_mod'])
    del model_df
    
    #Service Plan
    tariff_df = initdf.open_meter_tariff()
    tariff_df.rename(columns={tariff_df.columns[0]:'sp_descr', tariff_df.columns[1]:'Service Plan'}, inplace = True)
    df = pd.merge(df, tariff_df, how='left', on=['sp_descr', 'sp_descr'])
    del tariff_df

    #Rate Type
    rate_df = initdf.open_rate_type()
    rate_df.rename(columns={rate_df.columns[0]:'sp_descr', rate_df.columns[1]:'Rate Type'}, inplace = True)
    df = pd.merge(df, rate_df, how='left', on=['sp_descr', 'sp_descr'])
    del rate_df

    #Shortcode
    df['bill_pcode'] = df['bill_pcode'].fillna('NA')
    df['Shortcode'] = df['bill_pcode'].apply(lambda x: x.split(' ')[0])
    df.loc[df['bill_pcode']=='NA','Shortcode'] = 'NA'

    #Phone Number (Y/N)
    df['cust1_phon1_num'] = df['cust1_phon1_num'].fillna('NA')
    df['Phone Number (Y/N)'] = df['cust1_phon1_num'].apply(lambda x: 'N' if x == '' else 'Y')
    df.loc[df['cust1_phon1_num']=='NA','Phone Number (Y/N)'] = 'NA'

    #LandLine/Mobile    
    df['LandLine/Mobile'] = df['cust1_phon1_num'].apply(lambda x: 'Mobile' if x[:2] == '07' else 'Landline')
    df.loc[df['cust1_phon1_num']=='NA','LandLine/Mobile'] = 'NA'

    #Rate Type
    sic_df = initdf.open_sic_file()
    sic_df.rename(columns={sic_df.columns[0]:'SIC_CODE', sic_df.columns[1]:'Industry Sector'}, inplace = True)
    df = pd.merge(df, sic_df, how='left', on=['SIC_CODE', 'SIC_CODE'])
    del sic_df

    #Email Address
    df['E-Mail 1'] = df['E-Mail 1'].fillna('NA')
    df['Email Address (Y/N)'] = df['E-Mail 1'].apply(lambda x: 'N' if x == '' else 'Y')
    df.loc[df['E-Mail 1']=='NA','Email Address (Y/N)'] = 'NA'

    df.rename(columns = {'core_mpan':'MPAN','mpan_status':'New Sites', 'cust1_cust_nm':'Customer Name', 'id_crac':'CA Number', 'Current Capability': 'Current Meter', \
        'mtr_mod':'Meter Model', 'sp_descr':'Tariff Name', 'id_blfr1':'Billing Cycle', 'bill_addr':'Billing Address', 'bill_pcode':'Billing Address Postcode', \
            'site_addr1':'Address Line 1', 'site_addr2':'Address Line 2', 'site_addr3':'Address Line 3', 'site_addr4':'Address Line 4', 'site_pcode':'Postcode', \
                'cust1_phon1_num':'Phone Number Detail', 'id_proc':'P.C.', 'Count of MSN':'Meters per MPAN', 'MSN':'MSN',  \
                    'id_sstc':'SSC', 'CONTRACTSTARTDATE' :'Contract End Date', 'ESIQS_TLUPSTAFF_NAME': 'KAM', 'SIC_CODE':'SIC Code', 'mtr_inst_dt':'Meter Install Date', \
                        'E-Mail 1':'Email Address'},  inplace = True) 

    df['MPAN ID'] = df['MPAN'].apply(lambda x: str(x)[:2])

    new_df = df[[ \
        'MPAN','MPAN ID', 'New Sites', 'Customer Name', 'Customer Type',  'Single/Multi Site', 'CA Number', 'CT/WC', 'CT Ratio', 'Current Meter',  \
            'Supply Type', 'Service Plan', 'Tariff Name', 'Rate Type', 'Billing Cycle', \
                'Billing Address', 'Billing Address Postcode', 'Address Line 1', 'Address Line 2', 'Address Line 3', 'Address Line 4', 'Postcode', 'Shortcode', \
                    'Phone Number (Y/N)', 'Phone Number Detail', 'LandLine/Mobile',  'P.C.',\
                        'Meters per MPAN', 'MSN', 'Meter Model', 'SSC', 'Contract End Date',\
                            '1-4 Offer', 'P6 Migration Exclusion', 'On A Deployment List? (Y/N)', 'AMR/SMETS2 Deployment', 'Date Deployed',\
                                'Hard Refusal Received', 'Marketing Consent', 'MBC Status', 'Classification', 'WAN Region', 'WAN Signal',  \
                                'KAM', 'SIC Code', 'Industry Sector', 'Meter Install Date', \
                                    'Email Address', 'Email Address (Y/N)', 'Vulnerability Within Portfolio? (Y/N)']]
    
    return new_df

def merge_previous_funnel(df):
    
    prev_df = initdf.open_previous_funnel()

    prev_col = ['MPAN','1-4 Offer', 'P6 Migration Exclusion', 'On A Deployment List? (Y/N)', 'AMR/SMETS2 Deployment', 'Date Deployed',\
        'Hard Refusal Received', 'Marketing Consent', 'MBC Status', 'Classification', 'WAN Region', 'WAN Signal', 'Vulnerability Within Portfolio? (Y/N)']
    
    #1	MPAN
    #32	1-4 Offer
    #33	P6 Migration Exclusion
    #34	On A Deployment List? (Y/N)
    #35	AMR/SMETS2 Deployment
    #36	Date Deployed
    #37	Hard Refusal Received
    #38	Marketing Consent 
    #39	MBC Status
    #40	Classification
    #41	WAN Region
    #42	WAN Signal
    #49	Vulnerability Within Portfolio? (Y/N)

    #if column name varies
    #prev_df = prev_df[prev_df.columns[[0,32,33,34,35,36,37,38,39,40,41,42,49]]]

    prev_df = prev_df[prev_col]

    
    #for testing purpose only
    prev_df.rename(columns = {'MPAN':'core_mpan'}, inplace = True)
   
    df = pd.merge(df, prev_df, how='left', on=['core_mpan', 'core_mpan'])

    df.rename(columns = {'core_mpan':'MPAN'}, inplace = True)

    df = create_funnel(df)

    try:
        df.to_csv (r'temp/new.csv', index = None, header=True)
    except:
        pass

    del prev_df
    
    return df