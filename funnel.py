#from IPython.core.display import display, HTML
#display(HTML("<style>.container { width:100% !important; }</style>"))

__author__ = "Mithun Shokhwal"
__version__ = "<prototye>"
__maintainer__ = "MS75354"
__email__ = "mithun.shokhwal@sse.com"
__status__ = "Pilot"

import pandas as pd
import numpy as np

import compliance
import eligibility
import initdf
import time

from initdf import log


def rejected_d10():
    global df
    rejected_d10_frame = initdf.open_rejected_d10()

    mpan_col = rejected_d10_frame.columns[0]

    rejected_d10_frame.rename(columns = {mpan_col:'core_mpan'},  inplace = True) 

    rejected_d10_frame = rejected_d10_frame.drop(columns=rejected_d10_frame.columns[1:])

    rejected_d10_frame['Rejected_D10'] = 'Yes'
    rejected_d10_frame = rejected_d10_frame.drop_duplicates(['core_mpan'])

    df = pd.merge(df, rejected_d10_frame, how='left', on=['core_mpan', 'core_mpan'])
    df['Rejected_D10'] = df['Rejected_D10'].fillna('No')
    del rejected_d10_frame

def accepted_d10():
    global df
    accepted_d10_frame = initdf.open_accepted_d10()

    mpan_col = accepted_d10_frame.columns[0]

    accepted_d10_frame.rename(columns = {mpan_col:'core_mpan'},  inplace = True) 

    accepted_d10_frame = accepted_d10_frame.drop(columns=accepted_d10_frame.columns[1:])

    accepted_d10_frame['Accepted_D10'] = 'Yes'
    accepted_d10_frame = accepted_d10_frame.drop_duplicates(['core_mpan'])

    df = pd.merge(df, accepted_d10_frame, how='left', on=['core_mpan', 'core_mpan'])
    df['Accepted_D10'] = df['Accepted_D10'].fillna('No')
    del accepted_d10_frame
    
def accepted_d313():
    global df
    accepted_d313_frame = initdf.open_accepted_d313()

    mpan_col = accepted_d313_frame.columns[0]

    accepted_d313_frame.rename(columns = {mpan_col:'core_mpan'},  inplace = True) 

    accepted_d313_frame = accepted_d313_frame.drop(columns=accepted_d313_frame.columns[1:])

    accepted_d313_frame['Accepted_D313'] = 'Yes'
    accepted_d313_frame = accepted_d313_frame.drop_duplicates(['core_mpan'])

    df = pd.merge(df, accepted_d313_frame, how='left', on=['core_mpan', 'core_mpan'])
    df['Accepted_D313'] = df['Accepted_D313'].fillna('No')
    del accepted_d313_frame

def get_profile_data_frame():
    
    frame = initdf.get_profile_data()

    frame = frame.drop(columns=range(1,len(frame.columns)))
    frame.rename(columns = {0:'core_mpan'},  inplace = True)
    frame['profile_data'] = 'Yes'
    return frame

def add_profile_data():
    global df
    profile_data = get_profile_data_frame()
    df = pd.merge(df, profile_data, how='left', on=['core_mpan', 'core_mpan'])

    #update NaN with 0 for comparison
    df['profile_data'] = df['profile_data'].fillna('No')

def prev_rejected_d10(prev_eligibility_elec):
    global df

    d10 = [col for col in prev_eligibility_elec.columns if 'D10' in col]
    d10 = d10[2:len(d10)]
    d10.insert(0,'core_mpan')

    prev_rejected_d10_frame = prev_eligibility_elec[d10]
    prev_rejected_d10_frame = prev_rejected_d10_frame.drop_duplicates(['core_mpan'])

    df = pd.merge(df, prev_rejected_d10_frame, how='left', on=['core_mpan', 'core_mpan'])
    df[d10[1:]] = df[d10[1:]].fillna('No')
    del prev_rejected_d10_frame, prev_eligibility_elec


def capability_data_collection():
    global df
    prev_eligibility_elec = initdf.open_month_end_df()
    
    add_profile_data()

    rejected_d10()
    accepted_d10()
    accepted_d313()
    prev_rejected_d10(prev_eligibility_elec)


def msn_check():
    global df

    #copy old dataframe
    df_msn = initdf.open_month_end_df()
    
    #use the required dataframes
    df_msn = df_msn[['core_mpan','MSN']]
    df_msn.rename(columns = {'MSN':'Previous MSN'}, inplace = 'True')
    
    #merges dataframes with for msn check
    df_msn = df_msn.drop_duplicates(['core_mpan'])
    df = pd.merge(df, df_msn, how='left', on=['core_mpan', 'core_mpan'])
    
    #update NaN with 0 for comparison
    df['Previous MSN'] = df['Previous MSN'].fillna(0)
    #print(df[['core_mpan','MSN','co_num']])
    
    #check for changed MSN
    df.loc[:,'Meter Changed'] = df.apply(lambda row: 'Yes' if row.MSN != row['Previous MSN'] and row['Previous MSN'] != 0  else 'No', axis = 1)

    df.loc[df['Gain?'] == 'Yes', 'Meter Changed'] = 'Gain'
    
    #drop used and not required column    
    #df = df.drop(columns=['co_num'])
    
    #print(df[['core_mpan','MSN','co_num','msn_change']])

    return df['Meter Changed'].value_counts()['Yes']


def assign_gain():
    global df
    #df1 = pd.read_excel('eligibility_dup.xlsx')
    df_gain = initdf.open_month_end_df()

    last_mon = initdf.get_last_month()

    log(f'i.{df_gain.shape[0]} MPAN(s) in {last_mon} Elec Funnel')

    #gain_mpan_core = df_gain['core_mpan'].tolist()
    #print(gain_mpan_core)
    df.insert(1,'Gain?','')
    
    df.loc[:,'Gain?'] = df.apply(lambda row: 'No' if row.core_mpan in df_gain['core_mpan'].tolist() else 'Yes', axis = 1)
    
    #df.loc[df['gain'] == True, 'mpan_status'] = 'GAIN'
    #df['mpan_status'] = df['mpan_status'].fillna('NA')   

    #df = df.drop(columns='gain')
    

    gain_size = df['Gain?'].value_counts()['Yes']

    log(f'i.{gain_size} Meter Gained this Month')

    del df_gain, gain_size


    return None

def get_loss():
    global df
    
    df_old = initdf.open_month_end_df()



    current_mpan = set(df['core_mpan'].tolist())
    old_mpan = set(df_old['core_mpan'].tolist())
    loss_mpan = list(old_mpan.difference(current_mpan))

    df_loss = df_old.loc[(df_old['core_mpan'].isin(loss_mpan)), ['core_mpan','cust1_cust_nm', 'reg_st_dt']]

    cur_date = initdf.get_current_month()

    output_file = initdf.get_output_location() + 'ChurnLoss_' + cur_date[0] + cur_date[1][:3].upper() + cur_date[2] + '.csv'
    try:
        df_loss.to_csv (output_file, index = None, header=True)
    except:
        log('e.Churn Loss File could not be saved. Possible reasons access to write is restricted or the file with the same name is present and open')

    mpans_lost = df_loss.shape[0]



    log(f'i.{mpans_lost} MPANS Lost this Month')
    del old_mpan, current_mpan, df_loss, mpans_lost

    return None

def reintroduce_churn_loss():
    pass


def elec_initial_cleanse():
    global df

    
    #remove nat_id
    df = df.drop(columns='nat_id')

    #remove future data
    future_date_mpans = df[df.reg_st_dt > initdf.future_reg_date()].shape[0]
    df = df[df.reg_st_dt < initdf.future_reg_date()]

    log(f'i.{future_date_mpans} MPAN(s) registered after {initdf.future_reg_date()} removed')

    mpan_recv = df.shape[0]

    #remove duplicate MPAN and MSN combined
    df = df.drop_duplicates(['core_mpan','co_num'])

    #to get the duplicate size with MSN and MPANs and rem
    dup_size = mpan_recv - df.shape[0]
    log(f'i.{dup_size} MSN(s) duplicates removed')


    #add msn column to count
    df.loc[:,'msn_count'] = df.groupby('core_mpan')['core_mpan'].transform('count')

    #rename to MSN
    df.rename(columns = {'co_num':'MSN'},  inplace = True) 

    #drop duplicate MPANs
    df = df.drop_duplicates(['core_mpan'])

    dup_size = mpan_recv - df.shape[0] - dup_size 

    df= df.sort_values(['core_mpan'], ascending=False)

    log(f'i.{dup_size} MPAN(s) duplicates removed')
    log(f'i.{df.shape[0]} Actual MPAN(s) to process')

    return None

if __name__ == "__main__":
    

    initdf.get_user_details()

    print('Entering in Python Framework.....')



    log('Step 01/10 - Initialize Data Frames')


    #start = time.perf_counter()
    df = initdf.init_dataframe()
    #finish = time.perf_counter()

    #print(f'Time Taken {finish - start} seconds')

    raw_data_size = df.shape[0]
    log(f'i.{raw_data_size} MPAN(s) recorded')
    log('Step 02/10 - Clean-up Data')

    #function call to remove duplicates
    elec_initial_cleanse()
   

    log('Step 03/10 - Assign Gain')

    assign_gain()

    
    log('Step 04/10 - Get Loss MPANs')

    get_loss()

    

    log('Step 05/10 - MSN Changed')

    meters_changed = msn_check()

    log(f'i.{meters_changed} Meter(s) Changed')

    log('Step 06/10 - Compliance')

    capability_data_collection()

    df = compliance.establish_meter_compliance(df)

    #print(df)

    log('Step 07/10 - Reintroduce Churn Loss')
    #reintroduce_churn_loss()

    log('Step 08/10 - Merge Previous Funnel')
    df = eligibility.merge_previous_funnel(df)

    log('Step 09/10 - Create Eligbility Funnel')

    log('Step 10/10 - Report Complete') 


#print(__name__)