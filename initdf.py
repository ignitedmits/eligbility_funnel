import pandas as pd
import json
import glob
import time

def open_month_end_df():
    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['elec_reports']['elec_report_location']
    file_name_month_end = data['eligibility_funnel']['elec_reports']['elec_month_end']
    
    month_end_file = file_location + file_name_month_end
    
    if month_end_file[-4:] == '.csv':
        return pd.read_csv(month_end_file)
    else:
        return pd.read_excel(month_end_file)

    #month_end_df = month_end_df[['core_mpan', 'Current Meter Capability']]
    
    #month_end_df.rename(columns = {'Current Meter Capability':'previous_capability'},  inplace = True)

    #return month_end_df


def open_meter_type():
    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['funnel_lookups']['funnel_lookups']
    file_name = data['eligibility_funnel']['funnel_lookups']['meter_type']

    meter_typ_file = file_location + file_name

    if meter_typ_file[-4:] == '.csv':
        return  pd.read_csv(meter_typ_file)
    else:
        return pd.read_excel(meter_typ_file)


def open_previous_funnel():

    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['elec_reports']['elec_report_location']
    file_name = data['eligibility_funnel']['elec_reports']['elec_previous_funnel']

    elec_file = file_location + file_name

    if elec_file[-4:] == '.csv':
        return  pd.read_csv(elec_file)
    else:
        return pd.read_excel(elec_file)


def open_accepted_d10():

    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['flow_data']['locate_lookup']
    file_name = data['eligibility_funnel']['flow_data']['accepted_d10']

    d10_file = file_location + file_name

    if d10_file[-4:] == '.csv':
        return  pd.read_csv(d10_file)
    else:
        return pd.read_excel(d10_file)


def open_accepted_d313():

    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['flow_data']['locate_lookup']
    file_name = data['eligibility_funnel']['flow_data']['accepted_d313']

    d313_file = file_location + file_name

    if d313_file[-4:] == '.csv':
        return  pd.read_csv(d313_file)
    else:
        return pd.read_excel(d313_file)


def open_rejected_d10():

    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['flow_data']['locate_lookup']
    file_name = data['eligibility_funnel']['flow_data']['rejected_d10']

    d10_rejected_file = file_location + file_name

    if d10_rejected_file[-4:] == '.csv':
        return  pd.read_csv(d10_rejected_file)
    else:
        return pd.read_excel(d10_rejected_file)

def get_profile_data():

    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['flow_data']['locate_lookup']
    file_name = data['eligibility_funnel']['flow_data']['profile_data']

    profile_data_path = file_location + file_name


    path = profile_data_path # use your path
    all_files = glob.glob(path + "/*.xlsx")

    li = []
    for filename in all_files:
        pf_data = pd.read_excel(filename, header=None)
        li.append(pf_data)
        
    return pd.concat(li, axis=0, ignore_index=True, sort=True)

def init_dataframe():

    #print('Entering in Python Framework')

    with open('config.JSON') as config_file:
        data = json.load(config_file)

    file_location = data['eligibility_funnel']['elec_reports']['elec_report_location']
    file_name = data['eligibility_funnel']['elec_reports']['elec_esbos_raw']

    elec_file = file_location + file_name

    if elec_file[-4:] == '.csv':
        return  pd.read_csv(elec_file)
    else:
        return pd.read_excel(elec_file)

def future_reg_date():
    with open('config.JSON') as config_file:
        data = json.load(config_file)

    return data['eligibility_funnel']['elec_config']['future_reg_date']

def s1_compliant_date():
    with open('config.JSON') as config_file:
        data = json.load(config_file)

    return data['eligibility_funnel']['elec_config']['s1_compliant_date']

def log(message):
    log_time = time.strftime("%H:%M:%S",time.localtime())
    print(f'{log_time}:  {message}')