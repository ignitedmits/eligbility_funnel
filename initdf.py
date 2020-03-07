import pandas as pd
import json
import glob
import time

import socket
import os
import os.path
import getpass

import pendulum


month = ['Month','January','February','March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']


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


def open_sic_file():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    file_location = data['eligibility_funnel']['funnel_lookups']['locate_lookup']
    file_name = data['eligibility_funnel']['funnel_lookups']['sic_code']
    sic_code_file = file_location + file_name
    if sic_code_file[-4:] == '.csv':
        return  pd.read_csv(sic_code_file)
    else:
        return pd.read_excel(sic_code_file)


def open_meter_type():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    file_location = data['eligibility_funnel']['funnel_lookups']['locate_lookup']
    file_name = data['eligibility_funnel']['funnel_lookups']['meter_type']
    meter_typ_file = file_location + file_name
    if meter_typ_file[-4:] == '.csv':
        return  pd.read_csv(meter_typ_file)
    else:
        return pd.read_excel(meter_typ_file)


def open_rate_type():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    file_location = data['eligibility_funnel']['funnel_lookups']['locate_lookup']
    file_name = data['eligibility_funnel']['funnel_lookups']['rate_type']
    rate_type_file = file_location + file_name
    if rate_type_file[-4:] == '.csv':
        return  pd.read_csv(rate_type_file)
    else:
        return pd.read_excel(rate_type_file)


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


def open_meter_model():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    file_location = data['eligibility_funnel']['funnel_lookups']['locate_lookup']
    file_name = data['eligibility_funnel']['funnel_lookups']['meter_model']
    meter_model_file = file_location + file_name
    if meter_model_file[-4:] == '.csv':
        return  pd.read_csv(meter_model_file)
    else:
        return pd.read_excel(meter_model_file)


def open_meter_tariff():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    file_location = data['eligibility_funnel']['funnel_lookups']['locate_lookup']
    file_name = data['eligibility_funnel']['funnel_lookups']['tariff']
    meter_tariff = file_location + file_name
    if meter_tariff[-4:] == '.csv':
        return  pd.read_csv(meter_tariff)
    else:
        return pd.read_excel(meter_tariff)


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


def get_current_elec_name():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    return data['eligibility_funnel']['elec_reports']['current_name']

def get_output_location():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    return data['eligibility_funnel']['elec_reports']['elec_report_output_location']

def log(message):

    log_time = time.strftime("%H:%M:%S",time.localtime())
    if message[:2] == 'i.':
        print(f'{log_time}:\t[INFO] : {message[2:]}')
    elif message[:2] == 'w.':
        print(f'{log_time}:\t[WARNING] : {message[2:]}')
    elif message[:2] == 'e.':
        print(f'{log_time}:\t[ERROR] : {message[2:]}') 
    else:
        print(f'{log_time}:\t{message}')

def get_user_details():
    try:
        username = getpass.getuser()
        print(f'USER : {username}')
    except:
        print("User Unidentified") 

    try: 
        host_name = socket.gethostname() 
        host_ip = socket.gethostbyname(host_name) 
        print(f'HOST : {host_name} \tIP : {host_ip}')
    except: 
        print("Unable to get Hostname and IP") 

def get_current_month():
    global month
    #get current month from today's date
    mon = int(pendulum.datetime.today().strftime('%m'))
    cur_month = month[mon]
    #get current year
    cur_year = pendulum.datetime.today().strftime('%Y')
    #get todays date
    cur_date = pendulum.datetime.today().strftime('%d')
    #cur_year = int(pendulum.datetime.today().strftime('%Y'))
    return cur_date, cur_month, cur_year

def get_last_month():
    global month
    mon = int(pendulum.datetime.today().subtract(months=1).strftime('%m'))
    return month[mon]