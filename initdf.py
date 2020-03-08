import pandas as pd
import json
import glob
from system import log

def open_month_end_df():
    with open('config.JSON') as config_file:
        data = json.load(config_file)
    file_location = data['eligibility_funnel']['location']['elec_report_location']
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
    file_location = data['eligibility_funnel']['location']['elec_report_location']
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
    file_location = data['eligibility_funnel']['location']['elec_report_location']
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
    return data['eligibility_funnel']['location']['elec_report_output_location']