import socket
#import os
#import os.path
import getpass
import pendulum
import time
import os
import json

month = ['Month','January','February','March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

def validate_json_conf():

    try:
        with open('config.JSON') as config_file:
            data = json.load(config_file)
    except:
        log('e.Config File not present or not configured properly.\nThe program cannot run')
        quit()

    #to validate location have been defined in the config file
    try:
        #get location of files from esbos and previous funnels
        raw_report_location = data['eligibility_funnel']['location']['elec_report_location']
        funnel_output = data['eligibility_funnel']['location']['elec_report_output_location']
        #raw_report_location = data['eligibility_funnel']['location']['locate_lookup']
        #raw_report_location = data['eligibility_funnel']['location']['locate_flowdata']
    except:
        log('e.Location not defined or config file not valid.\nThe program cannot run')
        quit()
    
    #get list of raw data files for funnel creation
    try:
        #list of files present in the funnel directory on the system
        dir_list = os.listdir(raw_report_location)
        #list of files defined in the json
        files = [*data['eligibility_funnel']['elec_reports'].values()]
        #validate if files are present in the directory

        for file in files:
            if file in dir_list:
                pass
            else:
                raise FileNotFoundError      
    except FileNotFoundError:
        log('e.Funnel raw data not present or not configured properly.\nThe program cannot run')
        quit()
    except:
        log('e.Configuration not set properly.\nThe program cannot run')
        quit()

    #get list of raw data files for funnel creation
    try:
        #list of files present in the funnel directory on the system
        dir_list = os.listdir(funnel_output)   
    except FileNotFoundError:
        try:
            path = os.getcwd()
            path = path + '/' + raw_report_location + 'output'
            os.mkdir(path)
        except OSError:
            log(f'e.Creation of the directory {path} failed.\nThe program cannot run')
            quit()
        else:
            log(f"i.Successfully created the output directory at {path}")
    except:
        log('e.Output folder cannot be created.\nThe program cannot run')
        quit()

def log(message):

    log_time = time.strftime("%H:%M:%S",time.localtime())
    if message[:2] == 'i.':
        print(f'{log_time}: [INFO] : {message[2:]}')
    elif message[:2] == 'w.':
        print(f'{log_time}: [WARNING] : {message[2:]}')
    elif message[:2] == 'e.':
        print(f'{log_time}: [ERROR] : {message[2:]}') 
    else:
        print(f'{log_time}: {message}')

def user_info():
    try: 
        host_name = socket.gethostname() 
        host_ip = socket.gethostbyname(host_name) 
        print(f'\nHOST : {host_name} \tIP : {host_ip}')
    except: 
        log("e.Unable to get Hostname and IP") 
    
    try:
        username = getpass.getuser()
        print(f'USER : {username}')
    except:
        log("e.User Unidentified") 
    
    cur_date = get_current_month()
    print(f'DATE : {cur_date[1]} {cur_date[0]}, {cur_date[2]}')

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

def get_last_month(month_nums):
    global month
    mon = int(pendulum.datetime.today().subtract(months=month_nums).strftime('%m'))
    return month[mon]