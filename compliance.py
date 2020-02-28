import pandas as pd
import initdf

def no_meter_change_compliance(df):
    
    

    #traditional capability
    is_traditional = df['previous_capability'] == 'Traditional'
    #not_traditional = df['previous_capability'] != 'Traditional'

    #Filter the “Previous Capability” column to show “Traditional” meters only

    #Validate against the “Previous Funnel Capability” column to ensure consistent capability
    #If the capability differs between capabilities take the funnel capability as correct as this would have been most recently updated
    #Copy the Traditional value from previous capabilities columns into “Current Capability” column

    df.loc[is_traditional, 'current_capability'] = df.apply(lambda row: row.funnel_capability if row.previous_capability != row.funnel_capability else row.previous_capability, axis = 1)
    
    #non traditional meters
    #Set the “Current Capabilities” column to show only blanks and remove filters from previous capabilities columns
    #Filter “Previous Capability” column to show only “S2 Capable” meters 
    #Copy the “S2 Capable” meters from previous capabilities columns into “Current Capability” column
    #Remove filters from previous capabilities columns
    #Filter “Previous Capability” column to show only “S1 Capable” meters 
    #Copy the “S1 Capable” meters from previous capabilities columns into “Current Capability” column
    #Remove filters from previous capabilities columns
    #Filter “Previous Capability” column to show only “S1 Compliant” meters
    #Copy the “S1 Compliant” meters from previous capabilities columns into “Current Capability” column
    #Remove filters from previous capabilities columns
    #Filter “Previous Capability” column to show only “AMR Capable NRO” meters
    #Copy the AMR Capable NRO meters from previous capabilities columns into “Current Capability” column

    meter_capability = ['S2 Capable', 'S1 Capable', 'S1 Compliant', 'AMR Capable NRO']    
    df.loc[(df['previous_capability'].isin(meter_capability)), 'current_capability' ] = df['previous_capability']

    #Remove filters from previous capabilities columns
    #Filter “Previous Capability” column to show only “AMR Compliant” meters
    #Filter the D313 column to show “Yes”, “D10 Accepted” Column to show “Yes”, “Rejected D10” to show “No”
    #Mark these as “AMR Compliant” in the “Current Capability” column

    prev_amr_compliant = df['previous_capability'] == 'AMR Compliant'
    has_d313 = df['Accepted_D313'] == 'Yes'
    has_d10 = df['Accepted_D10'] == 'Yes'
    no_rejected_d10 = df['Rejected_D10'] == 'No'

    df.loc[prev_amr_compliant & has_d313 & has_d10 & no_rejected_d10, 'current_capability'] = 'AMR Compliant'

    #Remove the filer from “D10 accepted” and “Rejected D10”, now check the historical D10 received, 
    #if we have received a D10 within the last 6 months mark as “AMR Compliant” in the “Current Capability” column

    #historic D10s
    d10_arr = [col for col in df.columns if 'D10' in col][2:]    
    df.loc[has_d313 & (df['current_capability'] == '') & prev_amr_compliant & ((df[d10_arr[0]] == 'Yes') | (df[d10_arr[1]] == 'Yes') | (df[d10_arr[2]] == 'Yes') | (df[d10_arr[3]] == 'Yes') | (df[d10_arr[4]] == 'Yes') | (df[d10_arr[5]] == 'Yes')), 'current_capability'] = 'AMR Compliant'

    #Should a previously declared “AMR Compliant” meter now show no D10 reads or “Profile Data” 
    #mark as “AMR Capable” in the “Current Capability” column
    profile_data = df['profile_data'] == 'Yes'

    df.loc[has_d313 & (df['current_capability'] == '') & prev_amr_compliant & (~profile_data & ~has_d10 & (df[d10_arr[0]] == 'No') & (df[d10_arr[1]] == 'No') & (df[d10_arr[2]] == 'No') & (df[d10_arr[3]] == 'No') & (df[d10_arr[4]] == 'No') & (df[d10_arr[5]] == 'No') ), 'current_capability'] = 'AMR Capable'

    #df.loc[has_d313 & prev_amr_compliant & (df['current_capability'] == '') & (profile_data | has_d10 | ~no_rejected_d10),'current_capability'] = 'AMR Capable'


    #Remove filters from previous capabilities columns
    #Filter “Previous Capability” column to show only “AMR Capable” meters

    prev_amr_capable = df['previous_capability'] == 'AMR Capable'

    #Repeat the above process to establish if the “AMR Capable” meter is now receiving “Accepted D10” readings or “Profile Data”, 
    #if so mark as “AMR Compliant”
    df.loc[prev_amr_capable & has_d313 & has_d10 & no_rejected_d10, 'current_capability'] = 'AMR Compliant'

    df.loc[has_d313 & (df['current_capability'] == '') & prev_amr_capable & ((df[d10_arr[0]] == 'Yes') | (df[d10_arr[1]] == 'Yes') | (df[d10_arr[2]] == 'Yes') | (df[d10_arr[3]] == 'Yes') | (df[d10_arr[4]] == 'Yes') | (df[d10_arr[5]] == 'Yes')), 'current_capability'] = 'AMR Compliant'

    #If no “Accepted D10” or “Profile Data” is found then mark as “AMR Capable”

    df.loc[(df['current_capability'] == '') & prev_amr_capable & (profile_data | ~has_d10), 'current_capability'] = 'AMR Capable'

    #Added by Mithun to be verified with Tom
    df.loc[(df['current_capability'] == '') & (has_d313 | profile_data), 'current_capability'] = 'AMR Compliant'

    return df

def meter_changed_compliance(df):
    amr_type_arr = ['RCAMY', 'RCAMR']

    #Filter the 'Previous Capability' column to show 'Traditional' meters only
    #Check for the presence of a D313 flow by filtering the 'D313 Received' column to show 'Yes', this indicates an AMR meter at site
    #Filter the 'D10 Accepted' Column to show 'Yes', Rejected D10 to show 'No' and Profile Data 'Yes'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    #Remove the filter from 'Profile Data'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    is_traditional = df['previous_capability'] == 'Traditional'
    has_d313 = df['Accepted_D313'] == 'Yes'
    has_d10 = df['Accepted_D10'] == 'Yes'
    no_rejected_d10 = df['Rejected_D10'] == 'No'

    df.loc[is_traditional & has_d313 & has_d10 & no_rejected_d10, 'current_capability'] = 'AMR Compliant'

    #Remove the filter from 'D10 accepted' and 'Rejected D10' and mark these as 'AMR Capable'

    df.loc[is_traditional & has_d313 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Capable'

    #Remove the filter from 'D313 Received'
    #Filter column titled 'amr_type' to show AMR codes 'RCAMR' or 'RCAMY'
    #Filter the 'D10 Accepted' Column to show 'Yes', Rejected D10 to show 'No' and Profile Data 'Yes'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    #Remove the filter from 'Profile Data'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column

    df.loc[is_traditional & (df['amr_type'].isin(amr_type_arr)) & has_d10 & no_rejected_d10 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Compliant'

    #Remove the filer from 'D10 accepted' and 'Rejected D10' and mark these as 'AMR Capable

    df.loc[is_traditional & (df['amr_type'].isin(amr_type_arr)) & (df['current_capability'] == ''), 'current_capability'] = 'AMR Capable'

    #Remove the filters from 'amr_type'
    #Filter column 'mtr_typ' to show any meter types containing 'RCAMR' or 'RCAMY'
    #Filter the 'D10 Accepted' Column to show 'Yes', Rejected D10 to show 'No' and Profile Data 'Yes'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    #Remove the filter from 'Profile Data'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    df.loc[is_traditional & ((df['mtr_typ'].str.contains(amr_type_arr[0])) | (df['mtr_typ'].str.contains(amr_type_arr[1]))) & has_d10 & no_rejected_d10 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Compliant'

    #Remove the filer from 'D10 accepted' and 'Rejected D10' and mark these as 'AMR Capable
    df.loc[is_traditional & ((df['mtr_typ'].str.contains(amr_type_arr[0])) | (df['mtr_typ'].str.contains(amr_type_arr[1]))) & (df['current_capability'] == ''), 'current_capability'] = 'AMR Capable'

    #Remove the filter from 'mtr_typ'
    #Mark the remaining meters as 'Traditional' in the 'Current Capability' column
    df.loc[is_traditional & (df['current_capability'] == ''), 'current_capability'] = 'Traditional'

    #added by Mithun to be verified by Tom S
    #The remaining non Traditionals
    df.loc[~is_traditional, 'current_capability'] = df['previous_capability']
    return df

def meter_gained_compliance(df):
    amr_type_arr = ['RCAMY', 'RCAMR']
    smets2_mtr = ['S2', 'S2A', 'S2AD', 'S2ADE']
    smets1_mtr = ['S1', 'S1A', 'S1AD', 'S1ADE']

    #Filter column 'mtr_typ' to show any 'S2, S2A, S2AD or S2ADE' values, mark these as 'SMETS2 Capable' in the 'Current Capability' column
    df.loc[(df['mtr_typ'].str.contains(smets2_mtr[0])) | (df['mtr_typ'].str.contains(smets2_mtr[1])) | (df['mtr_typ'].str.contains(smets2_mtr[2])) | (df['mtr_typ'].str.contains(smets2_mtr[3])), 'current_capability'] = 'SMETS2 Capable'

    #Filter column 'mtr_typ' to show 'S1, S1A, S1AD or S1ADE' values, this indicates a 'SMETS1' meter
    #Filter column 'mtr_inst_dr' to show all installed post 5th Dec 2018, mark these as 'SMETS1 Capable' in the 'Current Capability' column
    df.loc[(df.loc[:,'mtr_typ'].str.contains(smets1_mtr[0])) | (df.loc[:,'mtr_typ'].str.contains(smets1_mtr[1])) | (df.loc[:,'mtr_typ'].str.contains(smets1_mtr[2])) | (df.loc[:,'mtr_typ'].str.contains(smets1_mtr[3])), 'current_capability'] = 'SMETS1 Capable'

    #Filter column 'mtr_inst_dt' to show all installs pre 5th Dec 2018, mark these as 'SMETS1 Compliant' in the 'Current Capability' column
    is_s1_compliant = df['mtr_inst_dt'] < initdf.s1_compliant_date()
    s1_capable = df['current_capability'] == 'SMETS1 Capable'
    df.loc[is_s1_compliant & s1_capable, 'current_capability'] = 'SMETS1 Compliant'

    #Remove all filters from the 'mtr_inst_dr' and 'mtr_typ' columns
    #Check for the presence of a D313 flow by filtering the 'D313 Received' column to show 'Yes', this indicates an AMR meter at site
    #Filter the 'D10 Accepted' Column to show 'Yes', Rejected D10 to show 'No' and Profile Data 'Yes'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    #Remove the filter from 'Profile Data'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column

    has_d313 = df['Accepted_D313'] == 'Yes'
    has_d10 = df['Accepted_D10'] == 'Yes'
    no_rejected_d10 = df['Rejected_D10'] == 'No'
    df.loc[has_d313 & has_d10 & no_rejected_d10 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Compliant'

    #Remove the filer from 'D10 accepted' and 'Rejected D10' and mark these as 'AMR Capable'
    df.loc[has_d313 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Capable'

    #Remove the filter from 'D313 Received'
    #Filter column titled 'amr_type' to show AMR codes 'RCAMR' or 'RCAMY'
    #Filter the 'D10 Accepted' Column to show 'Yes', Rejected D10 to show 'No' and Profile Data 'Yes'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    #Remove the filter from 'Profile Data'
    #Mark these as 'AMR Compliant' in the 'Current Capability' column
    df.loc[(df['amr_type'].isin(amr_type_arr)) & has_d10 & no_rejected_d10 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Compliant'

    #Remove the filer from 'D10 accepted' and 'Rejected D10' and mark these as 'AMR Capable
    df.loc[(df['amr_type'].isin(amr_type_arr)) & (df['current_capability'] == ''), 'current_capability'] = 'AMR Capable'

    #with Meter Type
    #df.loc[((df['mtr_typ'].str.contains(amr_type_arr[0])) | (df['mtr_typ'].str.contains(amr_type_arr[1]))) & has_d10 & no_rejected_d10 & (df['current_capability'] == ''), 'current_capability'] = 'AMR Compliant'
    #df.loc[((df['mtr_typ'].str.contains(amr_type_arr[0])) | (df['mtr_typ'].str.contains(amr_type_arr[1]))) & (df['current_capability'] == ''), 'current_capability'] = 'AMR Capable'

    #pending Traditionals
    df.loc[(df['current_capability'] == ''), 'current_capability'] = 'Traditional'
    return df

def establish_meter_compliance(df):
    month_end_df = initdf.open_month_end_df()
    month_end_df = month_end_df[[month_end_df.columns[0], 'Current Capability']]
    mpan_col = month_end_df.columns[0]
    month_end_df.rename(columns = {mpan_col:'core_mpan'},  inplace = True)
    df = pd.merge(df, month_end_df, how='left', on=['core_mpan', 'core_mpan'])
    df.rename(columns = {'Current Capability':'previous_capability'}, inplace = 'True')


    prev_df = initdf.open_previous_funnel()
    prev_df = prev_df[[prev_df.columns[0], 'Current Meter']]
    mpan_col = prev_df.columns[0]
    prev_df.rename(columns = {mpan_col:'core_mpan'},  inplace = True)
    df = pd.merge(df, prev_df, how='left', on=['core_mpan', 'core_mpan'])
    df.rename(columns = {'Current Meter':'funnel_capability'}, inplace = 'True')
    
df.loc['current_capability'] = ''
    compliance_df = []

    #for changed meters
    #Filter the 'Meter Change' column to show all meters that have changed 
    #between last month and this month
    compliance_df.append(df.loc[df['Meter Changed'] == 'Yes'])

    compliance_df[0] = meter_changed_compliance(compliance_df[0])

    #for same meters and no gains
    #Filter the 'Meter Change' column to show all meters that have NOT changed 
    #between last month and this month
    compliance_df.append(df.loc[df['Meter Changed'] == 'No'])
    compliance_df[1] = no_meter_change_compliance(compliance_df[1])

    #for gained meters
    #Filter the 'Meter Change' column to show all meters that have #N/A value, this indicates a gained meter
    #Here 'Meter Changed' == 'Gain'
    compliance_df.append(df.loc[df['Meter Changed'] == 'Gain'])
    compliance_df[2] = meter_gained_compliance(compliance_df[2])

    df.rename(columns = {'funnel_capability':'Funnel Capability', 'previous_capability':'Previous Capability'}, inplace = 'True')
    
    return pd.concat(compliance_df, axis=0, ignore_index=True, sort=False)