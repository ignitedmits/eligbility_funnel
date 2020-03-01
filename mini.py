import pandas as pd
import datetime

df = pd.read_excel('esbos/mpan_compliance.xlsx')
#df = establish_meter_compliance(df)
df['current_capability'] = 'U'
df = df.loc[df['Meter Changed'] == 'Gain']

amr_type_arr = ['RCAMY', 'RCAMR']
smets2_mtr = ['S2', 'S2A', 'S2AD', 'S2ADE']
smets1_mtr = ['S1', 'S1A', 'S1AD', 'S1ADE']


s1_capable = df['current_capability'] == 'SMETS1 Capable'



df.loc[(df['mtr_typ'].str.contains(smets2_mtr[0])) | (df['mtr_typ'].str.contains(smets2_mtr[1])) | (df['mtr_typ'].str.contains(smets2_mtr[2])) | (df['mtr_typ'].str.contains(smets2_mtr[3])), 'current_capability'] = 'SMETS2 Capable'

df.loc[(df['mtr_typ'].str.contains(smets1_mtr[0])) | (df['mtr_typ'].str.contains(smets1_mtr[1])) | (df['mtr_typ'].str.contains(smets1_mtr[2])) | (df['mtr_typ'].str.contains(smets1_mtr[3])), 'current_capability'] = 'SMETS1 Capable'

is_s1_compliant = df['mtr_inst_dt'] < datetime.date(2008, 12, 5)
s1_capable = df['current_capability'] == 'SMETS1 Capable'
df.loc[is_s1_compliant & s1_capable, 'current_capability'] = 'SMETS1 Compliant'

has_d313 = df['Accepted_D313'] == 'Yes'
has_d10 = df['Accepted_D10'] == 'Yes'
no_rejected_d10 = df['Rejected_D10'] == 'No'

df.loc[has_d313 & has_d10 & no_rejected_d10 & (df['current_capability'] == 'U'), 'current_capability'] = 'AMR Compliant'

has_d313 = df['Accepted_D313'] == 'Yes'

df.loc[has_d313 & (df['current_capability'] == 'U'), 'current_capability'] = 'AMR Capable'


df.loc[(df['amr_type'].isin(amr_type_arr)) & has_d10 & no_rejected_d10 & (df['current_capability'] == 'U'), 'current_capability'] = 'AMR Compliant'


df.loc[(df['amr_type'].isin(amr_type_arr)) & (df['current_capability'] == 'U'), 'current_capability'] = 'AMR Capable'


df.loc[(df['current_capability'] == 'U'), 'current_capability'] = 'Traditional'

print(df[['core_mpan','amr_type','mtr_typ','Meter Changed','Accepted_D313','profile_data','Accepted_D10','Rejected_D10', 'D10 (Aug)','D10 (July)', 'D10 (June)',  'D10 (May)' , 'D10 (Apr)', 'D10 (March)','current_capability']])