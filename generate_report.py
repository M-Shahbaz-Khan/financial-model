import csv
import pandas as pd
import numpy as np
import time
import sys
import pickle
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timedelta
from datetime import date
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import requests
import calendar
import pickle
import json
from pathlib import Path
from airtable import Airtable
from varname import nameof
from dateutil.relativedelta import relativedelta

def remove_fields(df):
    df.columns = list(map(lambda x: str.replace(x, 'fields.', ''), df.columns.values))
    return df

def get_gsheet_creds():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'gsheet_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

pd.set_option('display.max_columns', None) # display all columns
pd.set_option('display.max_rows', 100)
pd.set_option('display.min_rows', 60)

file = open("./airtable_credentials.txt", "r")
contents = file.read()
airtable_credentials = contents
file.close()

file = open("./hubspot_credentials.txt", "r")
contents = file.read()
hubspot_credentials = contents
file.close()

fresh_data = False

base_key = 'app9RJbzpT3jQFn1A'
if(fresh_data):
    print('Getting Stripe Payments')
    airtable = Airtable(base_key, 'Stripe Payments', airtable_credentials)
    Airtable_Stripe_Payments = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Stripe_Payments = remove_fields(Airtable_Stripe_Payments)

    print('Getting Stripe Disbursements')
    airtable = Airtable(base_key, 'Stripe Disbursements', airtable_credentials)
    Airtable_Stripe_Disbursements = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Stripe_Disbursements = remove_fields(Airtable_Stripe_Disbursements)

    print('Getting Stripe Invoices')
    airtable = Airtable(base_key, 'Stripe Invoices', airtable_credentials)
    Airtable_Stripe_Invoices = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Stripe_Invoices = remove_fields(Airtable_Stripe_Invoices)

    print('Getting Reconciliation')
    airtable = Airtable(base_key, 'Reconciliation', airtable_credentials)
    Reconciliation = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Reconciliation = remove_fields(Reconciliation)

    print('Getting WC Policies')
    wc_policy_base = 'appjFfjDxAogLiXWI'
    airtable = Airtable(wc_policy_base, 'WC Policies', airtable_credentials)
    wc_policies = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    wc_policies = remove_fields(wc_policies)

    print('Getting WCS Transaction History')
    airtable = Airtable(wc_policy_base, 'WCS Transaction History', airtable_credentials)
    WCS_Transaction_History = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    WCS_Transaction_History = remove_fields(WCS_Transaction_History)

    print('Getting WCS State Rating')
    airtable = Airtable(wc_policy_base, 'WCS State Rating', airtable_credentials)
    WCS_State_Rating = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    WCS_State_Rating = remove_fields(WCS_State_Rating)

    print('Getting WCS General')
    airtable = Airtable(wc_policy_base, 'WCS General', airtable_credentials)
    WCS_General = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    WCS_General = remove_fields(WCS_General)

    print('Getting Other Carrier Transactions')
    airtable = Airtable(wc_policy_base, 'Other Carrier Transactions', airtable_credentials)
    Other_Transaction_History = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Other_Transaction_History = remove_fields(Other_Transaction_History)

    print('Getting IPFS Transaction History')
    ipfs_base = 'appvudnVFWtHNxmQQ'
    airtable = Airtable(ipfs_base, 'IPFS Transaction History', airtable_credentials)
    IPFS_Transaction_History = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    IPFS_Transaction_History = remove_fields(IPFS_Transaction_History)

    print('Getting Glow Payments')
    billing_base = 'appzMh02XqmJGbjlo'
    airtable = Airtable(billing_base, 'Glow Payments', airtable_credentials)
    Airtable_Glow_Payments = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: '||'.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Glow_Payments = remove_fields(Airtable_Glow_Payments)

    print('Getting A&H Policies')
    ah_base = 'appcWje2y5RZJY6PM'
    airtable = Airtable(ah_base, 'A&H Policies', airtable_credentials)
    Airtable_AH_Policies = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_AH_Policies = remove_fields(Airtable_AH_Policies)

    print('Getting A&H Policies Members')
    airtable = Airtable(ah_base, 'A&H Policies Members', airtable_credentials)
    Airtable_AH_Policies_Members = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_AH_Policies_Members = remove_fields(Airtable_AH_Policies_Members)

    print('Getting A&H Benefit Enrollment Applications')
    airtable = Airtable(ah_base, 'A&H Benefit Enrollment Applications', airtable_credentials)
    Airtable_AH_Benefit_Enrollment = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_AH_Benefit_Enrollment = remove_fields(Airtable_AH_Benefit_Enrollment)

    print('Getting A&H Employer Enrollment')
    airtable = Airtable(ah_base, 'A&H Employer Enrollment', airtable_credentials)
    Airtable_AH_Employer_Enrollment = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_AH_Employer_Enrollment = remove_fields(Airtable_AH_Employer_Enrollment)

    print('Getting Acctg Bord')
    airtable = Airtable(ah_base, 'Acctg Bord', airtable_credentials)
    AH_Bord = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    AH_Bord = remove_fields(AH_Bord)

    print('Getting Chubb Statement of Account')
    chubb_statement_base = 'appX4eY4wipGvD2n4'
    airtable = Airtable(chubb_statement_base, 'Chubb Statement of Account', airtable_credentials)
    Airtable_Chubb_Statement = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Chubb_Statement = remove_fields(Airtable_Chubb_Statement)
    # Get all deals in Sales Pipeline ##############################################

    # Get all deals' field names
    headers = {'Content-Type':'application/json'}
    querystring = {"archived":"false","hapikey":hubspot_credentials}
    url = "https://api.hubapi.com/crm/v3/properties/deals"
    response = requests.request("GET", url, headers=headers, params=querystring)
    deal_fields_list = pd.json_normalize(json.loads(response.text)['results']).name.values.tolist()
    deal_fields = ','.join(deal_fields_list)

    df_deals = pd.DataFrame()
    url = 'https://api.hubapi.com/crm/v3/objects/deals/search?hapikey=' + hubspot_credentials
    data = '''{ "archived": false, "limit": 100, "properties":''' + str(deal_fields_list).replace('\'', '\"') + ''',
        "filterGroups":[
          {
            "filters":[
                    {
                        "propertyName": "pipeline",
                        "operator": "EQ",
                        "value": "default"
                    }
            ]
          },
          {
            "filters":[
                    {
                        "propertyName": "pipeline",
                        "operator": "EQ",
                        "value": "1052209"
                    }
            ]
          }
        ]
    }'''

    after = 0
    print('Retrieving deals')
    while(after is not None):
        retries = 0
        while(True):
            retries += 1
            try:
                if type(after) != type(0):
                    data = '''{
                        "after":''' + str(after) + ''', "archived": false, "limit": 100,
                        "properties":''' + str(deal_fields_list).replace('\'', '\"') + ''',
                        "filterGroups":[
                          {
                            "filters":[
                                    {
                                        "propertyName": "pipeline",
                                        "operator": "EQ",
                                        "value": "default"
                                    }
                            ]
                          },
                          {
                            "filters":[
                                    {
                                        "propertyName": "pipeline",
                                        "operator": "EQ",
                                        "value": 1052209
                                    }
                            ]
                          }
                        ]
                    }'''
                response = json.loads(requests.request("POST", url, headers=headers, data=data).text)
                deals = pd.json_normalize(response['results'])[['properties.policy_effective_date', 'properties.amount', 'properties.dealstage', 'properties.dealtype', 'properties.hs_object_id']]
                df_deals = df_deals.append(deals)
                after = response.get('paging', {'next':None})['next']
                if after is not None:
                    after = after.get('after', None)
                print('Retrieved', len(df_deals), 'deals:', pd.to_datetime(deals['properties.policy_effective_date']).min(), end='\r')
                break
            except Exception:
                # Make sure we get all deals
                print(sys.exc_info())
                print('failed, retries', retries)
                if(retries > 3):
                    raise ValueError
    print('\nRetrieved', len(df_deals), 'deals, max effective date:', pd.to_datetime(deals['properties.policy_effective_date']).max())
    df_deals.reset_index(drop=True, inplace=True)

    deal_stage_mapping = pd.DataFrame()
    pipeline_mapping = dict()

    for idx, row in pd.read_json('./dealPipelines.json').iterrows():
        deal_stage_mapping = deal_stage_mapping.append(pd.DataFrame(row['stages']))
        pipeline_mapping[row['id']] = row['label']

    deal_stage_mapping = dict(zip(deal_stage_mapping.id, deal_stage_mapping.label))

    df_deals['dealstage'] = df_deals['properties.dealstage'].map(lambda x: deal_stage_mapping.get(x, x))
    ################################################################################

    Airtable_Stripe_Payments.to_pickle('Airtable_Stripe_Payments.pkl')
    Airtable_Stripe_Disbursements.to_pickle('Airtable_Stripe_Disbursements.pkl')
    Airtable_Stripe_Invoices.to_pickle('Airtable_Stripe_Invoices.pkl')
    WCS_Transaction_History.to_pickle('WCS_Transaction_History.pkl')
    WCS_General.to_pickle('WCS_General.pkl')
    WCS_State_Rating.to_pickle('WCS_State_Rating.pkl')
    Airtable_Glow_Payments.to_pickle('Airtable_Glow_Payments.pkl')
    Airtable_AH_Policies.to_pickle('Airtable_AH_Policies.pkl')
    Airtable_AH_Policies_Members.to_pickle('Airtable_AH_Policies_Members.pkl')
    Airtable_AH_Benefit_Enrollment.to_pickle('Airtable_AH_Benefit_Enrollment.pkl')
    Airtable_AH_Employer_Enrollment.to_pickle('Airtable_AH_Employer_Enrollment.pkl')
    Other_Transaction_History.to_pickle('Other_Transaction_History.pkl')
    df_deals.to_pickle('df_deals.pkl')
    wc_policies.to_pickle('wc_policies.pkl')
    IPFS_Transaction_History.to_pickle('IPFS_Transaction_History.pkl')
    Airtable_Chubb_Statement.to_pickle('Airtable_Chubb_Statement.pkl')
    Reconciliation.to_pickle('Reconciliation.pkl')
    AH_Bord.to_pickle('AH_Bord.pkl')

Airtable_Stripe_Payments = pd.read_pickle('Airtable_Stripe_Payments.pkl')
Airtable_Stripe_Disbursements = pd.read_pickle('Airtable_Stripe_Disbursements.pkl')
Airtable_Stripe_Invoices = pd.read_pickle('Airtable_Stripe_Invoices.pkl')
WCS_Transaction_History = pd.read_pickle('WCS_Transaction_History.pkl')
WCS_General = pd.read_pickle('WCS_General.pkl')
WCS_State_Rating = pd.read_pickle('WCS_State_Rating.pkl')
Airtable_Glow_Payments = pd.read_pickle('Airtable_Glow_Payments.pkl')
Airtable_AH_Policies = pd.read_pickle('Airtable_AH_Policies.pkl')
Airtable_AH_Policies_Members = pd.read_pickle('Airtable_AH_Policies_Members.pkl')
Airtable_AH_Benefit_Enrollment = pd.read_pickle('Airtable_AH_Benefit_Enrollment.pkl')
Airtable_AH_Employer_Enrollment = pd.read_pickle('Airtable_AH_Employer_Enrollment.pkl')
Other_Transaction_History = pd.read_pickle('Other_Transaction_History.pkl')
df_deals = pd.read_pickle('df_deals.pkl')
wc_policies = pd.read_pickle('wc_policies.pkl')
IPFS_Transaction_History = pd.read_pickle('IPFS_Transaction_History.pkl')
Airtable_Chubb_Statement = pd.read_pickle('Airtable_Chubb_Statement.pkl')
Reconciliation = pd.read_pickle('Reconciliation.pkl')
AH_Bord = pd.read_pickle('AH_Bord.pkl')
stripe_transactions = pd.read_pickle('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\Reconciliation\\stripe_transactions.pkl')
leads = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\Dashboard_Data\\Sales_Data_2021\\leads_all_time.csv')
ipfs_payment_schedule = pd.read_csv('df_payment_schedule.csv')

empty = []
for i in range(1, 13):
    for j in range(2019, 2022):
        empty.append({
                'effective_month' : i,
                'effective_year' : j,
                'value' : 0.0
        })
empty = pd.DataFrame(empty).groupby(['effective_month', 'effective_year']).value.sum().unstack(fill_value=0)

single_customer = False
if(single_customer):
    #policy_id = 'DON LUCA LLC 9983638-71827476 (C66611012) - 01/28/20'
    #chubb_id = 'C66611012'

    policy_id = 'PATCO INC. 8591565-71827563 (C67302916) - 05/28/20'
    chubb_id = 'C67302916'

    #policy_id = 'TEA RUSH CAFE LLC 5542012-71827455 (C66467806) - 10/08/19'
    #chubb_id = 'C66467806'
    #policy_id = 'GILLIES, DOROTHY 1678196-71827504 (C67237158) - 03/22/20'
    #chubb_id = 'C67237158'
    #policy_id = 'CHIN, JOHN 1824471-71827608 (C67368290) - 06/18/20'
    #chubb_id = 'C67368290'

    stripe_id = 'cus_HUWykqjhaPWzsB'
    WCS_Transaction_History = WCS_Transaction_History[WCS_Transaction_History['Contract ID'] == chubb_id]
    WCS_General = WCS_General[WCS_General['Contract ID'] == chubb_id]
    WCS_State_Rating = WCS_State_Rating[WCS_State_Rating['Contract ID'] == chubb_id]
    Airtable_Glow_Payments = Airtable_Glow_Payments[Airtable_Glow_Payments['WC Policy Text [DND]'] == policy_id]
    wc_policies = wc_policies[wc_policies['Contract ID'] == chubb_id]
    Reconciliation = Reconciliation[Reconciliation['wc_policy_text [DND]'] == policy_id]

# A&H used mode and payment plan to build the annual A&H amount because no history for how A&H changed
Airtable_Glow_Payments.loc[:, 'WC Policy Text [DND]'] = Airtable_Glow_Payments['WC Policy Text [DND]'].apply(lambda x: x.split('||')[0])
Airtable_Glow_Payments.loc[:, 'Created (UTC) (from Stripe Payment)'] = Airtable_Glow_Payments['Created (UTC) (from Stripe Payment)'].apply(lambda x: x.split('||')[0])
Airtable_Glow_Payments.loc[:, 'Created (UTC) (from Stripe Payment)'] = pd.to_datetime(Airtable_Glow_Payments['Created (UTC) (from Stripe Payment)'])
Airtable_Glow_Payments.sort_values('Created (UTC) (from Stripe Payment)', inplace=True)

Airtable_Glow_Payments.loc[:, 'Amount Paid (from Customer/IPFS Transactions)'] = Airtable_Glow_Payments['Amount Paid (from Customer/IPFS Transactions)'].replace('', 0).astype(float)
Airtable_Glow_Payments.loc[:, 'WC Premium Paid'] = Airtable_Glow_Payments['WC Premium Paid'].replace('', 0).astype(float)
Airtable_Glow_Payments.loc[:, 'Total Paid'] = Airtable_Glow_Payments['Total Paid'].replace('', 0).astype(float)
Airtable_Glow_Payments.loc[:, 'A&H Premium Paid'] = Airtable_Glow_Payments['A&H Premium Paid'].replace('', 0).astype(float)
Airtable_Glow_Payments.loc[:, 'WC Premium Paid'] = Airtable_Glow_Payments.apply(lambda x: x['WC Premium Paid'] - x['Amount Paid (from Customer/IPFS Transactions)'], axis=1)
Airtable_Glow_Payments.loc[:, 'Total Paid'] = Airtable_Glow_Payments['A&H Premium Paid'] + Airtable_Glow_Payments['WC Premium Paid']

Airtable_Glow_Payments = Airtable_Glow_Payments[Airtable_Glow_Payments['Coverage Start Period'] != ''].copy()

glow_payments_liabilities = Airtable_Glow_Payments.copy()

glow_payments_ah = Airtable_Glow_Payments.copy()

def custom_round(x, base=5.1):
    return round(base * round(x/base), 2)

glow_payments_ah.loc[:, 'A&H Premium Paid'] = glow_payments_ah.apply(lambda x: custom_round(x['A&H Premium Paid']) if 'BRAISE' not in x['WC Policy Text [DND]'] else x['A&H Premium Paid'], axis=1)
glow_payments_ah = glow_payments_ah[glow_payments_ah['A&H Premium Paid'] > 0]

wc_policies.rename(columns={'Id':'WC Policy Text [DND]'}, inplace=True)
wc_policies = wc_policies[['WC Policy Text [DND]', 'Policy Status', 'Payment Plan', 'Writing Company', 'Contract ID']].copy()

policies_west = set(wc_policies[wc_policies['Writing Company'].str.contains('WEST')]['WC Policy Text [DND]'].unique())

ah_premium = pd.DataFrame(glow_payments_ah.groupby('WC Policy Text [DND]')['A&H Premium Paid'].agg(pd.Series.mode)).reset_index()

ah_premium = ah_premium[~ah_premium['WC Policy Text [DND]'].str.contains('ERROR')].copy()
ah_premium.loc[:, 'A&H Premium Paid'] = ah_premium['A&H Premium Paid'].apply(lambda x: x.min() if type(x) == np.ndarray else x).astype(float)
ah_premium['effective_date'] = pd.to_datetime(ah_premium['WC Policy Text [DND]'].str.split('- ').apply(lambda x: x[len(x) - 1]))
ah_premium['effective_year'] = ah_premium['effective_date'].dt.strftime('%Y').astype(int)
ah_premium['effective_month'] = ah_premium['effective_date'].dt.strftime('%m').astype(int)
ah_premium = ah_premium[ah_premium['A&H Premium Paid'] > 0]

policies_ah_paid = set(ah_premium['WC Policy Text [DND]'].unique())

ah_premium_merged = ah_premium.merge(wc_policies, on='WC Policy Text [DND]', how='left')
ah_premium_merged = ah_premium_merged[~ah_premium_merged['Policy Status'].str.contains('Flat') & ~ah_premium_merged['Policy Status'].str.contains('Rewrite')]

def get_annual_ah(payment_plan, payment_amount, policy_status):
    annual_amount = None
    if(payment_plan == 'Annually'):
        annual_amount = payment_amount
    elif(payment_plan == 'Quarterly'):
        annual_amount =  payment_amount * 4
    elif(payment_plan == 'Monthly' or payment_plan == 'Pay Go'):
        annual_amount = payment_amount * 12
    else:
        print('Invalid Payment Plan', payment_plan)
        raise ValueError
    return round(annual_amount, 2)

ah_premium_merged['annual_ah'] = ah_premium_merged.apply(lambda x: get_annual_ah(x['Payment Plan'], x['A&H Premium Paid'], x['Policy Status']), axis=1)

def get_effective_date(contract_id):
    curr_transactions = WCS_Transaction_History[(WCS_Transaction_History['Contract ID'] == contract_id)
                                                & (WCS_Transaction_History.Transaction.str.contains('REGISTER'))]
    if(len(curr_transactions) > 1):
        print('Multiple Registers')
        print(curr_transactions)
        raise ValueError
    elif(len(curr_transactions) == 0):
        print('No Register')
        raise ValueError
    else:
        return curr_transactions['Effective Date'].head(1).item()

def get_effective_cancel_date(contract_id):
    curr_transactions = WCS_Transaction_History[(WCS_Transaction_History['Contract ID'] == contract_id)
                                                & (WCS_Transaction_History.Transaction.str.contains('CANCEL PRO RATA')
                                                    | WCS_Transaction_History.Transaction.str.contains('CANCEL - FLAT'))]
    if(len(curr_transactions) > 1):
        print('Multiple Cancels')
        print(curr_transactions)
        raise ValueError
    elif(len(curr_transactions) == 0):
        return None
    else:
        return curr_transactions['Effective Date'].head(1).item()

ah_premium_merged['effective_cancel_date'] = pd.to_datetime(ah_premium_merged.apply(lambda x: get_effective_cancel_date(x['Contract ID']), axis=1))
ah_premium_merged['cancelled_month'] = round((ah_premium_merged.effective_cancel_date - ah_premium_merged.effective_date) / np.timedelta64(1, 'M'), 0)
ah_premium_merged.loc[:, 'cancelled_month'] = ah_premium_merged.cancelled_month.apply(lambda x: 0 if x < 0 else x)

written_registered_ah = ah_premium_merged.copy()
written_registered_ah = written_registered_ah.groupby(['effective_month', 'effective_year']).annual_ah.sum().unstack(fill_value=0)

written_final_ah_df = ah_premium_merged.copy()
written_final_ah_df.loc[:, 'annual_ah_before_cancel'] = round(written_final_ah_df.annual_ah.copy(), 2)
written_final_ah_df.loc[:, 'annual_ah'] = written_final_ah_df.apply(lambda x: x.annual_ah if(pd.isnull(x.cancelled_month)) else round((x.effective_cancel_date - x.effective_date).days / 365 * x.annual_ah, 2), axis=1)
written_final_ah = written_final_ah_df.groupby(['effective_month', 'effective_year']).annual_ah.sum().unstack(fill_value=0)

written_revenue_ah = written_final_ah.copy() * 0.18
total_due_to_combined = written_final_ah.copy() * 0.82

AH_Bord.loc[:, 'Eff Date'] = pd.to_datetime(AH_Bord['Eff Date'])
AH_Bord['effective_month'] = AH_Bord['Eff Date'].dt.strftime('%m').astype(int)
AH_Bord['effective_year'] = AH_Bord['Eff Date'].dt.strftime('%Y').astype(int)
already_remitted_to_combined = AH_Bord.groupby(['effective_month', 'effective_year'])['Net Remit'].sum().unstack(fill_value=0)
already_remitted_to_combined[2021] = [0] * 12

remaining_due_to_combined = total_due_to_combined - already_remitted_to_combined

adjusted_ah = ah_premium_merged.copy()
adjusted_ah['cancelled_effective_year'] = adjusted_ah['effective_cancel_date'].dt.strftime('%Y')
adjusted_ah['cancelled_effective_month'] = adjusted_ah['effective_cancel_date'].dt.strftime('%m')
adjusted_ah.loc[:, 'annual_ah'] = adjusted_ah.apply(lambda x: 0 if(pd.isnull(x.cancelled_month)) else round((x.effective_cancel_date - x.effective_date).days / 365 * x.annual_ah, 2) - x.annual_ah, axis=1)
adjusted_ah = adjusted_ah.groupby(['effective_month', 'effective_year']).annual_ah.sum().unstack(fill_value=0)

amortized_ah_premium = []
for idx, policy in written_final_ah_df.iterrows():
    policy_effective_date = policy.effective_date
    effective_cancel_date = policy.effective_cancel_date
    amount = policy.annual_ah_before_cancel
    amount_after_cancel = policy.annual_ah
    cancelled_month = policy.cancelled_month
    if(not pd.isnull(policy.cancelled_month)):
        month_to_stop = int(policy.cancelled_month)
    else:
        month_to_stop = 12
    amortized_total = 0
    #print(policy_effective_date, amount)
    for i in range(0, month_to_stop + 1):
        curr_date = policy_effective_date + relativedelta(months = i)
        effective_month = int(curr_date.strftime('%m'))
        effective_year = int(curr_date.strftime('%Y'))
        if(i != 0 and i != month_to_stop):
            amortized_amount = round(amount/12, 2)
            amortized_total += amortized_amount
        elif(i == 0):
            days_in_month = calendar.monthrange(effective_year, effective_month)[1]
            percent_of_month_remaining = round((days_in_month - curr_date.day) / days_in_month, 2)
            amortized_amount = round(round(amount/12, 2) * percent_of_month_remaining, 2)
            amortized_total += amortized_amount
        else:
            amortized_amount = round(amount_after_cancel - round(amortized_total, 2), 2)
            amortized_total += amortized_amount
        amortized_total = round(amortized_total, 2)
        amortized_ah_premium.append({
            'contract_id' : policy['Contract ID'],
            'effective_month' : effective_month,
            'effective_year' : effective_year,
            'premium' : amortized_amount
        })
        #print(curr_date, amortized_amount, amortized_total, percent_of_month_remaining)

amortized_ah_amount = pd.DataFrame(amortized_ah_premium)
earned_premium_ah = amortized_ah_amount.groupby(['effective_month', 'effective_year'], dropna=False).premium.sum().unstack(fill_value=0)
earned_revenue_ah = earned_premium_ah.copy() * 0.18

test_amortized_ah = pd.DataFrame(amortized_ah_amount.groupby(['contract_id']).premium.sum()).reset_index()
test_transactions_ah = pd.DataFrame(written_final_ah_df.groupby(['Contract ID']).annual_ah.sum()).reset_index().rename(columns={'Contract ID':'contract_id'})
test_merge_ah = test_amortized_ah.merge(test_transactions_ah, how='left', on='contract_id')

if(len(test_merge_ah[test_merge_ah.premium - test_merge_ah.annual_ah > 0.0001]) > 0):
    print('Mismatch Total Billable')
    raise ValueError

WCS_Transaction_History.loc[:, 'Effective Date'] = pd.to_datetime(WCS_Transaction_History['Effective Date'])
WCS_Transaction_History['effective_month'] = WCS_Transaction_History['Effective Date'].dt.strftime('%m').astype(int)
WCS_Transaction_History['effective_year'] = WCS_Transaction_History['Effective Date'].dt.strftime('%Y').astype(int)

written_registered_premium_wc = WCS_Transaction_History[WCS_Transaction_History.Transaction == 'REGISTER'].copy().groupby(['effective_month', 'effective_year'], dropna=False).Premium.sum().unstack(fill_value=0)
written_adjusted_premium_wc = WCS_Transaction_History[WCS_Transaction_History.Transaction != 'REGISTER'].copy().groupby(['effective_month', 'effective_year'], dropna=False).Premium.sum().unstack(fill_value=0)
written_final_premium_wc = WCS_Transaction_History.copy().groupby(['effective_month', 'effective_year'], dropna=False).Premium.sum().unstack(fill_value=0)

written_final_premium = written_final_premium_wc.copy() + written_final_ah.copy()

registered_count = WCS_Transaction_History[WCS_Transaction_History.Transaction == 'REGISTER'].copy().groupby(['effective_month', 'effective_year'], dropna=False).size().unstack(fill_value=0)
average_premium = (written_final_premium / registered_count).fillna(0)

if(len(WCS_General[WCS_General.duplicated(subset=['Contract ID'])]) > 0):
    print('Duplicates found in WCS General')
    raise ValueError

contract_id_to_policy_effective = dict(zip(WCS_General['Contract ID'], WCS_General['Effective Date']))
WCS_Transaction_History['policy_effective_date'] = WCS_Transaction_History['Contract ID'].apply(lambda x: get_effective_date(x))
WCS_Transaction_History.loc[:, 'policy_effective_date'] = pd.to_datetime(WCS_Transaction_History.policy_effective_date)

# Need month to start calculation for amortized transactions
WCS_Transaction_History['month_to_start'] = round((WCS_Transaction_History['Effective Date'] - WCS_Transaction_History.policy_effective_date) / np.timedelta64(1, 'M'), 0).astype(int)
WCS_Transaction_History.loc[:, 'month_to_start'] = WCS_Transaction_History.month_to_start.apply(lambda x: 0 if x < 0 else x)

if(len(WCS_Transaction_History[pd.isnull(WCS_Transaction_History.policy_effective_date)]) > 0):
    print('No Policy Effective Date Found')
    raise ValueError

'''
def get_effective_cancel_midterm_date(contract_id):
    curr_transactions = WCS_Transaction_History[(WCS_Transaction_History['Contract ID'] == contract_id)
                                                & (WCS_Transaction_History.Transaction.str.contains('CANCEL PRO RATA'))]
    if(len(curr_transactions) > 1):
        print('Multiple Cancels')
        print(curr_transactions)
        raise ValueError
    elif(len(curr_transactions) == 0):
        return None
    else:
        return curr_transactions['Effective Date'].head(1).item()
'''

WCS_Transaction_History['effective_cancel_date'] = pd.to_datetime(WCS_Transaction_History.apply(lambda x: get_effective_cancel_date(x['Contract ID']), axis=1))
WCS_Transaction_History['cancelled_month'] = round((WCS_Transaction_History.effective_cancel_date - WCS_Transaction_History.policy_effective_date) / np.timedelta64(1, 'M'), 0)
WCS_Transaction_History.loc[:, 'cancelled_month'] = WCS_Transaction_History.cancelled_month.apply(lambda x: 0 if x < 0 else x)

amortized_transactions = []
for idx, transaction in WCS_Transaction_History.iterrows():
    policy_effective_date = transaction.policy_effective_date
    effective_cancel_date = policy.effective_cancel_date
    effective_date = transaction['Effective Date']
    amount = transaction.Premium
    if(not pd.isnull(transaction.cancelled_month)):
        month_to_stop = int(transaction.cancelled_month) + 1
        cancelled = True
    else:
        month_to_stop = 13
        cancelled = False
    month_to_start = transaction.month_to_start
    months_to_run = month_to_stop - month_to_start
    amortized_total = 0
    #print(policy_effective_date, effective_cancel_date, amount, transaction.Transaction, month_to_start, month_to_stop)
    for i in range(month_to_start, month_to_stop):
        curr_date = policy_effective_date + relativedelta(months = i)
        effective_month = int(curr_date.strftime('%m'))
        effective_year = int(curr_date.strftime('%Y'))
        if(i == 0):
            days_in_month = calendar.monthrange(effective_year, effective_month)[1]
            percent_of_month_remaining = round((days_in_month - curr_date.day) / days_in_month, 2)
            amortized_amount = round(round(amount/12, 2) * percent_of_month_remaining, 2)
            amortized_total += amortized_amount
        elif(i != month_to_stop - 1):
            amortized_amount = round(amount/12, 2)
            amortized_total += amortized_amount
        else:
            amortized_amount = round(amount - round(amortized_total, 2), 2)
            amortized_total += amortized_amount
        amortized_total = round(amortized_total, 2)
        amortized_transactions.append({
            'contract_id' : transaction['Contract ID'],
            'effective_month' : effective_month,
            'effective_year' : effective_year,
            'premium' : amortized_amount
        })
        #print(i, '| ', effective_year, effective_month, amortized_amount, amortized_total)

# CHECK FOR EACH CONTRACT ID THAT SUM OF AMORTIZED AMOUNT EQUALS TOTAL BILLABLE
amortized_transactions = pd.DataFrame(amortized_transactions)
earned_premium_wc = amortized_transactions.groupby(['effective_month', 'effective_year'], dropna=False).premium.sum().unstack(fill_value=0)

test_amortized = pd.DataFrame(amortized_transactions.groupby(['contract_id']).premium.sum()).reset_index()
test_transactions = pd.DataFrame(WCS_Transaction_History.groupby(['Contract ID']).Premium.sum()).reset_index().rename(columns={'Contract ID':'contract_id'})
test_merge = test_amortized.merge(test_transactions, how='left', on='contract_id')

if(len(test_merge[test_merge.premium - test_merge.Premium > 0.0001]) > 0):
    print('Mismatch Total Billable')
    raise ValueError

df_deals = df_deals[~pd.isnull(df_deals['properties.policy_effective_date'])].copy()
df_deals.loc[:, 'properties.amount'] = df_deals['properties.amount'].replace('', 0).fillna(0).astype(float)
df_deals.loc[:, 'properties.policy_effective_date'] = pd.to_datetime(df_deals['properties.policy_effective_date'])
df_deals['effective_year'] = df_deals['properties.policy_effective_date'].dt.strftime('%Y').astype(int)
df_deals['effective_month'] = df_deals['properties.policy_effective_date'].dt.strftime('%m').astype(int)

# Revenue ######################################################################

WCS_State_Rating['Commission'] = WCS_State_Rating['Premium'] * (WCS_State_Rating['Comm Percent'] / 100)
WCS_State_Rating.loc[:, 'Effective Date'] = pd.to_datetime(WCS_State_Rating['Effective Date'])
WCS_State_Rating['effective_year'] = WCS_State_Rating['Effective Date'].dt.strftime('%Y').astype(int)
WCS_State_Rating['effective_month'] = WCS_State_Rating['Effective Date'].dt.strftime('%m').astype(int)
WCS_State_Rating['policy_effective_date'] = pd.to_datetime(WCS_State_Rating['Contract ID'].apply(lambda x: get_effective_date(x)))

# Need month to start calculation for amortized transactions
WCS_State_Rating['month_to_start'] = round((WCS_State_Rating['Effective Date'] - WCS_State_Rating.policy_effective_date) / np.timedelta64(1, 'M'), 0).astype(int)
WCS_State_Rating.loc[:, 'month_to_start'] = WCS_State_Rating.month_to_start.apply(lambda x: 0 if x < 0 else x)

WCS_State_Rating['effective_cancel_date'] = pd.to_datetime(WCS_State_Rating.apply(lambda x: get_effective_cancel_date(x['Contract ID']), axis=1))
WCS_State_Rating['cancelled_month'] = round((WCS_State_Rating.effective_cancel_date - WCS_State_Rating.policy_effective_date) / np.timedelta64(1, 'M'), 0)
WCS_State_Rating.loc[:, 'cancelled_month'] = WCS_State_Rating.cancelled_month.apply(lambda x: 0 if x < 0 else x)

written_revenue_wc = WCS_State_Rating.groupby(['effective_month', 'effective_year']).Commission.sum().unstack(fill_value=0)

amortized_revenue = []
for idx, record in WCS_State_Rating.iterrows():
    policy_effective_date = record.policy_effective_date
    effective_date = record['Effective Date']
    effective_cancel_date = record.effective_cancel_date
    amount = record.Commission
    if(not pd.isnull(record.cancelled_month)):
        month_to_stop = int(record.cancelled_month) + 1
        cancelled = True
    else:
        month_to_stop = 13
        cancelled = False
    month_to_start = record.month_to_start
    amortized_total = 0
    #print(policy_effective_date, effective_cancel_date, amount, month_to_start, month_to_stop)
    for i in range(month_to_start, month_to_stop):
        curr_date = policy_effective_date + relativedelta(months = i)
        effective_month = int(curr_date.strftime('%m'))
        effective_year = int(curr_date.strftime('%Y'))
        if(i == 0):
            days_in_month = calendar.monthrange(effective_year, effective_month)[1]
            percent_of_month_remaining = round((days_in_month - curr_date.day) / days_in_month, 2)
            amortized_amount = round(round(amount/12, 2) * percent_of_month_remaining, 2)
            amortized_total += amortized_amount
        elif(i != month_to_stop - 1):
            amortized_amount = round(amount/12, 2)
            amortized_total += amortized_amount
        else:
            amortized_amount = round(amount - round(amortized_total, 2), 2)
            amortized_total += amortized_amount
        amortized_total = round(amortized_total, 2)
        amortized_revenue.append({
            'contract_id' : record['Contract ID'],
            'effective_month' : effective_month,
            'effective_year' : effective_year,
            'commission' : amortized_amount
        })
        #print(i, '| ', effective_year, effective_month, amortized_amount, amortized_total)

amortized_revenue_wc = pd.DataFrame(amortized_revenue)
earned_revenue_wc = amortized_revenue_wc.groupby(['effective_month', 'effective_year']).commission.sum().unstack(fill_value=0)

test_amortized_revenue = pd.DataFrame(amortized_revenue_wc.groupby(['contract_id']).commission.sum()).reset_index()
test_transactions_revenue = pd.DataFrame(WCS_State_Rating.groupby(['Contract ID']).Commission.sum()).reset_index().rename(columns={'Contract ID':'contract_id'})
test_merge_revenue = test_amortized_revenue.merge(test_transactions_revenue, how='left', on='contract_id')

if(len(test_merge_revenue[test_merge_revenue.commission - test_merge_revenue.Commission > 0.0001]) > 0):
    print('Mismatch Total Billable')
    raise ValueError

written_revenue = written_revenue_ah.copy() + written_revenue_wc.copy()
earned_revenue = earned_revenue_ah.copy() + earned_revenue_wc.copy()

if(single_customer):
    written_revenue = written_revenue.reindex_like(empty).fillna(0)
    earned_revenue = earned_revenue.reindex_like(empty).fillna(0)

deferred_revenue_list = []
written_sum_so_far = 0
earned_sum_so_far = 0
for (idxCol, s1), (_, s2) in zip(written_revenue.iteritems(), earned_revenue.iteritems()):
    for (idxRow, curr_written), (_, curr_earned) in zip(s1.iteritems(), s2.iteritems()):
        written_sum_so_far += round(curr_written, 2)
        earned_sum_so_far += round(curr_earned, 2)
        curr_deferred = written_sum_so_far - earned_sum_so_far
        deferred_revenue_list.append({
            'effective_month' : idxRow,
            'effective_year' : idxCol,
            'amount' : curr_deferred
        })

deferred_revenue = pd.DataFrame(deferred_revenue_list).groupby(['effective_month', 'effective_year']).amount.sum().unstack(fill_value=0) * -1

deferred_revenue_subtraction = written_revenue.copy() - earned_revenue.copy()

# New Business #################################################################
new_deals = df_deals[df_deals['properties.dealtype'] == 'newbusiness'].copy()

new_deals_count = new_deals.groupby(['effective_month', 'effective_year'], dropna=False).size().unstack(fill_value=0)
new_deals_amount = new_deals.groupby(['effective_month', 'effective_year'], dropna=False)['properties.amount'].sum().unstack(fill_value=0)

# Renewals #####################################################################

renewal_deals = df_deals[df_deals['properties.dealtype'] == 'Renewal'].copy()

renewed_deals_count = renewal_deals.groupby(['effective_month', 'effective_year'], dropna=False).size().unstack(fill_value=0)
renewed_deals_amount = renewal_deals.groupby(['effective_month', 'effective_year'], dropna=False)['properties.amount'].sum().unstack(fill_value=0)

# Merchant Fees ################################################################

stripe_transactions['available_on_year'] = stripe_transactions.available_on.dt.strftime('%Y').astype(int)
stripe_transactions['available_on_month'] = stripe_transactions.available_on.dt.strftime('%m').astype(int)
stripe_fees_initial = round(stripe_transactions.groupby(['available_on_month', 'available_on_year'], dropna=False).fee.sum().unstack(fill_value=0), 2)
freestyle_credits = stripe_transactions[(stripe_transactions.type == 'adjustment') & (stripe_transactions.reporting_category == 'fee') & pd.isnull(stripe_transactions.source)].copy()
freestyle_credits.loc[:, 'amount'] = round(freestyle_credits.amount * -1, 2)
grouped_freestyle_credits = freestyle_credits.groupby(['available_on_month', 'available_on_year'], dropna=False).amount.sum().unstack(fill_value=0).reindex_like(empty).fillna(0)

stripe_fees = round(stripe_fees_initial + grouped_freestyle_credits, 2)

IPFS_Transaction_History.loc[:, 'Date'] = pd.to_datetime(IPFS_Transaction_History['Date'])
IPFS_Transaction_History['date_year'] = IPFS_Transaction_History['Date'].dt.strftime('%Y').astype(int)
IPFS_Transaction_History['date_month'] = IPFS_Transaction_History['Date'].dt.strftime('%m').astype(int)

IPFS_calendar_fees_transactions = IPFS_Transaction_History[((IPFS_Transaction_History['Transaction'] == 'Late Fee')
                                                            | (IPFS_Transaction_History['Transaction'] == 'Check Charge')
                                                            | (IPFS_Transaction_History['Transaction'] == 'Default Fee')
                                                            | (IPFS_Transaction_History['Transaction'] == 'Waive')
                                                            | (IPFS_Transaction_History['Transaction'] == 'Waive Corr'))].copy()

ipfs_calendar_fees = IPFS_calendar_fees_transactions.groupby(['date_month', 'date_year']).Amount.sum().unstack(fill_value=0)

Reconciliation['policy_effective_date'] = pd.to_datetime(Reconciliation['Policy Effective Date (from WC Policies)'])
Reconciliation = Reconciliation[~pd.isnull(Reconciliation.policy_effective_date)].copy()
Reconciliation['effective_month'] = Reconciliation.policy_effective_date.dt.strftime('%m').astype(int)
Reconciliation['effective_year'] = Reconciliation.policy_effective_date.dt.strftime('%Y').astype(int)

# Amortize IPFS Finance Fees

ipfs_finance_fees_written = Reconciliation.groupby(['effective_month', 'effective_year'])['IPFS Finance Fees'].sum().unstack(fill_value=0)

amortized_ipfs_finance_fees = []
for idx, policy in Reconciliation.iterrows():
    policy_effective_date = policy.policy_effective_date
    amount = policy['IPFS Finance Fees']
    amortized_total = 0
    for i in range(0, 12):
        curr_date = policy_effective_date + relativedelta(months = i)
        effective_month = int(curr_date.strftime('%m'))
        effective_year = int(curr_date.strftime('%Y'))
        if(i != 11):
            amortized_amount = round(amount/12, 2)
            amortized_total += amortized_amount
        else:
            amortized_amount = round(amount - round(amortized_total, 2), 2)
            amortized_total += amortized_amount
        amortized_ipfs_finance_fees.append({
            'Id' : policy['wc_policy_text [DND]'],
            'effective_month' : effective_month,
            'effective_year' : effective_year,
            'finance_fee' : round(amortized_amount, 2)
        })
amortized_ipfs_finance_fees = pd.DataFrame(amortized_ipfs_finance_fees)
prepaid_finance_fees = amortized_ipfs_finance_fees.groupby(['effective_month', 'effective_year']).finance_fee.sum().unstack(fill_value=0)

# Liabilities ##################################################################
due_to_customer = Reconciliation.groupby(['effective_month', 'effective_year'])['Net Owed to Customer'].sum().unstack(fill_value=0)
due_to_ipfs = Reconciliation.groupby(['effective_month', 'effective_year'])['Net Owed to IPFS'].sum().unstack(fill_value=0)
due_to_chubb = Reconciliation.groupby(['effective_month', 'effective_year'])['Net Owed to Chubb'].sum().unstack(fill_value=0)
due_to_glow = Reconciliation.groupby(['effective_month', 'effective_year'])['Net Owed to Glow'].sum().unstack(fill_value=0)

if(single_customer):
    written_revenue = written_revenue.reindex_like(empty).fillna(0)
    due_to_customer = due_to_customer.reindex_like(empty).fillna(0)
    due_to_ipfs = due_to_ipfs.reindex_like(empty).fillna(0)
    due_to_chubb = due_to_chubb.reindex_like(empty).fillna(0)
    due_to_glow = due_to_glow.reindex_like(empty).fillna(0)
    remaining_due_to_combined = remaining_due_to_combined.reindex_like(empty).fillna(0)
    written_final_premium = written_final_premium_wc.copy().reindex_like(empty).fillna(0) + written_final_ah.copy().reindex_like(empty).fillna(0)

def make_cumulative(df):
    curr_df = df.copy()
    last_month = 0.0
    for idxYear, month in curr_df.iteritems():
        for idxMonth, year in month.iteritems():
            curr_df.at[idxMonth, idxYear] = round(year + last_month, 2)
            last_month = curr_df.loc[idxMonth, idxYear]
    return curr_df

glow_payments_liabilities.loc[:, 'Coverage Start Period'] = pd.to_datetime(glow_payments_liabilities['Coverage Start Period'])
glow_payments_liabilities['effective_year'] = glow_payments_liabilities['Coverage Start Period'].dt.strftime('%Y').astype(int)
glow_payments_liabilities['effective_month'] = glow_payments_liabilities['Coverage Start Period'].dt.strftime('%m').astype(int)
glow_payments_liabilities.groupby(['effective_month', 'effective_year'])['Total Paid'].sum().unstack(fill_value=0)

ipfs_payment_schedule = ipfs_payment_schedule[pd.isnull(ipfs_payment_schedule['Paid Date'])].copy()
ipfs_payment_schedule.loc[:, 'Due Date'] = pd.to_datetime(ipfs_payment_schedule['Due Date'])
ipfs_payment_schedule['effective_month'] = ipfs_payment_schedule['Due Date'].dt.strftime('%m').astype(int)
ipfs_payment_schedule['effective_year'] = ipfs_payment_schedule['Due Date'].dt.strftime('%Y').astype(int)
ipfs_payment_schedule.loc[:, 'Amount'] = ipfs_payment_schedule['Amount'].str.replace(',', '').astype(float)

Airtable_Chubb_Statement.loc[:, 'PolicyEfftDate'] = pd.to_datetime(Airtable_Chubb_Statement['PolicyEfftDate'])
Airtable_Chubb_Statement['effective_month'] = Airtable_Chubb_Statement['PolicyEfftDate'].dt.strftime('%m').astype(int)
Airtable_Chubb_Statement['effective_year'] = Airtable_Chubb_Statement['PolicyEfftDate'].dt.strftime('%Y').astype(int)

earned_premium_total = earned_premium_wc + earned_premium_ah
# Need to remove IPFS direct transactions with customer
# Need to remove stripe fees
customer_liability = (round(earned_premium_total, 2) - round(glow_payments_liabilities.groupby(['effective_month', 'effective_year'])['Total Paid'].sum().unstack(fill_value=0).reindex_like(empty).fillna(0), 2))
customer_liability_from_glow_payments = make_cumulative(customer_liability)

due_to_chubb_by_statement = make_cumulative(Airtable_Chubb_Statement.groupby(['effective_month', 'effective_year']).OpenAmt.sum().unstack(fill_value=0)) * -1
ipfs_claimed_owed = make_cumulative(ipfs_payment_schedule.groupby(['effective_month', 'effective_year']).Amount.sum().unstack(fill_value=0)) * -1
cumulative_due_to_combined = make_cumulative(remaining_due_to_combined) * -1

due_to_ipfs = make_cumulative(due_to_ipfs) * -1 # Not on sheet
due_to_chubb = make_cumulative(due_to_chubb) * -1 # Not on sheet
due_to_customer = make_cumulative(due_to_customer) * -1
due_to_glow = make_cumulative(due_to_glow) * -1

IPFS_other_fees_paid = Reconciliation.groupby(['effective_month', 'effective_year'])['Paid IPFS Fees'].sum().unstack(fill_value=0)

grouped_leads = leads.groupby(['policyEffectiveMonth', 'policyEffectiveYear'], dropna=False).total.sum().unstack(fill_value=0)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = get_gsheet_creds()
service = build('sheets', 'v4', credentials=creds)

data = [
    'written_registered_ah',
    'written_registered_premium_wc',
    'adjusted_ah',
    'written_adjusted_premium_wc',
    'written_final_ah',
    'written_final_premium_wc',
    'written_final_premium',
    'earned_premium_ah',
    'earned_premium_wc',
    'written_revenue_ah',
    'written_revenue_wc',
    'earned_revenue_ah',
    'earned_revenue_wc',
    'registered_count',
    'average_premium',
    'new_deals_count',
    'new_deals_amount',
    'renewed_deals_count',
    'renewed_deals_amount',
    'stripe_fees',
    'IPFS_other_fees_paid',
    'ipfs_finance_fees_written',
    'prepaid_finance_fees',
    'due_to_customer',
    'due_to_ipfs',
    'due_to_chubb',
    'due_to_glow',
    'grouped_leads',
    'deferred_revenue',
    'deferred_revenue_subtraction',
    'already_remitted_to_combined',
    'total_due_to_combined',
    'cumulative_due_to_combined',
    'remaining_due_to_combined',
    'due_to_chubb_by_statement',
    'ipfs_claimed_owed',
    'customer_liability_from_glow_payments',
    'ipfs_calendar_fees'
]

average_premium.replace(np.inf, 0, inplace=True)

if(not fresh_data):
    print('Warning - Pushing without fresh data')
    SPREADSHEET_ID = '19RrDZ9tJLQ9rbc6FoKWUhGCGaH1nLvK_HuZKl-g9RG0'
    rows = 1
    for table in data:
        time.sleep(1.5)
        curr_table = eval(table).copy()
        curr_table.fillna(0, inplace=True)
        range = 'Data!A' + str(rows) + ':Z'
        request = service.spreadsheets().values().clear(spreadsheetId=SPREADSHEET_ID, range=range, body={})
        response = request.execute()
        values = curr_table.reset_index().values.tolist()
        values.insert(0, curr_table.reset_index().columns.values.tolist())
        values.insert(0, [table])
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=range,
            valueInputOption='USER_ENTERED', body=body).execute()
        print(range + ': {0} cells updated.'.format(result.get('updatedCells')))
        rows += 16
