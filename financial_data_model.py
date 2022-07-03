import pandas as pd
import numpy as np
import time
import os.path
from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from dateutil.relativedelta import relativedelta
from airtable import Airtable
import requests
import json
import sys

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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

push_to_sheet = True
def upload_data(sheet_name, data):
    if(push_to_sheet):
        range = sheet_name + '!A:Z'
        request = service.spreadsheets().values().clear(spreadsheetId=SPREADSHEET_ID, range=range, body={})
        response = request.execute()
        values = data.values.tolist()
        values.insert(0, data.columns.values.tolist())
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=range,
            valueInputOption='USER_ENTERED', body=body).execute()
        print(range + ': {0} cells updated.'.format(result.get('updatedCells')))
    else:
        print('Not uploaded to', sheet_name, '- Data push disabled')

def make_cumulative(df):
    curr_df = df.copy()
    last_month = 0.0
    for idxYear, month in curr_df.iteritems():
        for idxMonth, year in month.iteritems():
            curr_df.at[idxMonth, idxYear] = round(year + last_month, 2)
            last_month = curr_df.loc[idxMonth, idxYear]
    return curr_df

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
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "2053860"
                    },
                    {
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "1109324"
                    },
                    {
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "3660711"
                    }
            ]
          },
          {
            "filters":[
                    {
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "2053860"
                    },
                    {
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "1109324"
                    },
                    {
                        "propertyName": "dealstage",
                        "operator": "NEQ",
                        "value": "3660711"
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
                                        "propertyName": "dealstage",
                                        "operator": "NEQ",
                                        "value": "2053860"
                                    },
                                    {
                                        "propertyName": "dealstage",
                                        "operator": "NEQ",
                                        "value": "1109324"
                                    },
                                    {
                                        "propertyName": "dealstage",
                                        "operator": "NEQ",
                                        "value": "3660711"
                                    }
                            ]
                          },
                          {
                            "filters":[
                                    {
                                        "propertyName": "dealstage",
                                        "operator": "NEQ",
                                        "value": "2053860"
                                    },
                                    {
                                        "propertyName": "dealstage",
                                        "operator": "NEQ",
                                        "value": "1109324"
                                    },
                                    {
                                        "propertyName": "dealstage",
                                        "operator": "NEQ",
                                        "value": "3660711"
                                    }
                            ]
                          }
                        ]
                    }'''
                response = json.loads(requests.request("POST", url, headers=headers, data=data).text)
                deals = pd.json_normalize(response['results'])[['properties.policy_effective_date', 'properties.amount', 'properties.dealstage', 'properties.dealtype', 'properties.hs_object_id', 'properties.pipeline', 'createdAt', 'properties.last_deal_stage_before_lost', 'properties.closedate', 'properties.wcirb_id', 'properties.hubspot_owner_id']]
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

    # Fix mapping like pre_process_deals
    for idx, row in pd.read_json('./dealPipelines.json').iterrows():
        deal_stage_mapping = deal_stage_mapping.append(pd.DataFrame(row['stages']))
        pipeline_mapping[row['id']] = row['label']

    deal_stage_mapping = dict(zip(deal_stage_mapping.id, deal_stage_mapping.label))

    df_deals['dealstage'] = df_deals['properties.dealstage'].map(lambda x: deal_stage_mapping.get(x, x))
    df_deals.to_pickle('df_deals.pkl')
    ################################################################################
    airtable = Airtable('appfIwwat0W37vtdU', 'Cash Transactions', airtable_credentials)
    cash_transactions = pd.json_normalize(airtable.get_all(view='Balance With Trust')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    cash_transactions = remove_fields(cash_transactions)
    
    airtable = Airtable('appfIwwat0W37vtdU', 'LLC/INC Balance History', airtable_credentials)
    checking_balance_history = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    checking_balance_history = remove_fields(checking_balance_history)
    
    airtable = Airtable('appfIwwat0W37vtdU', 'UpWork', airtable_credentials)
    upwork_data = pd.json_normalize(airtable.get_all(view='Export')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    upwork_data = remove_fields(upwork_data)

    airtable = Airtable('appfIwwat0W37vtdU', 'CC', airtable_credentials)
    credit_card = pd.json_normalize(airtable.get_all(view='Export')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    credit_card = remove_fields(credit_card)

    airtable = Airtable('appfIwwat0W37vtdU', 'Bills', airtable_credentials)
    bills = pd.json_normalize(airtable.get_all(view='Export')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    bills = remove_fields(bills)

    airtable = Airtable('appfIwwat0W37vtdU', 'Fees', airtable_credentials)
    ledger_fees = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    ledger_fees = remove_fields(ledger_fees)

    airtable = Airtable('appfIwwat0W37vtdU', 'Chart of Accounts', airtable_credentials)
    chart_of_accounts = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    chart_of_accounts = remove_fields(chart_of_accounts)
    
    airtable = Airtable('app9RJbzpT3jQFn1A', 'SVB - Transactions', airtable_credentials)
    svb_expenses = pd.json_normalize(airtable.get_all(view='Expense Export')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    svb_expenses = remove_fields(svb_expenses)

    airtable = Airtable('appHj9Yo9OoPWpgLQ', 'Premium Audit', airtable_credentials)
    Premium_Audit = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Premium_Audit = remove_fields(Premium_Audit)

    print('Getting WC Policies')
    wc_policy_base = 'appjFfjDxAogLiXWI'
    airtable = Airtable(wc_policy_base, 'Policies', airtable_credentials)
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

    print('Getting Other Carrier Transactions')
    airtable = Airtable(wc_policy_base, 'Other Carrier Transactions', airtable_credentials)
    Other_Transaction_History = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Other_Transaction_History = remove_fields(Other_Transaction_History)

    print('Getting Glow Payments')
    billing_base = 'appzMh02XqmJGbjlo'
    airtable = Airtable(billing_base, 'Glow Payments', airtable_credentials)
    Airtable_Glow_Payments = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: '||'.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Glow_Payments = remove_fields(Airtable_Glow_Payments)

    print('Getting IPFS WC Policies')
    ipfs_base = 'appvudnVFWtHNxmQQ'
    airtable = Airtable(ipfs_base, 'WC Policies', airtable_credentials)
    Airtable_IPFS_Linking = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_IPFS_Linking = remove_fields(Airtable_IPFS_Linking)

    print('Getting IPFS Transaction History')
    ipfs_base = 'appvudnVFWtHNxmQQ'
    airtable = Airtable(ipfs_base, 'IPFS Transaction History', airtable_credentials)
    IPFS_Transaction_History = pd.json_normalize(airtable.get_all(view='Export [DND]')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    IPFS_Transaction_History = remove_fields(IPFS_Transaction_History)

    arrp_base = 'app9RJbzpT3jQFn1A'
    airtable = Airtable(arrp_base, '[WC] Chubb RP', airtable_credentials)
    chubb_rp = pd.json_normalize(airtable.get_all(view='Export')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    chubb_rp = remove_fields(chubb_rp)

    airtable = Airtable(arrp_base, '[WC] Chubb AP', airtable_credentials)
    chubb_ap = pd.json_normalize(airtable.get_all(view='Export')).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    chubb_ap = remove_fields(chubb_ap)

    ledger_base = 'appfIwwat0W37vtdU'
    print('Getting Expenses')
    airtable = Airtable(ledger_base, 'Intact Transactions', airtable_credentials)
    Airtable_Expenses = pd.json_normalize(airtable.get_all(view='Export')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Expenses = remove_fields(Airtable_Expenses)

    print('Getting BS Expenses')
    airtable = Airtable(ledger_base, 'Intact Transactions', airtable_credentials)
    Airtable_BS = pd.json_normalize(airtable.get_all(view='BS Export')).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_BS = remove_fields(Airtable_BS)

    print('Getting Payroll')
    airtable = Airtable(ledger_base, 'Payroll', airtable_credentials)
    Airtable_Payroll = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ', '.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
    Airtable_Payroll = remove_fields(Airtable_Payroll)
    ################################################################################

    #Reconciliation.to_pickle('Reconciliation.pkl')
    upwork_data.to_pickle('upwork_data.pkl')
    credit_card.to_pickle('credit_card.pkl')
    bills.to_pickle('bills.pkl')
    ledger_fees.to_pickle('ledger_fees.pkl')
    chart_of_accounts.to_pickle('chart_of_accounts.pkl')
    svb_expenses.to_pickle('svb_expenses.pkl')
    Airtable_IPFS_Linking.to_pickle('Airtable_IPFS_Linking.pkl')
    Premium_Audit.to_pickle('Premium_Audit.pkl')
    WCS_Transaction_History.to_pickle('WCS_Transaction_History.pkl')
    WCS_State_Rating.to_pickle('WCS_State_Rating.pkl')
    Airtable_Glow_Payments.to_pickle('Airtable_Glow_Payments.pkl')
    Other_Transaction_History.to_pickle('Other_Transaction_History.pkl')
    wc_policies.to_pickle('wc_policies.pkl')
    IPFS_Transaction_History.to_pickle('IPFS_Transaction_History.pkl')
    chubb_rp.to_pickle('chubb_rp.pkl')
    chubb_ap.to_pickle('chubb_ap.pkl')
    Airtable_Payroll.to_pickle('Airtable_Payroll.pkl')
    Airtable_Expenses.to_pickle('Airtable_Expenses.pkl')
    Airtable_BS.to_pickle('Airtable_BS.pkl')
    checking_balance_history.to_pickle('checking_balance_history.pkl')
    cash_transactions.to_pickle('cash_transactions.pkl')

upwork_data = pd.read_pickle('upwork_data.pkl')
credit_card = pd.read_pickle('credit_card.pkl')
bills = pd.read_pickle('bills.pkl')
ledger_fees = pd.read_pickle('ledger_fees.pkl')
chart_of_accounts = pd.read_pickle('chart_of_accounts.pkl')
svb_expenses = pd.read_pickle('svb_expenses.pkl')
Airtable_Payroll = pd.read_pickle('Airtable_Payroll.pkl')
Airtable_Expenses = pd.read_pickle('Airtable_Expenses.pkl')
Airtable_BS = pd.read_pickle('Airtable_BS.pkl')
checking_balance_history = pd.read_pickle('checking_balance_history.pkl')
Airtable_IPFS_Linking = pd.read_pickle('Airtable_IPFS_Linking.pkl')
Premium_Audit = pd.read_pickle('Premium_Audit.pkl')
df_deals = pd.read_pickle('df_deals.pkl')
WCS_Transaction_History = pd.read_pickle('WCS_Transaction_History.pkl')
WCS_State_Rating = pd.read_pickle('WCS_State_Rating.pkl')
Airtable_Glow_Payments = pd.read_pickle('Airtable_Glow_Payments.pkl')
Other_Transaction_History = pd.read_pickle('Other_Transaction_History.pkl')
wc_policies = pd.read_pickle('wc_policies.pkl')
IPFS_Transaction_History = pd.read_pickle('IPFS_Transaction_History.pkl')
chubb_rp = pd.read_pickle('chubb_rp.pkl')
chubb_ap = pd.read_pickle('chubb_ap.pkl')
cash_transactions = pd.read_pickle('cash_transactions.pkl')

bad_transactions = set([
    'recMX4wedCUDPezPm',
    'recv40FZ5978cqZ58'
])

cols = []
count = 1
for column in Other_Transaction_History.columns:
    if column == 'id':
        cols.append(f'id_{count}')
        count+=1
        continue
    cols.append(column)

Other_Transaction_History.columns = cols

Other_Transaction_History = Other_Transaction_History[~Other_Transaction_History['id_1'].isin(bad_transactions)].copy()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = get_gsheet_creds()
service = build('sheets', 'v4', credentials=creds)
SPREADSHEET_ID = '1r8d_vKNAdmYFWEZ9NSuFJhZqux8pxdKXN7X5NzNtVvE'

cash_transactions.loc[cash_transactions.Account == 'Glow LLC - Return Commission Due to Trust', 'Amount'] *= -1
cash_transactions.loc[cash_transactions.Account == 'Glow LLC - Stripe Fees Due to Trust'] *= -1
cash_transactions.loc[:, 'Date'] = pd.to_datetime(cash_transactions.Date).dt.strftime('%Y-%m')

cash_transactions.groupby(['Date', 'Account']).Amount.sum().reset_index()
glow_balance_with_trust = cash_transactions.groupby(['Date', 'Account']).Amount.sum().reset_index().copy()

upload_data('Data_Glow_Balance_With_Trust', glow_balance_with_trust.fillna(''))

if(len(checking_balance_history[checking_balance_history.duplicated(subset=['Account Number', 'Date'])]) > 0):
    print('Bad Balance data')
    raise ValueError

checking_balance_history.loc[:, 'Date'] = pd.to_datetime(checking_balance_history.Date)
checking_balance_history['Month-Year'] = checking_balance_history.Date.dt.strftime('%Y-%m')
checking_balance_history = checking_balance_history.sort_values('Date').reset_index(drop=True)
all_month_years = checking_balance_history['Month-Year'].unique()

close_1432 = checking_balance_history[(checking_balance_history['Account Number'] == '3303011432')]['Closing Ledger Balance'].head(1).item()
close_4592 = checking_balance_history[(checking_balance_history['Account Number'] == '3301374592')]['Closing Ledger Balance'].head(1).item()

closing_balances = []
for month_year in all_month_years:
    df_close_1432 = checking_balance_history[(checking_balance_history['Account Number'] == '3303011432') & (checking_balance_history['Month-Year'] == month_year)]['Closing Ledger Balance']
    df_close_4592 = checking_balance_history[(checking_balance_history['Account Number'] == '3301374592') & (checking_balance_history['Month-Year'] == month_year)]['Closing Ledger Balance']
    
    if(len(df_close_1432) > 0):
        close_1432 = df_close_1432.tail(1).item()
    else:
        close_1432 = close_1432
    
    if(len(df_close_4592) > 0):
        close_4592 = df_close_4592.tail(1).item()
    else:
        close_4592 = close_4592

    closing_balances.append({
        'Date' : month_year,
        'Amount' : close_1432 + close_4592
    })

df_closing_balances = pd.DataFrame(closing_balances)
upload_data('Checking_Closing_Balances', df_closing_balances.fillna(''))

new_data = []

upwork_data['year'] = pd.to_datetime(upwork_data.Date).dt.strftime('%Y')
upwork_data['month'] = pd.to_datetime(upwork_data.Date).dt.strftime('%m')
upwork_data.rename(columns={'Financial Model Mapping (from Chart of Accounts 2)':'Financial Model Mapping'}, inplace=True)
grouped_upwork_data = upwork_data.groupby(['year', 'month', 'Financial Model Mapping', 'Department']).Amount.sum().reset_index()
grouped_upwork_data.loc[:, 'Amount'] = grouped_upwork_data.Amount * -1

new_data += grouped_upwork_data.to_dict('records')

credit_card['year'] = pd.to_datetime(credit_card['Transaction date']).dt.strftime('%Y')
credit_card['month'] = pd.to_datetime(credit_card['Transaction date']).dt.strftime('%m')
credit_card.rename(columns={'Financial Model Mapping (from Chart of Accounts)':'Financial Model Mapping'}, inplace=True)
grouped_credit_card = credit_card.groupby(['year', 'month', 'Financial Model Mapping', 'Department']).Amount.sum().reset_index()
grouped_credit_card.loc[:, 'Amount'] = grouped_credit_card.Amount * -1

new_data += grouped_credit_card.to_dict('records')

bills['year'] = pd.to_datetime(bills['Date']).dt.strftime('%Y')
bills['month'] = pd.to_datetime(bills['Date']).dt.strftime('%m')
bills.rename(columns={'Financial Model Mapping (from Chart of Accounts)':'Financial Model Mapping'}, inplace=True)
grouped_bills = bills.groupby(['year', 'month', 'Financial Model Mapping', 'Department']).Amount.sum().reset_index()
grouped_bills.loc[:, 'Amount'] = grouped_bills.Amount

new_data += grouped_bills.to_dict('records')

ledger_fees['year'] = pd.to_datetime(ledger_fees['Date']).dt.strftime('%Y')
ledger_fees['month'] = pd.to_datetime(ledger_fees['Date']).dt.strftime('%m')
ledger_fees['Department'] = 'REV'
ledger_fees.rename(columns={'Account':'Financial Model Mapping', 'Fee':'Amount'}, inplace=True)
grouped_ledger_fees = ledger_fees.groupby(['year', 'month', 'Financial Model Mapping']).Amount.sum().reset_index()

new_data += grouped_ledger_fees.to_dict('records')

chart_of_accounts_mapping = dict(zip(chart_of_accounts['Name'], chart_of_accounts['Financial Model Mapping']))

svb_expenses['year'] = pd.to_datetime(svb_expenses['Date']).dt.strftime('%Y')
svb_expenses['month'] = pd.to_datetime(svb_expenses['Date']).dt.strftime('%m')
svb_expenses.loc[:, 'Debit Amount'] = svb_expenses['Debit Amount'].replace('', 0, regex=False).astype(float)
svb_expenses.loc[:, 'Credit Amount'] = svb_expenses['Credit Amount'].replace('', 0, regex=False).astype(float)
svb_expenses['Amount'] = svb_expenses['Debit Amount'] - svb_expenses['Credit Amount']
svb_expenses['Financial Model Mapping'] = svb_expenses['Chart of Accounts (Expenses)'].apply(lambda x: chart_of_accounts_mapping[x])
svb_expenses.rename(columns={'Department (Expenses)':'Department'}, inplace=True)
grouped_svb_expenses = svb_expenses.groupby(['year', 'month', 'Financial Model Mapping', 'Department']).Amount.sum().reset_index()

new_data += grouped_svb_expenses.to_dict('records')
df_recent_expenses = pd.DataFrame(new_data)

upload_data('Data_Recent_Expenses', df_recent_expenses.fillna(''))

Other_Transaction_History = Other_Transaction_History[Other_Transaction_History['Effective Date'] != ''].copy()
################################################################

url = "https://api.hubapi.com/owners/v2/owners?hapikey=" + hubspot_credentials + "&includeInactive=true"
r = requests.get(url = url)
df_owners = pd.json_normalize(json.loads(r.text))
df_owners['name'] = df_owners['firstName'] + ' ' + df_owners['lastName']
deal_owner_mapping = dict(zip(df_owners.ownerId, df_owners.name))
deal_owner_mapping = {str(k):v for k,v in deal_owner_mapping.items()}

df_deals.loc[:, 'properties.hubspot_owner_id'] = df_deals['properties.hubspot_owner_id'].apply(lambda x: deal_owner_mapping.get(x, x))

wc_policies.loc[:, 'Policy Effective Date'] = pd.to_datetime(wc_policies['Policy Effective Date'])
#wc_policies = wc_policies[wc_policies['Policy Status'] != 'Cancelled (Flat)'].copy()
wc_policies = wc_policies[wc_policies['Policy Status'] != 'Cancelled (Rewrite)'].copy()

WCS_Transaction_History.drop(columns=['id'], inplace=True) # duplicate columns from airtable
policies_with_loans = set(Airtable_IPFS_Linking.Id.values)
wc_policies['has_loan'] = wc_policies.Id.apply(lambda x: 'True' if x in policies_with_loans else 'False')

def get_customer_effective_date(x):
    return x.min()

customer_effective_dates = wc_policies.groupby(['Bureau Id']).apply(lambda x: get_customer_effective_date(x['Policy Effective Date'])).copy()
customer_effective_dates = pd.DataFrame(customer_effective_dates).reset_index().rename(columns={0:'effective_date'}).copy()
risk_group_to_effective_date = dict(zip(customer_effective_dates['Bureau Id'], customer_effective_dates.effective_date))
wc_policies['customer_effective_date'] = wc_policies['Bureau Id'].apply(lambda x: risk_group_to_effective_date.get(x, None))
wc_policies['cohort'] = wc_policies['customer_effective_date'].dt.strftime('%Y-%m')

policy_data = []
for idx, policy in wc_policies.iterrows():
    product_line = policy['Product Line']

    if(policy.has_loan == 'True'):
        billing = 'Financed'
    elif(policy.Carrier == 'Chubb'):
        billing = 'Agency'
    else:
        billing = 'Direct'

    if('A&H' in product_line):
        policy_data.append({
            'Policy' : policy.id + 'ah',
            'Carrier' : 'Combined',
            'Product' : 'A&H',
            'Producer' : policy['Owner (from HubSpot Deals)'],
            'State' : policy['State'],
            'Governing Class Code' : policy['Governing Class Code'],
            'Cohort' : policy.cohort,
            'Billing Type' : billing,
            'Policy Effective Date' : policy['Policy Effective Date'].strftime('%m/%d/%Y'),
            'Policy Expiration Date' : policy['Policy Expiration Date'],
            'Policy Status' : policy['Policy Status'],
            'Contract ID' : policy['Contract ID'],
            'Risk Group' : policy['Bureau Id'],
            'Contract Type' : policy['Contract Type'],
        })
        
        product_line = "Workers' Comp"

    policy_data.append({
        'Policy' : policy.id,
        'Carrier' : policy.Carrier,
        'Product' : product_line,
        'Producer' : policy['Owner (from HubSpot Deals)'],
        'State' : policy['State'],
        'Governing Class Code' : policy['Governing Class Code'],
        'Cohort' : policy.cohort,
        'Billing Type' : billing,
        'Policy Effective Date' : policy['Policy Effective Date'].strftime('%m/%d/%Y'),
        'Policy Expiration Date' : policy['Policy Expiration Date'],
        'Policy Status' : policy['Policy Status'],
        'Contract ID' : policy['Contract ID'],
        'Risk Group' : policy['Bureau Id'],
        'Contract Type' : policy['Contract Type'],
    })

df_policy_data = pd.DataFrame(policy_data)

airtable = Airtable('appfIwwat0W37vtdU', 'Policies', airtable_credentials)
print("===========================")
print('\nInserting', len(policy_data), 'records')
#airtable.batch_insert(policy_data, typecast=True)

################################################################

policies = pd.json_normalize(airtable.get_all()).apply(lambda x: x.apply(lambda y: ','.join([str(i) for i in y]) if type(y) == type([]) else y), axis=1).fillna('')
policies = remove_fields(policies)

def get_schedule(effective_date, policy_id, policy_length_months):
    curr_records = []
    for i in range (0, int(policy_length_months)):
        curr_records.append({
            'Policy' : policy_id,
            'Coverage Start' : effective_date + relativedelta(months=i),
            'Coverage End' : effective_date + relativedelta(months=i+1) - relativedelta(days=1),
            'Date' : effective_date + relativedelta(months=i),
            'Premium' : 0.0,
            'Revenue' : 0.0,
            'Remaining Periods' : 12 - i,
        })
    curr_df = pd.DataFrame(curr_records)
    return curr_df

Other_Transaction_History.loc[:, 'Premium']= Other_Transaction_History['Premium'].replace('', 0).astype(float)
Other_Transaction_History.loc[:, 'Process Date'] = pd.to_datetime(Other_Transaction_History['Process Date'])
Other_Transaction_History.loc[:, 'Effective Date'] = pd.to_datetime(Other_Transaction_History['Effective Date'])
WCS_Transaction_History.loc[:, 'Process Date'] = pd.to_datetime(WCS_Transaction_History['Process Date'])
WCS_Transaction_History.loc[:, 'Effective Date'] = pd.to_datetime(WCS_Transaction_History['Effective Date'])

policies.loc[:, 'Policy Effective Date'] = pd.to_datetime(policies['Policy Effective Date'])
policies.loc[:, 'Policy Expiration Date'] = pd.to_datetime(policies['Policy Expiration Date'])

cancel_transcations = set([
    'CANCEL PRO RATA',
    'CANCEL - FLAT',
    'CANCEL - MIDTERM'
])

def get_cancel_process_date(carrier, contract_id, policy_id, policy_status, expiration_date, product):
    cancel_date = expiration_date

    if('Cancelled' in policy_status):
        if(carrier == 'Combined'):
            curr_transactions = WCS_Transaction_History[(WCS_Transaction_History['Contract ID'] == contract_id) & (WCS_Transaction_History.Transaction.isin(cancel_transcations))].copy()
        elif(carrier == 'Chubb'):
            curr_transactions = WCS_Transaction_History[(WCS_Transaction_History['Contract ID'] == contract_id) & (WCS_Transaction_History.Transaction.isin(cancel_transcations))].copy()
        else:
            curr_transactions = Other_Transaction_History[(Other_Transaction_History['Policies'] == policy_id) & (Other_Transaction_History.Transaction.isin(cancel_transcations))].copy()
        
        if(len(curr_transactions) == 0):
            print('No cancel transaction found for', policy_id, contract_id, carrier)
        elif(len(curr_transactions) > 0):
            #print('Cancel transaction found for', policy_id, contract_id, carrier)
            cancel_date = curr_transactions['Process Date'].head(1).item()
    
    return cancel_date

policies.loc[:, 'Policy Expiration Date'] = policies.apply(lambda x: get_cancel_process_date(x['Carrier'], x['Contract ID'], x['Policy'], x['Policy Status'], x['Policy Expiration Date'], x['Product']), axis=1)
policies['policy_length_months'] = (12 * (policies['Policy Expiration Date'].dt.year - policies['Policy Effective Date'].dt.year) + (policies['Policy Expiration Date'].dt.month - policies['Policy Effective Date'].dt.month)).apply(lambda x: 1 if x < 1 else x)
wc_policies.loc[:, 'Other Carriers Comm Amount'] = wc_policies['Other Carriers Comm Amount'].replace('', 0).astype(float)

premium_transaction_data = []
earned_transaction_data = []
for idx, policy in policies.iterrows():
    if(policy.Product == 'A&H'):
        total_amount = wc_policies[(wc_policies['id'] == policy['Policy'].strip('ah')) & (wc_policies['Policy Status'] != 'Cancelled (Rewrite)')]['A&H Total Billable'].head(1).sum()

        if(total_amount == ''):
            total_amount = 0
        
        ah_transactions = []

        ah_transactions.append({
            'Policy' : policy.id,
            'Effective Date' : policy['Policy Effective Date'],
            'Process Date' : policy['Policy Effective Date'],
            'Amount' : total_amount,
            'Type' : 'REGISTER'
        })

        if(policy['Policy Status'] == 'Cancelled (Midterm)'):
            ah_transactions.append({
                'Policy' : policy.id,
                'Effective Date' : policy['Policy Expiration Date'],
                'Process Date' : policy['Policy Expiration Date'],
                'Amount' : 0.0,
                'Type' : 'CANCEL - MIDTERM'
            })
        elif(policy['Policy Status'] == 'Cancelled (Flat)'):
            ah_transactions.append({
                'Policy' : policy.id,
                'Effective Date' : policy['Policy Effective Date'],
                'Process Date' : policy['Policy Expiration Date'],
                'Amount' : 0.0,
                'Type' : 'CANCEL - FLAT'
            })

        curr_transactions = pd.DataFrame(ah_transactions).rename(columns={'Type':'Transaction', 'Amount':'Premium'})
        final_revenue = round(curr_transactions.Premium.sum() * 0.18, 2) if type(curr_transactions.Premium.sum()) != str else 0
    elif(policy.Carrier == 'Chubb'):
        curr_transactions = WCS_Transaction_History[(WCS_Transaction_History['Contract ID'] == policy['Contract ID']) & (WCS_Transaction_History.Premium != 0)].copy()
        final_revenue = WCS_State_Rating[(WCS_State_Rating['Contract ID'] == policy['Contract ID'])].Commission.sum()
    else:
        curr_transactions = Other_Transaction_History[Other_Transaction_History['Policies'] == policy['Policy']].copy()
        if('recqa4M2fGRWIeHVl' not in policy['Policy']):
            final_revenue = wc_policies[(wc_policies['id'] == policy['Policy']) & (wc_policies['Policy Status'] != 'Cancelled (Rewrite)')]['Other Carriers Comm Amount'].head(1).item()
        else:
            final_revenue = 0

    if(len(curr_transactions) == 0):
        #print(policy.Product, '|', policy['Policy Status'], '| No Transactions |', policy['Contract ID'])
        continue

    final_rev_pct = final_revenue / curr_transactions.Premium.sum() if curr_transactions.Premium.sum() != 0 else 0

    curr_transactions = curr_transactions.sort_values('Process Date').reset_index(drop=True)

    curr_transactions_no_audit = curr_transactions[~(curr_transactions.Transaction.str.contains('AUDIT') | curr_transactions.Transaction.str.contains('FLAT'))].copy()
    curr_transactions_audits = curr_transactions[(curr_transactions.Transaction.str.contains('AUDIT') | curr_transactions.Transaction.str.contains('FLAT'))].copy()

    schedule = get_schedule(policy['Policy Effective Date'], policy.id, policy['policy_length_months'])

    last_period = schedule['Remaining Periods'].min()

    for idx, period in schedule.iterrows():
        if(period['Remaining Periods'] != last_period or len(curr_transactions_audits) > 0):
            premium_earned_so_far = schedule.Premium.sum()
            premium_left_to_earn = curr_transactions_no_audit[curr_transactions_no_audit['Process Date'] < period['Coverage End']].Premium.sum() - premium_earned_so_far
            earned_premium = premium_left_to_earn / period['Remaining Periods']

            schedule.at[idx, 'Premium'] = round(earned_premium, 2)
            schedule.at[idx, 'Revenue'] = round(earned_premium * final_rev_pct, 2)
        else:
            schedule.at[idx, 'Premium'] = round(curr_transactions_no_audit.Premium.sum() - schedule.Premium.sum(), 2)
            schedule.at[idx, 'Revenue'] = round(final_revenue - schedule.Revenue.sum(), 2)

    earned_audits = []
    for idx, audit in curr_transactions_audits.iterrows():
        if(idx == curr_transactions_audits.index.max()):
            premium = round(curr_transactions.Premium.sum() - schedule.Premium.sum(), 2)
            revenue = round(final_revenue - schedule.Revenue.sum(), 2)
        else:
            premium = round(audit.Premium, 2)
            revenue = round(audit.Premium * final_rev_pct, 2)

        earned_audits = [{
            'Policy' : policy.id,
            'Coverage Start' : audit['Effective Date'],
            'Coverage End' : policy['Policy Expiration Date'],
            'Date' : audit['Process Date'],
            'Premium' : premium,
            'Revenue' : revenue,
            'Remaining Periods' : 0,
        }]

        schedule = pd.concat([schedule, pd.DataFrame(earned_audits)]).reset_index(drop=True)

    if(False and policy.Product == 'A&H'):
        print(policy['Policy Status'], "=======================", schedule.Premium.sum(), schedule.Revenue.sum(), final_rev_pct)
        print(schedule)
        print("-----------------------", curr_transactions.Premium.sum(), final_revenue)
        print(curr_transactions)

    if((abs(schedule.Premium.sum() - curr_transactions.Premium.sum()) > 0.01
            or abs(schedule.Revenue.sum() - final_revenue) > 0.01)):
        print("Validation error for", policy['Contract ID'])
        raise ValueError

    earned_transaction_data += schedule.to_dict('records')
    curr_transactions['rev'] = 0.0

    for idx, row in curr_transactions.iterrows():
        if(idx == curr_transactions.index.max()):
            revenue = round(final_revenue - curr_transactions.rev.sum(), 2)
        else:
            revenue = round(row.Premium * final_rev_pct, 2)
        
        curr_transactions.at[idx, 'rev'] = revenue
        premium_transaction_data.append({
            'Policy' : policy.id,
            'Effective Date' : row['Effective Date'],
            'Process Date' : row['Process Date'],
            'Amount' : row.Premium,
            'Revenue' : revenue,
            'Type' : row.Transaction,
        })

    if(abs(final_revenue - curr_transactions.rev.sum()) > 0.01):
        print("Validation error for", policy['Contract ID'])
        raise ValueError

df_premium_transactions = pd.DataFrame(premium_transaction_data)
df_earned_transactions = pd.DataFrame(earned_transaction_data).drop(columns=['Remaining Periods'])

# WCS overwrites registers with rewrites - for our purposes we will consider them the same
df_premium_transactions.loc[:, 'Type'] = df_premium_transactions.Type.apply(lambda x: 'REGISTER' if x == 'REWRITE' else x)

register_transasctions_count = df_premium_transactions[df_premium_transactions.Type == 'REGISTER'].groupby(['Policy']).size().reset_index(name='register_count')

for policy in register_transasctions_count[register_transasctions_count.register_count != 1].Policy.unique().tolist():
    curr_transactions = df_premium_transactions[(df_premium_transactions.Policy == policy) & (df_premium_transactions.Type == 'REGISTER')].copy()
    curr_transactions.loc[:, 'Effective Date'] = pd.to_datetime(curr_transactions['Effective Date'])
    curr_transactions = curr_transactions.sort_values('Effective Date')
    print("=====================")
    print(curr_transactions)
    for idx, row in curr_transactions[1:].iterrows():
        df_premium_transactions.at[idx, 'Type'] = 'ENDORSEMENT'

df_premium_transactions['Commission Expense'] = 0.0
comm_expense_mapping = {}
carrier_lookup = dict(zip(policies.Id, policies.Carrier))
def get_commission_expense_amount(curr_trans):
    curr_carrier = carrier_lookup[curr_trans.name]
    if(curr_carrier == 'Chubb'):
        total_revenue = curr_trans.Revenue.sum()
        total_premium = curr_trans.Amount.sum()
        new_revenue = total_premium * 0.18
        audits_trans = curr_trans[curr_trans.Type.str.contains('AUDIT')].copy()
        for idx, old_trans in curr_trans.iterrows():
            df_premium_transactions.at[idx, 'Revenue'] = old_trans.Amount * 0.18
        if(len(audits_trans) > 0):
            last_audit = audits_trans[audits_trans['Process Date'] == audits_trans['Process Date'].max()]
            df_premium_transactions.at[last_audit.index[0], 'Commission Expense'] = -(new_revenue - total_revenue)

df_premium_transactions.groupby('Policy').apply(lambda x: get_commission_expense_amount(x))

transaction_type_mapping = {
    'REGISTER' : 'REGISTER',
    'RATE NON - PROVISIONAL' : 'ENDORSEMENT',
    'AUDIT - EXPIRATION' : 'AUDIT',
    'CANCEL PRO RATA' : 'MIDTERM CANCEL',
    'CANCEL - FLAT' : 'FLAT CANCEL',
    'AUDIT - CANCELLATION' : 'AUDIT',
    'AUDIT - REVISE' : 'AUDIT',
    'NEGATE AUDIT' : 'AUDIT',
    'RATE PROVISIONAL' : 'ENDORSEMENT',
    'REINSTATE' : 'REINSTATE',
    'SF' : 'SF',
    'REWRITE' : 'REWRITE',
    '' : 'EMPTY',
    'ENDORSEMENT' : 'ENDORSEMENT',
    'CANCEL - MIDTERM' : 'MIDTERM CANCEL',
    'CANCEL - FLAT' : 'FLAT CANCEL',
    'AUDIT - CORRECTIION' : 'AUDIT',
    'REPLACE BINDER' : 'REINSTATE'
}

df_premium_transactions.loc[:, 'Type'] = df_premium_transactions.Type.apply(lambda x: transaction_type_mapping[x])
df_premium_transactions.loc[:, 'Process Date'] = df_premium_transactions['Process Date'].fillna(df_premium_transactions['Effective Date'])
df_premium_transactions = df_premium_transactions.sort_values('Process Date').reset_index(drop=True).copy()

df_premium_transactions['Accounting Date'] = ''
for policy in df_premium_transactions.Policy.unique():
    curr_transactions = df_premium_transactions[df_premium_transactions.Policy == policy]
    for idx, row in curr_transactions.iterrows():
        if(row.Type == 'REGISTER'):
            df_premium_transactions.at[idx, 'Accounting Date'] = row['Effective Date'].strftime('%m/%d/%Y')
        else:
            df_premium_transactions.at[idx, 'Accounting Date'] = row['Process Date'].strftime('%m/%d/%Y')

df_premium_transactions.loc[:, 'Effective Date'] = df_premium_transactions['Effective Date'].dt.strftime('%m/%d/%Y')
df_premium_transactions.loc[:, 'Process Date'] = df_premium_transactions['Process Date'].dt.strftime('%m/%d/%Y')

df_earned_transactions.loc[:, 'Coverage Start'] = df_earned_transactions['Coverage Start'].dt.strftime('%m/%d/%Y')
df_earned_transactions.loc[:, 'Coverage End'] = df_earned_transactions['Coverage End'].dt.strftime('%m/%d/%Y')
df_earned_transactions.loc[:, 'Date'] = df_earned_transactions['Date'].dt.strftime('%m/%d/%Y')

airtable = Airtable('appfIwwat0W37vtdU', 'Premium Transactions', airtable_credentials)
print('Inserting', len(df_premium_transactions), 'records')
#airtable.batch_insert(df_premium_transactions.fillna('').to_dict('records'), typecast=True)

airtable = Airtable('appfIwwat0W37vtdU', 'Earned Transactions', airtable_credentials)
print('Inserting', len(df_earned_transactions), 'records')
#airtable.batch_insert(df_earned_transactions.fillna('').to_dict('records'), typecast=True)

policies.loc[:, 'Policy Effective Date'] = policies['Policy Effective Date'].dt.strftime('%m/%d/%Y')
policies.loc[:, 'Policy Expiration Date'] = policies['Policy Expiration Date'].dt.strftime('%m/%d/%Y')

policy_lookup = dict(zip(policies.id, policies.Policy))

product_lookup = dict(zip(policies.Policy, policies.Product))
risk_group_lookup = dict(zip(policies.Policy, policies['Risk Group']))
contract_type_lookup = dict(zip(policies.Policy, policies['Contract Type']))
effective_date_lookup = dict(zip(policies.Policy, policies['Policy Effective Date']))
cohort_lookup = dict(zip(policies.Policy, policies['Cohort']))
billing_lookup = dict(zip(policies.Policy, policies['Billing Type']))
status_lookup = dict(zip(policies.Policy, policies['Policy Status']))

df_premium_transactions.loc[:, 'Policy'] = df_premium_transactions.Policy.apply(lambda x: policy_lookup[x])
df_earned_transactions.loc[:, 'Policy'] = df_earned_transactions.Policy.apply(lambda x: policy_lookup[x])

######################################### Filter out Flat Cancels

policies = policies[policies['Policy Status'] != 'Cancelled (Flat)'].copy()

df_premium_transactions['status'] = df_premium_transactions.Policy.apply(lambda x: status_lookup[x])
df_premium_transactions = df_premium_transactions[df_premium_transactions.status != 'Cancelled (Flat)'].copy()
df_premium_transactions.drop(columns=['status'], inplace=True)

df_earned_transactions['status'] = df_earned_transactions.Policy.apply(lambda x: status_lookup[x])
df_earned_transactions = df_earned_transactions[df_earned_transactions.status != 'Cancelled (Flat)'].copy()
df_earned_transactions.drop(columns=['status'], inplace=True)

wc_policies = wc_policies[wc_policies['Policy Status'] != 'Cancelled (Flat)'].copy()

################################################################

df_premium_transactions['Billing'] = df_premium_transactions.Policy.apply(lambda x: billing_lookup[x])
df_premium_transactions['Product'] = df_premium_transactions.Policy.apply(lambda x: product_lookup[x])
df_earned_transactions['Product'] = df_earned_transactions.Policy.apply(lambda x: product_lookup[x])
df_premium_transactions['risk_group'] = df_premium_transactions.Policy.apply(lambda x: risk_group_lookup[x])
df_earned_transactions['risk_group'] = df_earned_transactions.Policy.apply(lambda x: risk_group_lookup[x])
df_premium_transactions['contract_type'] = df_premium_transactions.Policy.apply(lambda x: contract_type_lookup[x])
df_earned_transactions['contract_type'] = df_earned_transactions.Policy.apply(lambda x: contract_type_lookup[x])

df_premium_transactions['policy_effective_date'] = df_premium_transactions.Policy.apply(lambda x: effective_date_lookup[x])
df_premium_transactions['cohort'] = df_premium_transactions.Policy.apply(lambda x: cohort_lookup[x])

df_premium_transactions['premium_bucket'] = ''
df_premium_transactions['product_bucket'] = ''

df_premium_transactions.reset_index(drop=True, inplace=True)
df_earned_transactions.reset_index(drop=True, inplace=True)

for idx, row in df_premium_transactions.iterrows():
    if((row.Type == 'REGISTER' or row.Type == 'REINSTATE' or row.Type == 'REWRITE') and (row.contract_type == 'NEW')):
        df_premium_transactions.at[idx, 'premium_bucket'] = 'New'
    elif((row.Type == 'REGISTER' or row.Type == 'REINSTATE' or row.Type == 'REWRITE') and row.contract_type == 'RENEWAL' or row.contract_type == ''):
        df_premium_transactions.at[idx, 'premium_bucket'] = 'Renewal'
    elif(row.Amount > 0):
        df_premium_transactions.at[idx, 'premium_bucket'] = 'Expansion'
    elif(row.Amount <= 0):
        df_premium_transactions.at[idx, 'premium_bucket'] = 'Churn'
    else:
        print('Edge')
        raise ValueError
    
    if(row.Product == "Workers' Comp"):
        df_premium_transactions.at[idx, 'product_bucket'] = 'WC'
    else:
        df_premium_transactions.at[idx, 'product_bucket'] = 'Cross'


################## Audit Analysis - get an average rate of change for verified audits, use to modify estimated audits to make them more realistic


verified_audits_policy_ids = set(Premium_Audit[Premium_Audit['Audit Status'] == 'Glow Verified']['Id (from WC Policies)'].values)

verified_audit_policies = wc_policies[(wc_policies.Id.isin(verified_audits_policy_ids)) & ~(wc_policies['Policy Status'].str.contains('Active'))].copy()
unverified_audit_policies = wc_policies[~(wc_policies.Id.isin(verified_audits_policy_ids)) & ~(wc_policies['Policy Status'].str.contains('Active')) & ~(wc_policies['Contract ID'] == '') & (wc_policies['Policy Status'] != ('Cancelled (Midterm)'))].copy()


audit_analysis = []
for idx, row in verified_audit_policies.iterrows():
    curr_transactions = WCS_Transaction_History[WCS_Transaction_History['Contract ID'] == row['Contract ID']].copy()

    audit_amount = curr_transactions[(curr_transactions.Transaction.str.contains('AUDIT'))].Premium.sum()

    total_billable_after_audit = curr_transactions.Premium.sum()
    total_billable_before_audit = curr_transactions[~(curr_transactions.Transaction.str.contains('AUDIT'))].Premium.sum()

    if(audit_amount > 0.0):
        audit_analysis.append({
            'Contract ID' : row['Contract ID'],
            'Policy Status' : row['Policy Status'],
            'Before positive audit' : total_billable_before_audit,
            'After positive audit' : total_billable_after_audit
        })
    else:
        audit_analysis.append({
            'Contract ID' : row['Contract ID'],
            'Policy Status' : row['Policy Status'],
            'Before negative audit' : total_billable_before_audit,
            'After negative audit' : total_billable_after_audit
        })

audit_analysis_df = pd.DataFrame(audit_analysis)

audit_analysis_df['premium_change_positive'] = (audit_analysis_df['After positive audit'] - audit_analysis_df['Before positive audit']) / audit_analysis_df['Before positive audit']
audit_analysis_df['premium_change_negative'] = (audit_analysis_df['After negative audit'] - audit_analysis_df['Before negative audit']) / audit_analysis_df['Before negative audit']

avg_audit_change_positive = audit_analysis_df['premium_change_positive'].sum() / len(audit_analysis_df[~pd.isnull(audit_analysis_df.premium_change_positive)])
avg_audit_change_negative = audit_analysis_df['premium_change_negative'].sum() / len(audit_analysis_df[~pd.isnull(audit_analysis_df.premium_change_negative)])

avg_missing_audit_increase = 0.35

risk_group_cohort_table = df_premium_transactions[~pd.isnull(df_premium_transactions.risk_group) & ~pd.isnull(df_premium_transactions.cohort)].copy()
risk_group_to_cohort = dict(zip(risk_group_cohort_table.risk_group, risk_group_cohort_table.cohort))

missing_audits = []
for contract_id in unverified_audit_policies['Contract ID'].unique():
    curr_transactions = WCS_Transaction_History[WCS_Transaction_History['Contract ID'] == contract_id].copy()
    if(len(curr_transactions) == 0):
        print('No WCS Data yet for', contract_id)
        continue
    audit_amount = curr_transactions[curr_transactions.Transaction.str.contains('AUDIT')].Premium.sum()
    total_billable_without_audit = curr_transactions[~curr_transactions.Transaction.str.contains('AUDIT')].Premium.sum()
    policy_effective_date = pd.to_datetime(curr_transactions['Policy Effective Date'].head(1).item())
    if(pd.isnull(policy_effective_date) or policy_effective_date == ''):
        print('No effective date for', contract_id)
        continue
    if(audit_amount == 0.0):
        # add audit transaction to policies missing one
        required_change = (total_billable_without_audit * avg_missing_audit_increase)
        missing_audits.append({
            'Policy' : curr_transactions['WC Policies'].head(1).item(),
            'Accounting Date' : policy_effective_date + relativedelta(years=1),
            'Process Date' : policy_effective_date + relativedelta(years=1),
            'Amount' : required_change,
            'Type' : 'AUDIT - MISSING',
            'Product' : "Workers' Comp",
            'risk_group' : curr_transactions['Bureau Id (from WC Policies)'].head(1).item(),
            'Contract Type' : curr_transactions['Contract Type'].head(1).item(),
            'premium_bucket' : "Audit Projection",
            'product_bucket' : "WC",
            'Effective Date' : policy_effective_date,
            'Revenue' : round(required_change * 0.18, 2),
            'cohort' : risk_group_to_cohort[curr_transactions['Bureau Id (from WC Policies)'].head(1).item()],
            'policy_effective_date' : policy_effective_date
        })

df_audit_projections = pd.DataFrame(missing_audits)
df_audit_projections.loc[:, 'Effective Date'] = df_audit_projections['Effective Date'].dt.strftime('%m/%d/%Y')
df_audit_projections.loc[:, 'Process Date'] = df_audit_projections['Process Date'].dt.strftime('%m/%d/%Y')
df_audit_projections.loc[:, 'Accounting Date'] = df_audit_projections['Accounting Date'].dt.strftime('%m/%d/%Y')
df_audit_projections.loc[:, 'policy_effective_date'] = df_audit_projections['policy_effective_date'].dt.strftime('%m/%d/%Y')

df_premium_transactions = pd.concat([df_premium_transactions, df_audit_projections], ignore_index=True).copy()

upload_data('Data_Policies', policies[['id', 'Carrier', 'Product', 'Producer', 'State', 'Governing Class Code', 'Cohort', 'Billing Type', 'Policy Effective Date', 'Policy Expiration Date', 'Policy Status', 'Contract ID']].fillna(''))
upload_data('Data_Premium_Transactions', df_premium_transactions[['Policy', 'Accounting Date', 'Process Date', 'Amount', 'Type', 'Product', 'risk_group', 'contract_type', 'premium_bucket', 'product_bucket', 'Effective Date', 'Revenue', 'policy_effective_date', 'cohort', 'Commission Expense', 'Billing']].fillna(''))
upload_data('Data_Earned_Transactions', df_earned_transactions.fillna(''))

################################################################

df_customer_transactions = df_premium_transactions.copy()
df_customer_transactions.loc[:, 'Effective Date'] = pd.to_datetime(df_customer_transactions['Effective Date'])

register_transactions = set([
    'REGISTER',
    'REINSTATE',
    'REWRITE'
])

cancel_transactions = set([
    'MIDTERM CANCEL',
    'FLAT CANCEL',
])

df_customer_register_transactions = df_customer_transactions[df_customer_transactions.Type.isin(register_transactions)].copy()
df_customer_cancel_transactions = df_customer_transactions[df_customer_transactions.Type.isin(cancel_transactions)].copy()

df_customer_register_transactions.loc[:, 'Type'] = 'BIND'
df_customer_register_transactions['sub_type'] = 'BIND'
df_customer_cancel_transactions['sub_type'] = df_customer_cancel_transactions.Type.copy()
df_customer_cancel_transactions.loc[:, 'Type'] = 'CANCEL'

df_customer_register_transactions = df_customer_register_transactions.sort_values('Effective Date').drop_duplicates(subset=['risk_group'], keep='first').copy()
df_customer_cancel_transactions = df_customer_cancel_transactions.sort_values('Effective Date').drop_duplicates(subset=['risk_group'], keep='last').copy()

df_customer_transactions = pd.concat([df_customer_register_transactions, df_customer_cancel_transactions]).sort_values('risk_group').reset_index(drop=True)

all_risk_groups = set(wc_policies['Bureau Id'].unique())

active_policy_statuses = set([
    'Active (New)',
    'Active (Renewed)',
])

rg_mapping = {}

# Remove cancel transactions for still-active customers
non_renew_transactions = []
for rg in all_risk_groups:
    curr_policies = wc_policies[(wc_policies['Bureau Id'] == rg) & (wc_policies['Policy Status'].isin(active_policy_statuses))].copy()
    if(len(curr_policies) > 0):
        rg_mapping[rg] = 'Active'
        cancel_transactions = df_customer_transactions[(df_customer_transactions.risk_group == rg) & (df_customer_transactions.Type == 'CANCEL')].copy()
        if(len(cancel_transactions) > 0):
            df_customer_transactions = df_customer_transactions[~((df_customer_transactions.risk_group == rg) & (df_customer_transactions.Type == 'CANCEL'))].copy()
    else:
        rg_mapping[rg] = 'Cancelled'
        if(len(df_customer_transactions[(df_customer_transactions.risk_group == rg) & ((df_customer_transactions.Type == 'CANCEL'))].copy()) == 0):
            curr_premium_transactions = df_premium_transactions[(df_premium_transactions.risk_group == rg) & (df_premium_transactions.Type == 'REGISTER')].copy()
            curr_premium_transactions.loc[:, 'Effective Date'] = pd.to_datetime(curr_premium_transactions['Effective Date'])
            curr_premium_transactions = curr_premium_transactions.sort_values('Effective Date').reset_index(drop=True)
            if(len(curr_premium_transactions) > 0):
                #print('================================', (curr_premium_transactions['Effective Date'].max() + relativedelta(years=1)).strftime('%m/%d/%Y'), rg)
                #print(curr_premium_transactions[['Type', 'Effective Date']])
                non_renew_transactions.append({
                    'Policy' : '',
                    'Effective Date' : curr_premium_transactions['Effective Date'].max() + relativedelta(years=1),
                    'Process Date' : '',
                    'Amount' : '',
                    'Revenue' : '',
                    'Type' : '',
                    'Commission Expense' : '',
                    'Accounting Date' : '',
                    'Billing' : '',
                    'Product' : '',
                    'risk_group' : curr_premium_transactions.risk_group.head(1).item(),
                    'contract_type' : '',
                    'policy_effective_date' : '',
                    'cohort' : curr_premium_transactions.cohort.head(1).item(),
                    'premium_bucket' : '',
                    'product_bucket' : '',
                    'Contract Type' : '',
                    'sub_type' : 'NON RENEW',
                })

df_customer_transactions = pd.concat([df_customer_transactions, pd.DataFrame(non_renew_transactions)], ignore_index=True)

df_customer_transactions['customer_effective_date'] = pd.to_datetime(df_customer_transactions.cohort)
df_customer_transactions['effective_months_after_start'] = (12 * (df_customer_transactions['Effective Date'].dt.year - df_customer_transactions.customer_effective_date.dt.year) + (df_customer_transactions['Effective Date'].dt.month - df_customer_transactions.customer_effective_date.dt.month)).apply(lambda x: 0 if x < 0 else x)
df_customer_transactions.loc[:, 'Effective Date'] = df_customer_transactions['Effective Date'].dt.strftime('%m/%d/%Y')

upload_data('Data_Customers', df_customer_transactions[['Policy', 'Effective Date', 'Process Date', 'Type', 'risk_group', 'cohort', 'effective_months_after_start', 'sub_type', 'risk_group']].fillna(''))

################################################################

df_cohort_premium = df_premium_transactions.copy()

df_cohort_premium.loc[:, 'Effective Date'] = pd.to_datetime(df_cohort_premium['Effective Date'])
df_cohort_premium.loc[:, 'Process Date'] = pd.to_datetime(df_cohort_premium['Process Date'])
df_cohort_premium = df_cohort_premium.sort_values('Effective Date').reset_index(drop=True).copy()

# Use process date for everything except REGISTER and AUDIT transactions
for idx, row in df_cohort_premium.iterrows():
    if row.Type != 'REGISTER' and not 'AUDIT' in row.Type and not 'FLAT' in row.Type:
        df_cohort_premium.at[idx, 'Effective Date'] = row['Process Date']

policy_order_lookup = {}
policy_effective_date_lookup = {}
last_policies = []
risk_group_registers = df_cohort_premium[(df_cohort_premium.Type == 'REGISTER') | (df_cohort_premium.Type == 'REINSTATE')].copy()
for rg in risk_group_registers.risk_group.unique().tolist():
    policy_effective_date_lookup[rg] = {}
    current_registers = risk_group_registers[risk_group_registers.risk_group == rg].copy()
    all_products = current_registers.Product.unique().tolist()
    for product in all_products:
        policy_effective_date_lookup[rg][product] = {}
        curr_product_registers = current_registers[current_registers.Product == product].copy()
        current_policy_order = 1
        for idx, row in curr_product_registers.iterrows():
            policy_effective_date_lookup[rg][product][current_policy_order] = {}
            policy_effective_date_lookup[rg][product][current_policy_order]['effective_date'] = row['Effective Date']
            policy_effective_date_lookup[rg][product][current_policy_order]['policy_id'] = row['Policy']
            policy_order_lookup[row.Policy] = current_policy_order
            current_policy_order += 1
        last_policies.append(row.Policy)

# Find renewals by product and tag with policy order. Then put end policy transactions
# for each policy at the start of the next policy, unless it is the last one, in which case
# put the end policy transaction either at the cancellation or at the expiration if no cancellation present.
# Move Cancellations by effective date to the policy register effective date.

last_policies = set(last_policies)
df_cohort_premium['policy_order'] = df_cohort_premium.Policy.apply(lambda x: policy_order_lookup[x])
df_cohort_premium['is_last_policy'] = df_cohort_premium.Policy.isin(last_policies)


# Move audit to start of next policy AND change policy ID on the audit so that it correctly impacts end policy
# Zero out audits that are close to the customer effective date
for policy in df_cohort_premium.Policy.unique().tolist():
    curr_transactions = df_cohort_premium[df_cohort_premium.Policy == policy].copy()
    is_last_policy = True if policy in last_policies else False
    current_policy_order = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].policy_order.head(1).item()
    current_policy_product = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].Product.head(1).item()
    current_policy_risk_group = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].risk_group.head(1).item()
    current_policy_effective_date = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].policy_effective_date.head(1).item()
    current_customer_effective_date = risk_group_to_effective_date[current_policy_risk_group]
    curr_policy_cohort = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].cohort.head(1).item()

    if(not is_last_policy):
        next_effective_date = policy_effective_date_lookup[current_policy_risk_group][current_policy_product][current_policy_order + 1]['effective_date']
        next_id = policy_effective_date_lookup[current_policy_risk_group][current_policy_product][current_policy_order + 1]['policy_id']
        current_audits = curr_transactions[curr_transactions.Type.str.contains('AUDIT')].copy()

        for idx, row in current_audits.iterrows():
            df_cohort_premium.at[idx, 'Effective Date'] = next_effective_date
            df_cohort_premium.at[idx, 'Policy'] = next_id

end_policy_txns = []
for policy in df_cohort_premium.Policy.unique().tolist():
    curr_transactions = df_cohort_premium[df_cohort_premium.Policy == policy].copy()
    is_last_policy = True if policy in last_policies else False
    current_policy_order = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].policy_order.head(1).item()
    current_policy_product = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].Product.head(1).item()
    current_policy_risk_group = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].risk_group.head(1).item()
    current_policy_effective_date = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].policy_effective_date.head(1).item()
    curr_policy_cohort = curr_transactions[curr_transactions.Type != 'AUDIT - MISSING'].cohort.head(1).item()

    if(not is_last_policy):
        next_effective_date = policy_effective_date_lookup[current_policy_risk_group][current_policy_product][current_policy_order + 1]['effective_date']

        end_policy_txns.append({
                'Policy' : policy,
                'Effective Date' : next_effective_date,
                'Amount' : -curr_transactions.Amount.sum(),
                'Type' : 'End Policy',
                'Product' : current_policy_product,
                'risk_group' : current_policy_product,
                'policy_effective_date' : current_policy_effective_date,
                'cohort' : curr_policy_cohort,
                'policy_order' : current_policy_order,
                'is_last_policy' : is_last_policy
        })
    else:
        curr_cancel_transactions = curr_transactions[curr_transactions.Type.str.contains('CANCEL')].copy()

        if(len(curr_cancel_transactions) > 0):
            num_cancel_txns = 0
            for idx, cancel_txn in curr_cancel_transactions.iterrows():
                if(num_cancel_txns >= 1):
                    print('Skipping multiple cancel transactions for policy', policy)
                    continue

                if(cancel_txn.Type == 'MIDTERM CANCEL'):
                    end_policy_txns.append({
                        'Policy' : policy,
                        'Effective Date' : cancel_txn['Effective Date'],
                        'Amount' : -curr_transactions.Amount.sum(),
                        'Type' : 'End Policy',
                        'Product' : current_policy_product,
                        'risk_group' : current_policy_product,
                        'policy_effective_date' : current_policy_effective_date,
                        'cohort' : curr_policy_cohort,
                        'policy_order' : current_policy_order,
                        'is_last_policy' : is_last_policy
                   })
                elif(cancel_txn.Type == 'FLAT CANCEL'):
                    if((curr_transactions.Amount.sum() != 0) and (current_policy_product != 'A&H')):
                        #print('Flat Cancel amount not zero', curr_transactions.Amount.sum())
                        #print(curr_transactions[['Type', 'Amount', 'policy_effective_date']])
                        end_policy_txns.append({
                            'Policy' : policy,
                            'Effective Date' : pd.to_datetime(current_policy_effective_date),
                            'Amount' : -curr_transactions.Amount.sum(),
                            'Type' : 'End Policy',
                            'Product' : current_policy_product,
                            'risk_group' : current_policy_product,
                            'policy_effective_date' : current_policy_effective_date,
                            'cohort' : curr_policy_cohort,
                            'policy_order' : current_policy_order,
                            'is_last_policy' : is_last_policy
                        })
                    elif(current_policy_product == 'A&H'):
                        # A&H transactions work slightly differently
                        end_policy_txns.append({
                            'Policy' : policy,
                            'Effective Date' : pd.to_datetime(current_policy_effective_date),
                            'Amount' : -curr_transactions.Amount.sum(),
                            'Type' : 'End Policy',
                            'Product' : current_policy_product,
                            'risk_group' : current_policy_product,
                            'policy_effective_date' : current_policy_effective_date,
                            'cohort' : curr_policy_cohort,
                            'policy_order' : current_policy_order,
                            'is_last_policy' : is_last_policy
                        })
                    else:
                        # Add end policy transactions just to signify end with no amount
                        end_policy_txns.append({
                            'Policy' : policy,
                            'Effective Date' : pd.to_datetime(current_policy_effective_date),
                            'Amount' : -curr_transactions.Amount.sum(),
                            'Type' : 'End Policy',
                            'Product' : current_policy_product,
                            'risk_group' : current_policy_product,
                            'policy_effective_date' : current_policy_effective_date,
                            'cohort' : curr_policy_cohort,
                            'policy_order' : current_policy_order,
                            'is_last_policy' : is_last_policy
                        })

                else:
                    print('Unknown Cancel Type')
                    raise ValueError
                
                df_cohort_premium.at[idx, 'Effective Date'] = current_policy_effective_date
                num_cancel_txns += 1
        else:
            end_policy_txns.append({
                'Policy' : policy,
                'Effective Date' : (pd.to_datetime(current_policy_effective_date) + relativedelta(years=1)),
                'Amount' : -curr_transactions.Amount.sum(),
                'Type' : 'End Policy',
                'Product' : current_policy_product,
                'risk_group' : current_policy_product,
                'policy_effective_date' : current_policy_effective_date,
                'cohort' : curr_policy_cohort,
                'policy_order' : current_policy_order,
                'is_last_policy' : is_last_policy
            })

df_cohort_premium = pd.concat([df_cohort_premium, pd.DataFrame(end_policy_txns)], ignore_index=True)

df_cohort_premium['customer_effective_date'] = pd.to_datetime(df_cohort_premium.cohort)
df_cohort_premium['effective_months_after_start'] = (12 * (df_cohort_premium['Effective Date'].dt.year - df_cohort_premium.customer_effective_date.dt.year) + (df_cohort_premium['Effective Date'].dt.month - df_cohort_premium.customer_effective_date.dt.month)).apply(lambda x: 0 if x < 0 else x)
df_cohort_premium.loc[:, 'Effective Date'] = df_cohort_premium['Effective Date'].dt.strftime('%m/%d/%Y')
df_cohort_premium.loc[:, 'Process Date'] = df_cohort_premium['Process Date'].dt.strftime('%m/%d/%Y')
df_cohort_premium.loc[:, 'customer_effective_date'] = df_cohort_premium['customer_effective_date'].dt.strftime('%m/%d/%Y')

df_cohort_premium = df_cohort_premium.sort_values(['effective_months_after_start', 'Type']).reset_index(drop=True)

current_audits = curr_transactions[curr_transactions.Type.str.contains('AUDIT - MISSING')].copy()
for idx, row in df_cohort_premium.iterrows():
    if('AUDIT' in row.Type and row.effective_months_after_start <= 0):
        df_cohort_premium.at[idx, 'Amount'] = 0

upload_data('Data_Cohort_Premium', df_cohort_premium[['Policy', 'Effective Date', 'Process Date', 'Amount', 'cohort', 'customer_effective_date', 'effective_months_after_start', 'Type', 'policy_effective_date', 'Product', 'is_last_policy', 'policy_order']].fillna(''))

df_cohort_policies = df_cohort_premium.copy()
df_cohort_policies.loc[:, 'Type'] = df_cohort_policies.Type.apply(lambda x: 'REGISTER' if x == 'REINSTATE' else x)

print('\n\n')
for policy in df_cohort_policies.Policy.unique().tolist():
    curr_transactions = df_cohort_policies[df_cohort_policies.Policy == policy].copy()
    if(len(curr_transactions[curr_transactions.Type == 'REGISTER']) != len(curr_transactions[curr_transactions.Type == 'End Policy'])):
        print('=========================')
        print(curr_transactions[['Type', 'Effective Date']])
        for idx, row in curr_transactions[curr_transactions.Type == 'REGISTER'][1:].iterrows():
            df_cohort_policies.at[idx, 'Type'] = 'RE-REGISTER'
        print('\nChanged To\n', df_cohort_policies[df_cohort_policies.Policy == policy][['Type', 'Effective Date']])

df_cohort_policies = df_cohort_policies[(df_cohort_policies.Type == 'REGISTER') | (df_cohort_policies.Type == 'End Policy')].copy()


upload_data('Data_Cohort_Policies', df_cohort_policies[['Policy', 'Effective Date', 'Process Date', 'Amount', 'cohort', 'customer_effective_date', 'effective_months_after_start', 'Type']].fillna(''))

################################################################

leads = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\Dashboard_Data\\Sales_Data_2021\\Leads_All_Time.csv')
leads = leads.groupby(['policyEffectiveYear','policyEffectiveMonth','governingClassCode']).total.sum().reset_index()
upload_data('Data_Leads', leads.fillna(''))

prospects = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\Dashboard_Data\\Sales_Data_2021\\Prospects_by_Month.csv')
prospects = prospects.groupby(['createdMonth','policyEffectiveMonth','governingClassCode']).total.sum().reset_index()
upload_data('Data_Prospects', prospects.fillna(''))

deals = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\deals_sales_report\\all_deals_with_history.csv')
deals = deals[(deals['properties.pipeline.value'] == '1052209') | (deals['properties.pipeline.value'] == 'default')].copy()
deals = deals.groupby(['effective_month', 'effective_year', 'deal_type']).size().reset_index().rename(columns={0:'counts'})
upload_data('Data_Deals', deals.fillna(''))

########################################## Payroll and Expenses

Airtable_Expenses.loc[:, 'Debit'] = Airtable_Expenses.Debit.replace('', 0).astype(float)
Airtable_Expenses.loc[:, 'Credit'] = Airtable_Expenses.Credit.replace('', 0).astype(float)

Airtable_Expenses['Amount'] = Airtable_Expenses.Debit - Airtable_Expenses.Credit
Airtable_Expenses.loc[:, 'Posted Dt.'] = pd.to_datetime(Airtable_Expenses['Posted Dt.'])

Airtable_Expenses['posted_date_month'] = Airtable_Expenses['Posted Dt.'].dt.strftime('%m').astype(int)
Airtable_Expenses['posted_date_year'] = Airtable_Expenses['Posted Dt.'].dt.strftime('%Y').astype(int)

expenses_aggregated = Airtable_Expenses.groupby(['posted_date_year', 'posted_date_month', 'Department', 'Financial Model Mapping (from Accounts)']).Amount.sum().reset_index()
upload_data('Data_Expenses', expenses_aggregated)

#####################################################################################

Airtable_BS.loc[:, 'Debit'] = Airtable_BS.Debit.replace('', 0).astype(float)
Airtable_BS.loc[:, 'Credit'] = Airtable_BS.Credit.replace('', 0).astype(float)

Airtable_BS['Amount'] = Airtable_BS.Debit - Airtable_BS.Credit
Airtable_BS.loc[:, 'Posted Dt.'] = pd.to_datetime(Airtable_BS['Posted Dt.'])

Airtable_BS['posted_date_month'] = Airtable_BS['Posted Dt.'].dt.strftime('%m').astype(int)
Airtable_BS['posted_date_year'] = Airtable_BS['Posted Dt.'].dt.strftime('%Y').astype(int)

Airtable_BS.groupby(['Type (from Accounts)', 'Financial Model Mapping (from Accounts)']).Amount.sum().reset_index()

bs_aggregated = Airtable_BS.groupby(['posted_date_year', 'posted_date_month', 'Type (from Accounts)']).Amount.sum().reset_index()
upload_data('Data_BS_Expenses', bs_aggregated)

#####################################################################################

Airtable_Payroll.loc[:, 'Total employer cost'] = Airtable_Payroll['Total employer cost'].replace('', 0).astype(float)
Airtable_Payroll.loc[:, 'Commission'] = Airtable_Payroll.Commission.replace('', 0).astype(float).fillna(0) + Airtable_Payroll['Commission - Sup'].replace('',0).astype(float).fillna(0)

Airtable_Payroll.loc[:, 'Total employer cost'] = Airtable_Payroll['Total employer cost'] - Airtable_Payroll.Commission

Airtable_Payroll.loc[:, 'Pay period start'] = pd.to_datetime(Airtable_Payroll['Pay period start'].replace('', np.NaN).fillna(Airtable_Payroll['Pay Date']))

Airtable_Payroll['Type'] = 'Payroll'

commission_data = []
for idx, row in Airtable_Payroll[Airtable_Payroll.Commission != 0].iterrows():
    commission_data.append({
        'Pay period start' : row['Pay period start'],
        'Total employer cost' : row.Commission,
        'Department' : row.Department,
        'Type' : 'Commission',
        'First name' : row['First name'],
        'Last name' : row['Last name']
    })

Airtable_Payroll = Airtable_Payroll.copy().append(pd.DataFrame(commission_data))

Airtable_Payroll.reset_index(drop=True, inplace=True)

Airtable_Payroll['pay_period_month'] = Airtable_Payroll['Pay period start'].dt.strftime('%m').astype(int)
Airtable_Payroll['pay_period_year'] = Airtable_Payroll['Pay period start'].dt.strftime('%Y').astype(int)

Airtable_Payroll['name'] = Airtable_Payroll['First name'].str.capitalize() + ' ' + Airtable_Payroll['Last name'].str.capitalize()
Airtable_Payroll.fillna('', inplace=True)

payroll_aggregated = Airtable_Payroll.groupby(['pay_period_year', 'pay_period_month', 'Department', 'Type', 'name', 'Account Memo'], dropna=False)['Total employer cost'].sum().reset_index()
upload_data('Data_Payroll', payroll_aggregated[['pay_period_year', 'pay_period_month',	'Department', 'Type', 'Total employer cost', 'name', 'Account Memo']])

#################################################################### Prospects by Create Date

empty = []
for i in range(1, 13):
    for j in range(2019, 2023):
        empty.append({
                'effective_month' : i,
                'effective_year' : j,
                'value' : 0.0
        })
empty = pd.DataFrame(empty).groupby(['effective_month', 'effective_year']).value.sum().unstack(fill_value=0)

prospects = pd.read_csv('C:\\Users\\shahb\\Documents\\Glow\\lead-prospect-data-discovery\\Dashboard_Data\\Sales_Data_2021\\prospects_by_Month.csv')

prospect_stage_mapping = {
    'Bureau' : 'Risk',
    'Market Declined' : 'Risk',
    'Market Hold' : 'Risk',
    'Sales Declined' : 'Market Approved',
    'Sales Approved' : 'Sales Approved'
}

prospects.loc[:, 'stage'] = prospects.stage.apply(lambda x: prospect_stage_mapping.get(x, x))
prospects.loc[:, 'policyEffectiveMonth'] = prospects.policyEffectiveMonth.fillna(0).astype(int)
prospects['created_year'] = 2021

prospects_by_created_month = prospects.groupby(['createdMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0).reindex_like(empty).fillna(0)
risks_by_created_month = prospects[prospects.stage == 'Risk'].groupby(['createdMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0).reindex_like(empty).fillna(0)
market_approved_by_created_month = prospects[prospects.stage == 'Market Approved'].groupby(['createdMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0).reindex_like(empty).fillna(0)
sales_approved_by_created_month = prospects[prospects.stage == 'Sales Approved'].groupby(['createdMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0).reindex_like(empty).fillna(0)

#################################################################### Prospects by Effective Date

# Have to separate the prospects for which we don't know the "real" create date because these were
# imported manually (before April 2021). We also only want to count it in an effective month if the prospect was
# created before that effective month.

unknown_create_date_prospects = prospects[(prospects.createdMonth <= 4) & (prospects.policyEffectiveMonth > 4)].copy()

risk_prospects_unknown_cdate = unknown_create_date_prospects[unknown_create_date_prospects.stage == 'Risk'].copy()
market_approved_prospects_unknown_cdate = unknown_create_date_prospects[unknown_create_date_prospects.stage == 'Market Approved'].copy()
sales_approved_prospects_unknown_cdate = unknown_create_date_prospects[unknown_create_date_prospects.stage == 'Sales Approved'].copy()

grouped_unknown_create_date_prospects = unknown_create_date_prospects.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)
grouped_risk_prospects_unknown_cdate = risk_prospects_unknown_cdate.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)
grouped_market_approved_prospects_unknown_cdate = market_approved_prospects_unknown_cdate.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)
grouped_sales_approved_prospects_unknown_cdate = sales_approved_prospects_unknown_cdate.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)

prospects = prospects[(prospects.createdMonth < prospects.policyEffectiveMonth) & (prospects.createdMonth > 4)].copy()

risk_prospects = prospects[(prospects.createdMonth < prospects.policyEffectiveMonth) & (prospects.createdMonth > 4) & (prospects.stage == 'Risk')].copy()
market_approved_prospects = prospects[(prospects.createdMonth < prospects.policyEffectiveMonth) & (prospects.createdMonth > 4) & (prospects.stage == 'Market Approved')].copy()
sales_approved_prospects = prospects[(prospects.createdMonth < prospects.policyEffectiveMonth) & (prospects.createdMonth > 4) & (prospects.stage == 'Sales Approved')].copy()

grouped_prospects = prospects.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)
grouped_risk_prospects = risk_prospects.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)
grouped_market_approved = market_approved_prospects.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)
grouped_sales_approved = sales_approved_prospects.groupby(['policyEffectiveMonth', 'created_year'], dropna=False).total.sum().unstack(fill_value=0)

grouped_prospects = grouped_prospects.reindex_like(empty).fillna(0) + grouped_unknown_create_date_prospects.reindex_like(empty).fillna(0)
grouped_risk_prospects = grouped_risk_prospects.reindex_like(empty).fillna(0) + grouped_risk_prospects_unknown_cdate.reindex_like(empty).fillna(0)
grouped_market_approved = grouped_market_approved.reindex_like(empty).fillna(0) + grouped_market_approved_prospects_unknown_cdate.reindex_like(empty).fillna(0)
grouped_sales_approved = grouped_sales_approved.reindex_like(empty).fillna(0) + grouped_sales_approved_prospects_unknown_cdate.reindex_like(empty).fillna(0)

data = [
    'grouped_risk_prospects',
    'grouped_market_approved',
    'grouped_sales_approved',
]

rows = 1
for table in data:
    time.sleep(1.5)
    curr_table = eval(table).copy()
    curr_table.fillna(0, inplace=True)
    range = 'Prospect_Data!A' + str(rows) + ':Z'
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

leads = pd.read_csv('allLeads.csv')
leads_stage_before_lost = pd.read_csv('Lost_Leads_Last_Status.csv')

leads.loc[:, 'policy_effective_date'] = pd.to_datetime(leads.policy_effective_date)
leads['effective_month'] = leads.policy_effective_date.dt.strftime('%m').astype(int)
leads['effective_year'] = leads.policy_effective_date.dt.strftime('%Y').astype(int)
leads['effective_month_year'] = pd.to_datetime(leads.policy_effective_date).dt.strftime('%Y-%m')

lead_id_to_last_stage_before_lost = dict(zip(leads_stage_before_lost.id, leads_stage_before_lost.previous_stage))

def apply_last_stage_before_lost(x):
    if(x.stage != 'Lost'):
        return x.stage
    else:
        previous_stage = lead_id_to_last_stage_before_lost.get(x.id, 'not found')
        if(previous_stage != 'not found' and previous_stage != 'Won' and previous_stage != 'New'):
            return previous_stage
        elif(previous_stage == 'New'):
            return 'Connected'
        else:
            return x.stage

leads.loc[:, 'stage'] = leads.apply(lambda x: apply_last_stage_before_lost(x), axis=1)

stages_to_filter_out = set([
    'Sales Filter - Tech No xMod',
    'Sales Filter - No class code',
    'UW Filter - Temporary Hold',
    'Sales Filter - No phone number',
    'Sales Filter - Temporary Hold',
    'No phone number',
    'Contact Information Needed',
    'UW Filter - State Decline'
])

leads = leads[~leads.stage.isin(stages_to_filter_out)].copy()

lead_new = set([
    'New', 
    'Effective Date Pending'
])

lead_connected = set([
    'Effective Date Passed',
    'Approaching',
    'Lead DM Identified', 
    'Sales Filter - Temporarily Closed',
    'Sales Filter - Business Closed',
    'Temporarily Closed',
    'Connected',
    'Lost'
])

lead_dm_reached = set([
    'Lead DM Reached',
    'Lead DM Information Requested',
    'Lead DM Requested Information',
    'Engaged - DM Reached'
])

lead_app_started = set([
    'Application Started',
    'Application Loss Runs Promised',
    'Application Payroll Bookkeeper Provided',
    'Application Loss Runs Sent',
    'UW Filter - UW Decline'
])

lead_app_submitted = set([
    'Application Submitted',
    'Won'
])

def map_lead_stage(x):
    if x in lead_new:
        return 'Leads'
    elif x in lead_connected:
        return 'Connected'
    elif x in lead_dm_reached:
        return 'DM Reached'
    elif x in lead_app_started:
        return 'App Started'
    elif x in lead_app_submitted:
        return 'App Submitted'
    else:
        print(x, 'not in lead stage mapping.')
        raise ValueError

leads.loc[:, 'stage'] = leads.stage.apply(lambda x: map_lead_stage(x))

leads.loc[:, 'created_at'] = pd.to_datetime(leads.created_at)
leads['create_month'] = leads.created_at.dt.strftime('%m').astype(int)
leads['create_year'] = leads.created_at.dt.strftime('%Y').astype(int)

leads_by_mapped_stage = leads.groupby(['effective_month', 'effective_year', 'create_month', 'create_year', 'stage']).size().reset_index().rename(columns={0:'total'})
leads_by_mapped_stage = leads_by_mapped_stage[['effective_month', 'effective_year', 'stage', 'total', 'create_month', 'create_year']].copy()

upload_data('Data_Leads_Mapped_Stage', leads_by_mapped_stage)