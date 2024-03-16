import utils as ut
import pandas as pd
import numpy as np
from pandas.api.types import is_object_dtype

## Function to Clean Stripe Accounts Transactions
def PreprocessTransactionsFile(StripeTransactionDF):
    TransactionDF = StripeTransactionDF.copy()
    TransactionDF["Created (UTC)"] = TransactionDF["Created (UTC)"].dt.strftime('%m/%d/%Y')
    TransactionDF = TransactionDF[TransactionDF.Type.str.strip().str.lower().isin(['charge','stripe_fee','payout','refund','adjustment'])]
    
    if is_object_dtype(TransactionDF['Fee']):
        TransactionDF['Fee'] = TransactionDF['Fee'].str.replace(",","").astype(float)
    if is_object_dtype(TransactionDF['Net']):
        TransactionDF['Net'] = TransactionDF['Net'].str.replace(",","").astype(float)
    if is_object_dtype(TransactionDF['Amount']):
        TransactionDF['Amount'] = TransactionDF['Amount'].str.replace(",","").astype(float)
        
    TransactionDF = TransactionDF[['id','Type','Amount','Fee','Net','Created (UTC)','Ko-fi Transaction Id (metadata)']]
    TransactionDFColumns = ['id','type','amount','fee','net','created_date','kofi_trans_id']
    TransactionDF.columns = TransactionDFColumns
    TransactionDF = TransactionDF[~((TransactionDF.fee.isnull()) | (TransactionDF.net.isnull()) | (TransactionDF.amount.isnull()))]
    TransactionDF = TransactionDF.sort_values(by='created_date').reset_index(drop=True)
    return TransactionDF

## Function to create JournalDF
def CreateJournal(DF):
    # print(DF)
    JournalColumns = ['Journal Date','Reference Number','Journal Number Prefix','Journal Number Suffix','Notes','Journal Type',
                      'Currency','Account','Description','Contact Name','Debit','Credit','Tax Name','Tax Type','Tax Percentage',
                      'Exemption Code','Tax Authority','Item Tax Exemption Reason','Project Name']
    NonEditableColumns = ['Journal Number Prefix','Contact Name','Tax Name','Tax Type', 'Tax Percentage', 'Exemption Code', 
                          'Tax Authority','Item Tax Exemption Reason', 'Project Name']
    JournalDate = []
    ReferenceNumber = []
    JournalNumberSuffix = []
    Notes = []
    Account = []
    Description = []
    Debit = []
    Credit = []
    
    add_suffix = 1
    
    for index,row in DF.iterrows():
        
        if row['type'].strip().lower() == "charge":
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append(row['net'])
            Credit.append('')
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            if (str(row['kofi_trans_id']).strip() == "nan") or (str(row['kofi_trans_id']).strip() == ""):
                Account.append('Income from SaaS')
            else:
                Account.append('Income from Donations')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append('')
            Credit.append(row['net'])
            
            add_suffix = add_suffix + 1
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe Charges')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append(row['fee'])
            Credit.append('')
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            if (str(row['kofi_trans_id']).strip() == "nan") or (str(row['kofi_trans_id']).strip() == ""):
                Account.append('Income from SaaS')
            else:
                Account.append('Income from Donations')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append('')
            Credit.append(row['fee'])
            
            add_suffix = add_suffix + 1
            
        elif row['type'].strip().lower() == "stripe_fee":
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe Charges')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append(row['amount'])
            Credit.append('')
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append('')
            Credit.append(row['amount'])
            
            add_suffix = add_suffix + 1
            
        elif row['type'].strip().lower() == "payout":
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Uncategorized Revenue - Stripe')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append(row['amount'])
            Credit.append('')
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append('')
            Credit.append(row['amount'])
            
            add_suffix = add_suffix + 1
        elif row['type'].strip().lower() == "adjustment":
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Refunds')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append(row['amount'])
            Credit.append('')
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append('')
            Credit.append(row['amount'])
            
            add_suffix = add_suffix + 1

        elif row['type'].strip().lower() == "refund":
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Refunds')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append(row['amount'])
            Credit.append('')
            
            JournalDate.append(row['created_date'])
            ReferenceNumber.append(row['id'])
            Notes.append(row['type'])
            Account.append('Stripe')
            Description.append(str(row['type']))
            JournalNumberSuffix.append(add_suffix)
            Debit.append('')
            Credit.append(row['amount'])
            
            add_suffix = add_suffix + 1
                
    JournalDF = pd.DataFrame.from_dict({'Journal Date':JournalDate,'Reference Number': ReferenceNumber, 
                                        'Journal Number Suffix':JournalNumberSuffix,'Notes':Notes,'Account':Account,
                                        'Description':Description,'Debit':Debit,'Credit':Credit})
    JournalDF['Journal Type'] = 'both'
    JournalDF['Currency'] = 'USD'
    for col in NonEditableColumns:
        JournalDF[col] = np.nan
    JournalDF = JournalDF[JournalColumns]
    return JournalDF