import utils as ut
import pandas as pd
import numpy as np
from pandas.api.types import is_object_dtype

## Function to Clean Paypal Accounts Transactions
def PreprocessTransactionsFile(WiseTransactionDF):
    TransactionDF = WiseTransactionDF.copy()
    TransactionDF = TransactionDF[~((TransactionDF['Source fee amount'].isnull()) | (TransactionDF['Source amount (after fees)'].isnull()))]
    TransactionDF["Created on"] = TransactionDF["Created on"].dt.strftime('%m/%d/%Y')
    if is_object_dtype(TransactionDF['Source fee amount']):
        TransactionDF['Source fee amount'] = TransactionDF['Source fee amount'].str.replace(",","").astype(float)
    if is_object_dtype(TransactionDF['Source amount (after fees)']):
        TransactionDF['Source amount (after fees)'] = TransactionDF['Source amount (after fees)'].str.replace(",","").astype(float)
    TransactionDF = TransactionDF[["ID","Created on","Status","Target name","Source fee amount","Source amount (after fees)"]]
    TransactionDFColumns = ['Id','Date', 'Status','Target','Fee','Amount']
    TransactionDF.columns = TransactionDFColumns
    TransactionDF = TransactionDF.sort_values(by='Date').reset_index(drop=True)
    return TransactionDF

## Function to create JournalDF
def CreateJournal(DF):
    JournalColumns = ['Journal Date','Reference Number','Journal Number Prefix','Journal Number Suffix','Notes','Journal Type','Currency','Account','Description','Contact Name',
                      'Debit','Credit','Tax Name','Tax Type','Tax Percentage','Exemption Code','Tax Authority','Item Tax Exemption Reason','Project Name']
    NonEditableColumns = ['Journal Number Prefix','Tax Name','Tax Type', 'Tax Percentage', 'Exemption Code', 
                          'Tax Authority','Item Tax Exemption Reason', 'Project Name']
    JournalDate = []
    ReferenceNumber = []
    JournalNumberSuffix = []
    Notes = []
    Account = []
    Description = []
    ContactName = []
    Debit = []
    Credit = []
    add_suffix = 1
    for index,row in DF.iterrows():
        
        JournalDate.append(row['Date'])
        ReferenceNumber.append(row['Id'])
        JournalNumberSuffix.append(add_suffix)
        Notes.append(row['Status'])
        Account.append('Contractor Expenses')
        Description.append('Payment for contractor')
        ContactName.append(row['Target'])
        Debit.append(row['Amount'])
        Credit.append('')
        
        JournalDate.append(row['Date'])
        ReferenceNumber.append(row['Id'])
        JournalNumberSuffix.append(add_suffix)
        Notes.append(row['Status'])
        Account.append('Uncategorized Revenue - Wise Inc')
        Description.append('Payment for contractor')
        ContactName.append(row['Target'])
        Debit.append('')
        Credit.append(row['Amount'])
        
        add_suffix = add_suffix+1
        
        JournalDate.append(row['Date'])
        ReferenceNumber.append(row['Id'])
        JournalNumberSuffix.append(add_suffix)
        Notes.append(row['Status'])
        Account.append('Wise Fees and Charges')
        Description.append('Payment for contractor')
        ContactName.append(row['Target'])
        Debit.append(row['Fee'])
        Credit.append('')
        
        JournalDate.append(row['Date'])
        ReferenceNumber.append(row['Id'])
        JournalNumberSuffix.append(add_suffix)
        Notes.append(row['Status'])
        Account.append('Uncategorized Revenue - Wise Inc')
        Description.append('Payment for contractor')
        ContactName.append(row['Target'])
        Debit.append('')
        Credit.append(row['Fee'])
        
        add_suffix = add_suffix + 1
        
    JournalDF = pd.DataFrame.from_dict({'Journal Date':JournalDate,'Reference Number': ReferenceNumber, 'Journal Number Suffix':JournalNumberSuffix,'Notes':Notes,'Account':Account,'Description':Description,'Contact Name':ContactName,
                                        'Debit':Debit,'Credit':Credit})
    JournalDF['Journal Type'] = 'both'
    JournalDF['Currency'] = 'USD'
    for col in NonEditableColumns:
        JournalDF[col] = np.nan
    JournalDF = JournalDF[JournalColumns]
    return JournalDF
