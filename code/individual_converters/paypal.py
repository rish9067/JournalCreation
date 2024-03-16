import utils as ut
import pandas as pd
import numpy as np
from pandas.api.types import is_object_dtype

## Function to Clean Paypal Accounts Transactions
def PreprocessTransactionsFile(PaypalTransactionDF):
    TransactionDF = PaypalTransactionDF.copy()
    
    TransactionDFColumns = ['Date', 'Type','Transaction ID','Gross','Fee']
    if is_object_dtype(TransactionDF['Gross']):
        TransactionDF['Gross'] = TransactionDF['Gross'].str.replace(",","").astype(float)
    if is_object_dtype(TransactionDF['Fee']):
        TransactionDF['Fee'] = TransactionDF['Fee'].str.replace(",","").astype(float)
    TransactionDF["Date"] = TransactionDF["Date"].dt.strftime('%m/%d/%Y')
    TransactionDF = TransactionDF[~(TransactionDF.Gross.isnull() & TransactionDF.Fee.isnull())]
    TransactionDF = TransactionDF[~TransactionDF.Type.isnull()]
    TransactionDF = TransactionDF[TransactionDFColumns].sort_values(by='Date').reset_index(drop=True)
    # print(TransactionDF)
    TypeCleanedList = []
    for index, row in TransactionDF.iterrows():
        TypeCleanedList.append(ut.CleanValue(row['Type']))
    TransactionDF['CleanedType'] = TypeCleanedList
    TransactionDF = TransactionDF[TransactionDFColumns+['CleanedType']]
    TransactionDF.columns = ['TransactionDate','Type','TransactionID','Gross','Fee','CleanedType']
    # print(TransactionDF.info())
    return TransactionDF

## Function to Check Upwork Missing Vendor
def VendorsCheck(TransactionDF,VendorsDF,OutputFilesPath,log_file_path,curr_date_time):
    MissingTypeList = list(set(TransactionDF.CleanedType).difference(set(VendorsDF.CleanedMatchingtext)))
    
    VendorType = []
    MatchingText = []
    CleanedMatchingtext = []
    Vendor = []
    DebitAccount = []
    CreditAccount = []
    
    if len(MissingTypeList) > 0:
        curr_msg = "There are "+str(len(MissingTypeList))+" Types in Transactions file which does not have a match with MatchingText in VendorsMatchFile."
        ut.print_and_log_msg(curr_msg,log_file_path)
        curr_msg = "If these vendors are not created in next step then the corressponding records will not be included in the output journal."
        ut.print_and_log_msg(curr_msg,log_file_path)
        for cleaned_type in MissingTypeList:
            TransactionType = list(TransactionDF[TransactionDF.CleanedType==cleaned_type].head(1).Type)[0]
            create_vendor_response = str(input("Do you want to create Vendor for missing Type: "+TransactionType+". Press 'y' or 'n': ")).strip().lower()
            if create_vendor_response != 'y':
                curr_msg = "User did not want to create vendor for missing Type: "+TransactionType+"."
                ut.print_and_log_msg(curr_msg,log_file_path)
            else:
                curr_msg = "Creating vendor for missing Type: "+TransactionType
                ut.print_and_log_msg(curr_msg,log_file_path)
                vendor_name = input("Enter Vendor for missing Type '"+TransactionType+"': ")
                vendor_debit_account = input("Enter Vendor Debit Account (Gross Amount) for missing Type '"+TransactionType+"': ")
                vendor_credit_account = input("Enter Vendor Credit Account (Gross Amount) for missing Type '"+TransactionType+"': ")
                
                VendorType.append("Paypal")
                MatchingText.append(TransactionType)
                CleanedMatchingtext.append(cleaned_type)
                Vendor.append(vendor_name)
                DebitAccount.append(vendor_debit_account)
                CreditAccount.append(vendor_credit_account)
                
                curr_msg = "Vendor for missing Type: "+TransactionType+" created successfully. "
                ut.print_and_log_msg(curr_msg,log_file_path)
    if len(MatchingText) > 0:
        NewVendorDF = pd.DataFrame.from_dict({'VendorType':VendorType,
                                              'MatchingText':MatchingText,
                                              'CleanedMatchingtext':CleanedMatchingtext,
                                              'Vendor':Vendor,
                                              'DebitAccount':DebitAccount,
                                              'CreditAccount':CreditAccount}) 
        NumberOfNewRecords = NewVendorDF.shape[0]
        FinalVendorsDF = pd.concat([VendorsDF,NewVendorDF], axis=0, ignore_index=True)
        curr_msg = "There are "+str(NumberOfNewRecords)+" new vendors added to VendorsDF. Saving the new Vendors file in the Output directory."
        ut.print_and_log_msg(curr_msg,log_file_path)
        VendorsOutputFilesPath = OutputFilesPath+"vendors_match_new_"+curr_date_time+".csv"
        FinalVendorsDF.to_csv(VendorsOutputFilesPath,index=False)
        return FinalVendorsDF
    else:
        curr_msg = "No new vendors are added. The vendors match file remains unchanged."
        ut.print_and_log_msg(curr_msg,log_file_path)
        return VendorsDF
    
## Function to populate Credit and Debit Account 
def PopulateCreditAndDebitAccount(JoinedDF,log_file_path):
    TransactionDate=[]
    Type=[]
    TransactionID=[]
    DebitAccount=[]
    CreditAccount=[]
    Amount=[]
    ManualEntryDF = JoinedDF[(JoinedDF.DebitAccount == 'Manual Entry') | 
                             ((JoinedDF.Type.str.strip().str.lower() == "express checkout payment") & 
                              ((JoinedDF.Gross < 0) | (JoinedDF.Fee < 0)))].reset_index(drop=True)
    if ManualEntryDF.shape[0] > 0:
        ManualEntryDF.DebitAccount = "Manual Entry"
        ManualEntryDF.CreditAccount = "Manual Entry"
    CorrectEntryDF = JoinedDF[JoinedDF.DebitAccount != 'Manual Entry'].reset_index(drop=True)
    if CorrectEntryDF.shape[0] > 0:
        for index, row in CorrectEntryDF.iterrows():
            CurrentTransactionDate = row['TransactionDate']
            CurrentType = row['Type']
            CurrentTransactionID = row['TransactionID']
            CurrenGrossAmount = row['Gross']
            CurrentFeeAmount = row['Fee']
            CurrentDebitAccount = row['DebitAccount']
            CurrentCreditAccount = row['CreditAccount']
            TransactionDate.append(CurrentTransactionDate)
            Type.append(CurrentType)
            TransactionID.append(CurrentTransactionID)
            DebitAccount.append(CurrentDebitAccount)
            CreditAccount.append(CurrentCreditAccount)
            Amount.append(CurrenGrossAmount)
            if not(CurrentType.strip().lower() in ["general withdrawal"]):
                TransactionDate.append(CurrentTransactionDate)
                Type.append(CurrentType)
                TransactionID.append(CurrentTransactionID)
                Amount.append(CurrentFeeAmount)
                DebitAccount.append("Paypal Fees and Charges")
                CreditAccount.append("Paypal")
        PopulatedDF = pd.DataFrame.from_dict(dict(TransactionDate=TransactionDate,Type=Type,TransactionID=TransactionID,
                                                DebitAccount=DebitAccount,CreditAccount=CreditAccount,Amount=Amount))
    else:
        PopulatedDF = pd.DataFrame.from_dict(dict(TransactionDate=[],Type=[],TransactionID=[],DebitAccount=[],
                                                  CreditAccount=[],Amount=[]))
    return [PopulatedDF,ManualEntryDF]

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
    for index,row in DF.iterrows():
        for i in range(2):
            if i == 0:
                JournalDate.append(row['TransactionDate'])
                ReferenceNumber.append(row['TransactionID'])
                JournalNumberSuffix.append(index+1)
                Notes.append(row['Type'])
                Account.append(row['DebitAccount'])
                Description.append(row['Type'])
                ContactName.append('Paypal')
                Debit.append(row['Amount'])
                Credit.append('')
            else:
                JournalDate.append(row['TransactionDate'])
                ReferenceNumber.append(row['TransactionID'])
                JournalNumberSuffix.append(index+1)
                Notes.append(row['Type'])
                Account.append(row['CreditAccount'])
                Description.append(row['Type'])
                ContactName.append('Paypal')
                Debit.append('')
                Credit.append(row['Amount'])
    JournalDF = pd.DataFrame.from_dict({'Journal Date':JournalDate,'Reference Number': ReferenceNumber, 'Journal Number Suffix':JournalNumberSuffix,'Notes':Notes,'Account':Account,'Description':Description,'Contact Name':ContactName,
                                        'Debit':Debit,'Credit':Credit})
    JournalDF['Journal Type'] = 'both'
    JournalDF['Currency'] = 'USD'
    for col in NonEditableColumns:
        JournalDF[col] = np.nan
    JournalDF = JournalDF[JournalColumns]
    return JournalDF
    
## Function For Validation
def ValidationCheck(joined_df,journal_df,log_file_path):
    MaxSuffix = journal_df['Journal Number Suffix'].max()  
    NumRecords = joined_df.shape[0]
    curr_msg = "Completeness Check -  Number of records in Cleaned Transaction DF: "+str(NumRecords)+" and max value of Journal Suffix: "+str(MaxSuffix)
    ut.print_and_log_msg(curr_msg,log_file_path)
    completeness_check = MaxSuffix==NumRecords
    journal_debit_sum = round(journal_df[journal_df.Debit != ''].Debit.sum(),2)
    journal_credit_sum = round(journal_df[journal_df.Credit != ''].Credit.sum(),2)
    joined_df_amount_sum = round(joined_df.Amount.sum(),2)
    curr_msg = "Accuracy Check1 -  Journal Debit Sum: "+str(journal_debit_sum)+" || Journal Credit Sum: "+str(journal_credit_sum)+" || Transaction Amount Sum: "+str(joined_df_amount_sum)
    ut.print_and_log_msg(curr_msg,log_file_path)
    accuracy_check1 = (journal_debit_sum==joined_df_amount_sum) and (journal_credit_sum == joined_df_amount_sum)
    validation_check = completeness_check and accuracy_check1
    curr_msg = "Paypal Completeness and Accuracy Check Passed: "+str(validation_check)
    ut.print_and_log_msg(curr_msg,log_file_path)
