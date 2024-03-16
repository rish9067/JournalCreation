import utils as ut
import pandas as pd
import numpy as np

## Function to Clean Mercury Accounts Transactions
def PreprocessTransactionsFile(MercuryTransactionDF):
    TransactionDF = MercuryTransactionDF.copy()
    
    TransactionDFColumns = ['Date (UTC)', 'Description','Bank Description','Amount']
    TransactionDF["Date (UTC)"] = TransactionDF["Date (UTC)"].dt.strftime('%m/%d/%Y')
    TransactionDF = TransactionDF[~TransactionDF.Amount.isnull()]
    TransactionDF = TransactionDF[~TransactionDF.Description.isnull()]
    TransactionDF = TransactionDF[TransactionDFColumns].sort_values(by='Date (UTC)').reset_index(drop=True)
    # print(TransactionDF)
    DescriptionCleanedList = []
    for index, row in TransactionDF.iterrows():
        DescriptionCleanedList.append(ut.CleanValue(row['Description']))
    TransactionDF['CleanedDescription'] = DescriptionCleanedList
    TransactionDF = TransactionDF[TransactionDFColumns+['CleanedDescription']]
    TransactionDF.columns = ['TransactionDate','Bank Description','Description','Amount','CleanedDescription']
    # print(TransactionDF)
    return TransactionDF

## Function to Check Upwork Missing Vendor
def VendorsCheck(TransactionDF,VendorsDF,OutputFilesPath,log_file_path,curr_date_time):
    MissingDescriptionList = list(set(TransactionDF.CleanedDescription).difference(set(VendorsDF.CleanedMatchingtext)))
    
    VendorType = []
    MatchingText = []
    CleanedMatchingtext = []
    Vendor = []
    DebitAccount = []
    CreditAccount = []
    
    CleanedDescriptionWithoutProbableMatch = []
    
    if len(MissingDescriptionList) > 0:
        UniqueVendorsCleanedMatchingText = list(set(VendorsDF.CleanedMatchingtext))
        for curr_cleaned_description in MissingDescriptionList:
            ProbableMatches = [item for item in UniqueVendorsCleanedMatchingText if curr_cleaned_description.startswith(item)]
            if len(ProbableMatches) == 0:
                CleanedDescriptionWithoutProbableMatch.append(curr_cleaned_description)
            else:
                CurrMatch = ProbableMatches[0]
                MatchedVendorRow = VendorsDF[VendorsDF.CleanedMatchingtext==CurrMatch].head(1)
                
                CurrVendor = list(MatchedVendorRow.Vendor)[0]
                CurrDebitAccount = list(MatchedVendorRow.DebitAccount)[0]
                CurrCreditAccount = list(MatchedVendorRow.CreditAccount)[0]
                TransactionDescription = list(TransactionDF[TransactionDF.CleanedDescription==curr_cleaned_description].head(1).Description)[0]
                
                curr_msg = "Description: '"+TransactionDescription+"' in Transactions File starts with MatchingText: "+list(MatchedVendorRow.MatchingText)[0]+" in Vendors Match File."
                # ut.print_and_log_msg(curr_msg,log_file_path)
                
                VendorType.append("Mercury")
                MatchingText.append(TransactionDescription)
                CleanedMatchingtext.append(curr_cleaned_description)
                Vendor.append(CurrVendor)
                DebitAccount.append(CurrDebitAccount)
                CreditAccount.append(CurrCreditAccount)
    
    if len(CleanedDescriptionWithoutProbableMatch) > 0:
        curr_msg = "There are "+str(len(CleanedDescriptionWithoutProbableMatch))+" Descriptions in Transactions file which does not have a match with MatchingText in VendorsMatchFile."
        ut.print_and_log_msg(curr_msg,log_file_path)
        curr_msg = "If these vendors are not created in next step then the corressponding records will not be included in the output journal."
        ut.print_and_log_msg(curr_msg,log_file_path)
        for cleaned_des in CleanedDescriptionWithoutProbableMatch:
            TransactionDescription = list(TransactionDF[TransactionDF.CleanedDescription==cleaned_des].head(1).Description)[0]
            create_vendor_response = str(input("Do you want to create Vendor for missing Description: "+TransactionDescription+". Press 'y' or 'n': ")).strip().lower()
            if create_vendor_response != 'y':
                curr_msg = "User did not want to create vendor for missing Description: "+TransactionDescription+"."
                ut.print_and_log_msg(curr_msg,log_file_path)
            else:
                curr_msg = "Creating vendor for missing Description: "+TransactionDescription
                ut.print_and_log_msg(curr_msg,log_file_path)
                vendor_name = input("Enter Vendor for missing Description '"+TransactionDescription+"': ")
                vendor_debit_account = input("Enter Vendor Debit Account for missing Description '"+TransactionDescription+"': ")
                vendor_credit_account = input("Enter Vendor Credit Account for missing Description '"+TransactionDescription+"': ")
                
                VendorType.append("Mercury")
                MatchingText.append(TransactionDescription)
                CleanedMatchingtext.append(cleaned_des)
                Vendor.append(vendor_name)
                DebitAccount.append(vendor_debit_account)
                CreditAccount.append(vendor_credit_account)
                
                curr_msg = "Vendor for missing Description: "+TransactionDescription+" created successfully. "
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
        VendorsOutputFilesPath = OutputFilesPath+"vendors_match_new"+curr_date_time+".csv"
        FinalVendorsDF.to_csv(VendorsOutputFilesPath,index=False)
        return FinalVendorsDF
    else:
        curr_msg = "No new vendors are added. The vendors match file remains unchanged."
        ut.print_and_log_msg(curr_msg,log_file_path)
        return VendorsDF
    
## Function to populate Credit and Debit Account 
def PopulateCreditAndDebitAccount(JoinedDF,log_file_path):
    CreditAccountList=[]
    DebitAccountList=[]
    for index, row in JoinedDF.iterrows():
        CurrenAmount = row['Amount']
        CurrentVendor = row['Vendor'].strip().lower()
        if CurrentVendor in ['andrey sergeev','guadalupe vilariño marsilli']:
            CreditAccountList.append(row['CreditAccount'])
            DebitAccountList.append(row['DebitAccount'])
        elif CurrenAmount > 0:
            CreditAccountList.append(row['CreditAccount'])
            DebitAccountList.append(row['DebitAccount'])
        else:
            CreditAccountList.append(row['DebitAccount'])
            DebitAccountList.append(row['CreditAccount'])
    JoinedDF['DebitAccount'] = DebitAccountList
    JoinedDF['CreditAccount'] = CreditAccountList
    return JoinedDF

## Function to create JournalDF
def CreateJournal(DF):
    JournalColumns = ['Journal Date','Reference Number','Journal Number Prefix','Journal Number Suffix','Notes','Journal Type','Currency','Account','Description','Contact Name',
                      'Debit','Credit','Tax Name','Tax Type','Tax Percentage','Exemption Code','Tax Authority','Item Tax Exemption Reason','Project Name']
    NonEditableColumns = ['Reference Number', 'Journal Number Prefix','Tax Name','Tax Type', 'Tax Percentage', 'Exemption Code', 
                          'Tax Authority','Item Tax Exemption Reason', 'Project Name']
    JournalDate = []
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
                JournalNumberSuffix.append(index+1)
                if row['Amount'] > 0:
                    Notes.append('Deposit')
                else:
                    Notes.append('Payment')
                Account.append(row['DebitAccount'])
                Description.append(row['Description'])
                ContactName.append(row['Bank Description'])
                Debit.append(row['Amount'])
                Credit.append('')
            else:
                JournalDate.append(row['TransactionDate'])
                JournalNumberSuffix.append(index+1)
                if row['Amount'] > 0:
                    Notes.append('Deposit')
                else:
                    Notes.append('Payment')
                Account.append(row['CreditAccount'])
                Description.append(row['Description'])
                ContactName.append(row['Bank Description'])
                Debit.append('')
                Credit.append(row['Amount'])
    JournalDF = pd.DataFrame.from_dict({'Journal Date':JournalDate, 'Journal Number Suffix':JournalNumberSuffix,'Notes':Notes,'Account':Account,
                                        'Description':Description,'Contact Name':ContactName,'Debit':Debit,'Credit':Credit})
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
    journal_debit_sum = journal_df[journal_df.Debit != ''].Debit.sum()
    journal_credit_sum = journal_df[journal_df.Credit != ''].Credit.sum()
    joined_df_debit_credit_sum = joined_df.Amount.sum()
    curr_msg = "Accuracy Check1 -  Journal Debit Sum: "+str(journal_debit_sum)+" || Journal Credit Sum: "+str(journal_credit_sum)+" || Transaction Debit and Credit Sum: "+str(joined_df_debit_credit_sum)
    ut.print_and_log_msg(curr_msg,log_file_path)
    accuracy_check1 = (journal_debit_sum==joined_df_debit_credit_sum) and (journal_credit_sum == joined_df_debit_credit_sum)
    validation_check = completeness_check and accuracy_check1
    curr_msg = "Mercury Completeness and Accuracy Check Passed: "+str(validation_check)
    ut.print_and_log_msg(curr_msg,log_file_path)