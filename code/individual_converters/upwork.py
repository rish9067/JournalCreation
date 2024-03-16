import utils as ut
import pandas as pd
import numpy as np

## Function to Clean Upwork Accounts Transactions
def PreprocessTransactionsFile(UpworkTransactionDF):
    TransactionDF = UpworkTransactionDF.copy()
    TransactionDFColumns = ['Date','Ref ID','Type','Description','Freelancer','Amount']
    TransactionDF["Date"] = TransactionDF["Date"].dt.strftime('%m/%d/%Y')
    TransactionDF = TransactionDF[~TransactionDF.Amount.isnull()]
    TransactionDF = TransactionDF[TransactionDFColumns].sort_values(by='Date').reset_index(drop=True)
    TransactionDF.loc[TransactionDF.Freelancer.isnull(),'Freelancer']='Upwork'
    FreelancerCleanedList = []
    for index, row in TransactionDF.iterrows():
        FreelancerCleanedList.append(ut.CleanValue(row['Freelancer']))
    TransactionDF['FreelancerCleaned'] = FreelancerCleanedList
    return TransactionDF

## Function to Check Upwork Missing Vendor
def VendorsCheck(TransactionDF,VendorsDF,OutputFilesPath,log_file_path,curr_date_time):
    MissingVendorList = list(set(TransactionDF.FreelancerCleaned).difference(set(VendorsDF.CleanedMatchingtext)))
    
    VendorType = []
    MatchingText = []
    CleanedMatchingtext = []
    Vendor = []
    DebitAccount = []
    CreditAccount = []
    
    if len(MissingVendorList) > 0:
        
        curr_msg = "There are "+str(len(MissingVendorList))+" Freelancer in Transactions file which does not have a match with MatchingText in VendorsMatchFile."
        ut.print_and_log_msg(curr_msg,log_file_path)
        curr_msg = "If these vendors are not created in next step then the corressponding records will not be included in the output journal."
        ut.print_and_log_msg(curr_msg,log_file_path)
        
        for cleaned_freelancer in MissingVendorList:
            TransactionFreelancer = list(TransactionDF[TransactionDF.FreelancerCleaned==cleaned_freelancer].head(1).Freelancer)[0]
            create_vendor_response = str(input("Do you want to create missing Freelancer: "+TransactionFreelancer+". Press 'y' or 'n': ")).strip().lower()
            if create_vendor_response != 'y':
                curr_msg = "User did not want to create vendor for missing Freelancer: "+TransactionFreelancer+"."
                ut.print_and_log_msg(curr_msg,log_file_path)
            else:
                curr_msg = "Creating vendor for missing Freelancer: "+TransactionFreelancer
                ut.print_and_log_msg(curr_msg,log_file_path)
                vendor_name = input("Enter Vendor for missing Freelancer '"+TransactionFreelancer+"': ")
                MatchingText.append(TransactionFreelancer)
                CleanedMatchingtext.append(cleaned_freelancer)
                Vendor.append(vendor_name)
                DebitAccount.append('Calculated')
                CreditAccount.append('Calculated')
                VendorType.append("Upwork")
                curr_msg = "Vendor for missing Freelancer: "+TransactionFreelancer+" created successfully. "
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

## Function to Create Credit and Debit Account
def CreateCreditAndDebitAccount(des,vendor):
    CreditAccount='Unknown'
    DebitAccount='Unknown'
    if des.startswith('invoice for'):
        CreditAccount = 'Accounts Payable - Upwork'
        if vendor in ['abhi mukhopadhyay','boaz sasson']:
            DebitAccount = 'Software Development Expenses'
        else:
            DebitAccount = 'Contractor Expenses'
            
    elif des.startswith('payment processing fee'):
        CreditAccount = 'Accounts Payable - Upwork'
        DebitAccount = 'Upwork Processing Fee'
        
    elif des.startswith('marketplace fee'):
        CreditAccount = 'Accounts Payable - Upwork'
        DebitAccount = 'Upwork Marketplace Fee'
        
    elif des.startswith('contract initiation fee'):
        CreditAccount = 'Accounts Payable - Upwork'
        DebitAccount = 'Upwork Contract Initiation Fee'
        
    elif des.startswith('paid from visa 5983 to escrow'):
        CreditAccount = 'Capital One'
        DebitAccount = 'Upwork Escrow Fund'
        
    elif des.startswith('paid from visa'):
        CreditAccount = 'Capital One'
        DebitAccount = 'Accounts Payable - Upwork'
        
    elif des.startswith('paid from escrow'):
        CreditAccount = 'Upwork Escrow Fund'
        DebitAccount = 'Accounts Payable - Upwork'
        
    elif des.startswith('refund') or des.startswith('credit for') or des.startswith('funding request for'):
        CreditAccount = 'Manual Entry'
        DebitAccount = 'Manual Entry'

    return [CreditAccount,DebitAccount]

## Function to populate Credit and Debit Account 
def PopulateCreditAndDebitAccount(JoinedDF,JoinedColumns,log_file_path):
    CreditAccountList=[]
    DebitAccountList=[]
    for index, row in JoinedDF.iterrows():
        curr_des = row['Description'].strip().lower()
        curr_vendor = row['Vendor'].strip().lower()
        Accounts = CreateCreditAndDebitAccount(curr_des,curr_vendor)
        CreditAccount = Accounts[0]
        DebitAccount = Accounts[1]
        CreditAccountList.append(CreditAccount)
        DebitAccountList.append(DebitAccount)
        curr_msg = "The Transaction with Ref ID:'"+str(row['Ref ID'])+"' and Description:'"+row['Description']+"' related to a fixed contract? Please input a manual entry in Zoho. You can find this transaction inside Output folder in the file FixedContract.csv."
        ut.print_and_log_msg(curr_msg,log_file_path)
    JoinedDF['DebitAccount'] = DebitAccountList
    JoinedDF['CreditAccount'] = CreditAccountList
    FixedContractDF = JoinedDF[JoinedDF.DebitAccount=='Manual Entry'][JoinedColumns].reset_index(drop=True)
    CleanedJoinedDF = JoinedDF[JoinedDF.DebitAccount!='Manual Entry'].reset_index(drop=True)
    return [FixedContractDF,CleanedJoinedDF]

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
                JournalDate.append(row['Date'])
                ReferenceNumber.append(row['Ref ID'])
                JournalNumberSuffix.append(index+1)
                Notes.append(row['Type'])
                Account.append(row['DebitAccount'])
                Description.append(row['Description'])
                ContactName.append(row['Vendor'])
                Debit.append(row['Amount'])
                Credit.append('')
            else:
                JournalDate.append(row['Date'])
                ReferenceNumber.append(row['Ref ID'])
                JournalNumberSuffix.append(index+1)
                Notes.append(row['Type'])
                Account.append(row['CreditAccount'])
                Description.append(row['Description'])
                ContactName.append(row['Vendor'])
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
    curr_msg = "Upwork Completeness and Accuracy Check Passed: "+str(validation_check)
    ut.print_and_log_msg(curr_msg,log_file_path)
