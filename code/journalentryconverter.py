import pandas as pd
from datetime import datetime as dt
import traceback
import utils as ut
from individual_converters import capital_one as co
from individual_converters import upwork as up
from individual_converters import mercury as mer
from individual_converters import paypal as pp
from individual_converters import stripe as sp
from individual_converters import wise as ws
    
if __name__=="__main__":
    shouldProceed = True
    # 1. Create Log File For Current Transaction
    curr_date = "_".join(str(dt.now().date()).split("-"))
    curr_time = "_".join(str(dt.now().time()).split(".")[0].split(":"))
    curr_date_time = curr_date+"_"+curr_time
    log_file_path = ut.CreateLogFile(curr_date_time)
    
    # 2. Get Input Files
    try:
        input_files_path = "..\\input_files"
        input_files_path_dict = ut.GetInputFiles(input_files_path)
        selected_files = ["\\".join(item.split("\\")[-2:]) for item in list(input_files_path_dict.values())]
        print()
        curr_msg = "There are "+str(len(input_files_path_dict))+" input files:"
        ut.print_and_log_msg(curr_msg,log_file_path)
        for pos,selfile in enumerate(selected_files):
            curr_msg = str(pos+1)+". "+selfile
            ut.print_and_log_msg(curr_msg,log_file_path)
    except Exception as e:
        shouldProceed = False
        curr_msg = "Error !! Stopping Transaction due to the following exception: \n\n"+traceback.format_exc()
        ut.print_and_log_msg(curr_msg,log_file_path)
    
    # 3. Read and Clean Vendor Match File
    if shouldProceed:
        curr_msg = "Preprocessing Vendor Match File"
        ut.print_and_log_msg(curr_msg,log_file_path)
        try:
            CleanedVendorsDF = ut.PreprocessVendorMatchFile()
            curr_msg = "Vendor Match File preprocessed successfully."
            ut.print_and_log_msg(curr_msg,log_file_path)
        except Exception as e:
            shouldProceed = False
            curr_msg = "Error !! Stopping Transaction due to the following exception: \n\n"+traceback.format_exc()
            ut.print_and_log_msg(curr_msg,log_file_path)
            
    # 4. Loop over input files
    if shouldProceed:
        JournalDFList = []
        suffix_to_be_added = 0
        TotalsDict = dict(Type=[],Amount=[])
        try:
            for input_file_type, input_file_path in input_files_path_dict.items():
                # print("suffix_to_be_added:",suffix_to_be_added)
                # print(input_file_path)
                input_file_name = input_file_path.split("\\")[-1]
                curr_msg = "-------------------------------------------------------------------------------------------------------"
                ut.print_and_log_msg(curr_msg,log_file_path)
                curr_msg = "-------------------------------------------------------------------------------------------------------"
                ut.print_and_log_msg(curr_msg,log_file_path)
                curr_msg = "Processing "+input_file_name+" of account transaction type: "+input_file_type+"."
                ut.print_and_log_msg(curr_msg,log_file_path)
                curr_msg = "-------------------------------------------------------------------------------------------------------"
                ut.print_and_log_msg(curr_msg,log_file_path)
                ## Define Output Directory Path
                input_file_path_splitted = input_file_path.split("\\")
                print("input_file_path_splitted:",input_file_path_splitted)
                output_files_dir = "..\\output_files\\individual_outputs\\"+input_file_path_splitted[2]+"\\"
                ## Check Filetype e.g. capital_one, upwork, wise etc.
                if input_file_type.strip().lower() == "capital_one":
                    ## Read Transactions File
                    CurrDateColumns = ["Posted Date"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("capital_one")
                    TotalsDict["Amount"].append(TransactionDF.Debit.sum()-TransactionDF.Credit.sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean the Description column for matching."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Clean TransactionDF
                    CleanedTransactionDF = co.PreprocessTransactionsFile(TransactionDF)
                    curr_msg = "Description column of "+input_file_name+" cleaned successfully. Starting to check missing vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Add missing vendors
                    FinalVendorsDF = co.VendorsCheck(CleanedTransactionDF,CleanedVendorsDF,output_files_dir,log_file_path,curr_date_time)
                    curr_msg = "Final Vendors File created successfully. Starting to join Transaction with Vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Join Transactions with Vendors
                    
                    JoinedColumns = ['Posted Date','Vendor','DebitAccount','CreditAccount','Amount','Debit','Credit','Category']
                    JoinedDF = CleanedTransactionDF.merge(FinalVendorsDF,left_on="CleanedDescription",right_on='CleanedMatchingtext')
                    JoinedDF = JoinedDF[JoinedColumns].reset_index(drop=True)
                    curr_msg = "Transaction with Vendors joined successfully. Starting to create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Create Journal
                    CapitalOneJournalDF = co.CreateJournal(JoinedDF)
                    curr_msg = "Capital One JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Check Validation
                    co.ValidationCheck(JoinedDF,CapitalOneJournalDF,log_file_path)
                    ## Get Absolute Value in Journals
                    CapitalOneJournalDF = ut.ConvertNegativeToPositiveInJournals(CapitalOneJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    CapitalOneJournalDF.to_csv(journal_file_path,index=False)
                    if CapitalOneJournalDF.shape[0] > 0:
                        CapitalOneJournalDF['Journal Number Suffix'] = CapitalOneJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(CapitalOneJournalDF)
                        suffix_to_be_added = CapitalOneJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Capital One JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                if input_file_type.strip().lower() == "upwork":  
                    ## Read Transactions File
                    CurrDateColumns = ["Date"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("upwork")
                    TotalsDict["Amount"].append(TransactionDF.Amount.sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean the Description column for matching." 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Clean TransactionDF
                    CleanedTransactionDF = up.PreprocessTransactionsFile(TransactionDF)
                    curr_msg = "Freelancer column of "+input_file_name+" cleaned successfully. Starting to check if Upwork is missing in vendors file."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    # print(CleanedTransactionDF)
                    ## Add missing vendors
                    UpworkVendorsDF = CleanedVendorsDF.copy()
                    UpworkVendorsDF = UpworkVendorsDF[UpworkVendorsDF.VendorType == 'Upwork']
                    if UpworkVendorsDF[UpworkVendorsDF['CleanedMatchingtext']=='upwork'].shape[0] == 0:
                        UpworkDF = pd.DataFrame.from_dict({'MatchingText':['Upwork'],
                                                           'CleanedMatchingtext':['upwork'],
                                                           'Vendor':['Upwork'],
                                                           'DebitAccount':['Contractor Expenses'],
                                                           'CreditAccount':['Calculated']})
                        UpworkVendorsDF = pd.concat([UpworkVendorsDF, UpworkDF], ignore_index = True)
                    ## Add missing vendors
                    FinalVendorsDF = up.VendorsCheck(CleanedTransactionDF,UpworkVendorsDF,output_files_dir,log_file_path,curr_date_time)
                    curr_msg = "Final Vendors File created successfully. Starting to join Transaction with Vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    # print("FinalVendorsDF:")
                    # print(FinalVendorsDF)
                    ## Create Transactions and Vendor Joined DF
                    JoinedColumns = ['Date','Ref ID','Type','Description','Vendor','Amount']
                    JoinedDF = CleanedTransactionDF.merge(FinalVendorsDF,left_on="FreelancerCleaned",right_on='CleanedMatchingtext')
                    JoinedDF = JoinedDF[JoinedColumns]
                    # print(JoinedDF)
                    curr_msg = "Transaction and Vendors file joined successfully. Starting to calculate the Credit and Debit Account."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Calculate DebitAccount and CreditAccount in JoinedDF
                    UpworkDFs = up.PopulateCreditAndDebitAccount(JoinedDF,JoinedColumns,log_file_path)
                    curr_msg = "Credit and Debit Account calculated successfully. Starting to Create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    FixedContractDF = UpworkDFs[0]
                    CleanedJoinedDF = UpworkDFs[1]
                    if FixedContractDF.shape[0] > 0:
                        FixedContractPath = output_files_dir+"FixedContracts_"+curr_date_time+".csv"
                        FixedContractDF.to_csv(FixedContractPath,index=False)
                        curr_msg = "Fixed Contract file written successfully. Continuing to Create JournalDF."
                        ut.print_and_log_msg(curr_msg,log_file_path)
                    UpworkJournalDF = up.CreateJournal(CleanedJoinedDF)
                    curr_msg = "Upwork JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Check Validation
                    up.ValidationCheck(CleanedJoinedDF,UpworkJournalDF,log_file_path)
                    ## Get Absolute Value in Journals
                    UpworkJournalDF = ut.ConvertNegativeToPositiveInJournals(UpworkJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    UpworkJournalDF.to_csv(journal_file_path,index=False)
                    if UpworkJournalDF.shape[0] > 0:
                        UpworkJournalDF['Journal Number Suffix'] = UpworkJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(UpworkJournalDF)
                        suffix_to_be_added = UpworkJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Upwork JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                if input_file_type.strip().lower() == "mercury_3320":  
                    ## Read Transactions File
                    CurrDateColumns = ["Date (UTC)"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("mercury_3320")
                    TotalsDict["Amount"].append(TransactionDF.Amount.sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean the Description column for matching." 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Clean TransactionDF
                    CleanedTransactionDF = mer.PreprocessTransactionsFile(TransactionDF)
                    curr_msg = "Description column of "+input_file_name+" cleaned successfully. Starting to check for missing vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Add missing vendors
                    MercuryVendorsDF = CleanedVendorsDF.copy()
                    FinalVendorsDF = mer.VendorsCheck(CleanedTransactionDF,MercuryVendorsDF,output_files_dir,log_file_path,curr_date_time)
                    curr_msg = "Final Vendors File created successfully. Starting to join Transaction with Vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Join Transactions with Vendors
                    JoinedColumns = ['TransactionDate','Description','Bank Description','Vendor','DebitAccount','CreditAccount','Amount']
                    JoinedDF = CleanedTransactionDF.merge(FinalVendorsDF,left_on="CleanedDescription",right_on='CleanedMatchingtext')
                    JoinedDF = JoinedDF[JoinedColumns].reset_index(drop=True)
                    curr_msg = "Transaction with Vendors joined successfully. Starting to create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Calculate DebitAccount and CreditAccount in JoinedDF
                    MercuryDF = mer.PopulateCreditAndDebitAccount(JoinedDF,log_file_path)
                    curr_msg = "Credit and Debit Account calculated successfully. Starting to Create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Create Journal
                    MercuryJournalDF = mer.CreateJournal(JoinedDF)
                    curr_msg = "Mercury 3320 JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Check Validation
                    mer.ValidationCheck(JoinedDF,MercuryJournalDF,log_file_path)
                    ## Get Absolute Value in Journals
                    MercuryJournalDF = ut.ConvertNegativeToPositiveInJournals(MercuryJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    MercuryJournalDF.to_csv(journal_file_path,index=False)
                    if MercuryJournalDF.shape[0] > 0:
                        MercuryJournalDF['Journal Number Suffix'] = MercuryJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(MercuryJournalDF)
                        suffix_to_be_added = MercuryJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Mercury 3320 JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                if input_file_type.strip().lower() == "mercury_9493":  
                    ## Read Transactions File
                    CurrDateColumns = ["Date (UTC)"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("mercury_9493")
                    TotalsDict["Amount"].append(TransactionDF.Amount.sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean the Description column for matching." 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Clean TransactionDF
                    CleanedTransactionDF = mer.PreprocessTransactionsFile(TransactionDF)
                    curr_msg = "Description column of "+input_file_name+" cleaned successfully. Starting to check for missing vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Add missing vendors
                    MercuryVendorsDF = CleanedVendorsDF.copy()
                    FinalVendorsDF = mer.VendorsCheck(CleanedTransactionDF,MercuryVendorsDF,output_files_dir,log_file_path,curr_date_time)
                    curr_msg = "Final Vendors File created successfully. Starting to join Transaction with Vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Join Transactions with Vendors
                    JoinedColumns = ['TransactionDate','Description','Bank Description','Vendor','DebitAccount','CreditAccount','Amount']
                    JoinedDF = CleanedTransactionDF.merge(FinalVendorsDF,left_on="CleanedDescription",right_on='CleanedMatchingtext')
                    JoinedDF = JoinedDF[JoinedColumns].reset_index(drop=True)
                    curr_msg = "Transaction with Vendors joined successfully. Starting to create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Calculate DebitAccount and CreditAccount in JoinedDF
                    MercuryDF = mer.PopulateCreditAndDebitAccount(JoinedDF,log_file_path)
                    curr_msg = "Credit and Debit Account calculated successfully. Starting to Create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Create Journal
                    MercuryJournalDF = mer.CreateJournal(JoinedDF)
                    curr_msg = "Mercury 9493 JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Check Validation
                    mer.ValidationCheck(JoinedDF,MercuryJournalDF,log_file_path)
                    ## Get Absolute Value in Journals
                    MercuryJournalDF = ut.ConvertNegativeToPositiveInJournals(MercuryJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    MercuryJournalDF.to_csv(journal_file_path,index=False)
                    if MercuryJournalDF.shape[0] > 0:
                        MercuryJournalDF['Journal Number Suffix'] = MercuryJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(MercuryJournalDF)
                        suffix_to_be_added = MercuryJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Mercury 9493 JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                if input_file_type.strip().lower() == "paypal": 
                    ## Read Transactions File
                    CurrDateColumns = ["Date"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("paypal")
                    TotalsDict["Amount"].append(TransactionDF.Net.sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean the Type column for matching." 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Clean TransactionDF
                    CleanedTransactionDF = pp.PreprocessTransactionsFile(TransactionDF)
                    curr_msg = "Description column of "+input_file_name+" cleaned successfully. Starting to check for missing vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Add missing vendors
                    PaypalVendorsDF = CleanedVendorsDF.copy()
                    FinalVendorsDF = pp.VendorsCheck(CleanedTransactionDF,PaypalVendorsDF,output_files_dir,log_file_path,curr_date_time)
                    curr_msg = "Final Vendors File created successfully. Starting to join Transaction with Vendors."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Join Transactions with Vendors
                    JoinedColumns = ['TransactionDate','Type','TransactionID','DebitAccount','CreditAccount','Gross','Fee']
                    JoinedDF = CleanedTransactionDF.merge(FinalVendorsDF,left_on="CleanedType",right_on='CleanedMatchingtext')
                    JoinedDF = JoinedDF[JoinedColumns].reset_index(drop=True)
                    curr_msg = "Transaction with Vendors joined successfully. Starting to create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Calculate DebitAccount and CreditAccount in JoinedDF
                    PaypalDFs = pp.PopulateCreditAndDebitAccount(JoinedDF,log_file_path)
                    curr_msg = "Credit and Debit Account calculated successfully. Starting to Create JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    CleanedJoinedDF = PaypalDFs[0]
                    ManualEntryDF = PaypalDFs[1]
                    if ManualEntryDF.shape[0] > 0:
                        ManualEntryPath = output_files_dir+"ManualEntryDF_"+curr_date_time+".csv"
                        ManualEntryDF.to_csv(ManualEntryPath,index=False)
                        curr_msg = "Manual Entry file written successfully. Continuing to Create JournalDF."
                        ut.print_and_log_msg(curr_msg,log_file_path)
                    PaypalJournalDF = pp.CreateJournal(CleanedJoinedDF)
                    curr_msg = "Paypal JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Check Validation
                    pp.ValidationCheck(CleanedJoinedDF,PaypalJournalDF,log_file_path)
                    ## Get Absolute Value in Journals
                    PaypalJournalDF = ut.ConvertNegativeToPositiveInJournals(PaypalJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    PaypalJournalDF.to_csv(journal_file_path,index=False)
                    if PaypalJournalDF.shape[0] > 0:
                        PaypalJournalDF['Journal Number Suffix'] = PaypalJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(PaypalJournalDF)
                        suffix_to_be_added = PaypalJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Paypal JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                if input_file_type.strip().lower() == "stripe": 
                    ## Read Transactions File
                    CurrDateColumns = ["Created (UTC)"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("mercury_3320")
                    TotalsDict["Amount"].append(TransactionDF.Net.sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean Transaction file." 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    CleanedTransactionDF = sp.PreprocessTransactionsFile(TransactionDF)
                    StripeJournalDF = sp.CreateJournal(CleanedTransactionDF)
                    curr_msg = "Stripe JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Get Absolute Value in Journals
                    StripeJournalDF = ut.ConvertNegativeToPositiveInJournals(StripeJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    StripeJournalDF.to_csv(journal_file_path,index=False)
                    if StripeJournalDF.shape[0] > 0:
                        StripeJournalDF['Journal Number Suffix'] = StripeJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(StripeJournalDF)
                        suffix_to_be_added = StripeJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Stripe JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                if input_file_type.strip().lower() == "wise": 
                    CurrDateColumns = ["Created on"]
                    TransactionDF = ut.ReadCSV(input_file_path,CurrDateColumns)
                    TotalsDict["Type"].append("mercury_3320")
                    TotalsDict["Amount"].append(TransactionDF['Source fee amount'].sum()+TransactionDF['Source amount (after fees)'].sum())
                    curr_msg = input_file_name+" read successfully. Starting to Clean Transaction file." 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    CleanedTransactionDF = ws.PreprocessTransactionsFile(TransactionDF)
                    curr_msg = "Transaction file "+input_file_name+" cleaned successfully. Starting to write JournalDF" 
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    WiseJournalDF = ws.CreateJournal(CleanedTransactionDF)
                    curr_msg = "Wise JournalDF created successfully. Starting to write JournalDF."
                    # ut.print_and_log_msg(curr_msg,log_file_path)
                    ## Get Absolute Value in Journals
                    WiseJournalDF = ut.ConvertNegativeToPositiveInJournals(WiseJournalDF)
                    ## Write Journal
                    journal_file_path = output_files_dir+"journals_"+curr_date_time+".csv"
                    WiseJournalDF.to_csv(journal_file_path,index=False)
                    if WiseJournalDF.shape[0] > 0:
                        WiseJournalDF['Journal Number Suffix'] = WiseJournalDF['Journal Number Suffix']+suffix_to_be_added
                        JournalDFList.append(WiseJournalDF)
                        suffix_to_be_added = WiseJournalDF['Journal Number Suffix'].max()
                    curr_msg = "Wise JournalDF written successfully."
                    ut.print_and_log_msg(curr_msg,log_file_path)
                else:
                    pass
            if len(JournalDFList) > 0:
                combined_journals_dir = "..\\output_files\\"
                combined_journals_path = combined_journals_dir+"journals_"+curr_date_time+".csv"
                CombinedJournalDF = pd.concat(JournalDFList, axis=0).reset_index(drop=True)
                CombinedJournalDF.to_csv(combined_journals_path,index=False)
                curr_msg = "Combined JournalDF written successfully."
                ut.print_and_log_msg(curr_msg,log_file_path)
            else:
                curr_msg = "There are no rows in any of the journals dataframe. Hence Combined JournalDF will not be written."
                ut.print_and_log_msg(curr_msg,log_file_path)
            TotalsDF = pd.DataFrame.from_dict(TotalsDict) 
            TotalsDF.to_csv("..\\output_files\\totals_"+curr_date_time+".csv",index=False)
        except Exception as e:
            shouldProceed = False
            curr_msg = "Error !! Stopping Transaction due to the following exception: \n\n"+traceback.format_exc()
            ut.print_and_log_msg(curr_msg,log_file_path)        
    print()