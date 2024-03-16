import os
import pandas as pd
log_dir = "../logs/"

## Function to create Log file
def CreateLogFile(curr_date_time):
    log_file_name = "logs_"+curr_date_time+".txt"
    log_file_path = os.path.join(log_dir,log_file_name)
    with open(log_file_path, "w") as myfile:
        myfile.write("Starting journal entry process !!")
        myfile.write("\n")
        myfile.write("\n")
    return log_file_path

## Function to write to Log file
def WriteLogs(msg,logfilepath):
    with open(logfilepath, "a") as myfile:
        myfile.write(msg)
        myfile.write("\n")
        myfile.write("\n")
        
## Single Function to log and print messages  
def print_and_log_msg(msg,log_file_path):
    WriteLogs(msg,log_file_path)
    print(msg)
    print()
    
## Function to list input files
def GetInputFiles(input_files_path):
    input_files_path_dict = {}
    dirs =  os.listdir(input_files_path)
    for dir in dirs:
        if dir!='.DS_Store':
            curr_path = os.path.join(input_files_path,dir)
            curr_file_list = os.listdir(curr_path)
            if len(curr_file_list) > 0:
                input_files_path_dict[dir] = os.path.join(curr_path,curr_file_list[0])
    return input_files_path_dict
        
## Function to read CSV files
def ReadCSV(filepath,datecolumns=None):
    try:
        if datecolumns is None:
            CurrDF = pd.read_csv(filepath)
        else:
            CurrDF = pd.read_csv(filepath,parse_dates=datecolumns)
    except Exception as e:
        if datecolumns is None:
            CurrDF = pd.read_csv(filepath,encoding='latin1')
        else:
            CurrDF = pd.read_csv(filepath,parse_dates=datecolumns,encoding='latin1')
    return CurrDF
        
## Function to clean matching values
def CleanValue(curr_val):
    curr_val = curr_val.lower().strip()
    curr_val_splitted = curr_val.split()
    curr_val = " ".join(item for item in curr_val_splitted if len(item.strip())>0)
    return curr_val

## Function to read and clean Vendor Match File
def PreprocessVendorMatchFile():
    VendorsDFColumns = ['VendorType','MatchingText','CleanedMatchingtext','Vendor','DebitAccount','CreditAccount']
    vendor_match_file_path = "../vendor_match/vendor_match.csv"
    VendorsDF = ReadCSV(vendor_match_file_path)
    VendorsDF = VendorsDF.dropna(subset=['MatchingText'],axis=0).drop_duplicates().reset_index(drop=True)
    MatchingTextCleaned = []
    for index,row in VendorsDF.iterrows():
        curr_match_text = row['MatchingText']
        curr_match_text = CleanValue(curr_match_text)
        MatchingTextCleaned.append(curr_match_text)
    VendorsDF['CleanedMatchingtext'] = MatchingTextCleaned
    VendorsDF = VendorsDF.reset_index(drop=True)[VendorsDFColumns]
    return VendorsDF

## Convert Negatives to Positives in Journals
def ConvertNegativeToPositiveInJournals(DF):
    DebitABS = []
    CreditABS = []
    for item in list(DF['Debit']):
        if item != '':
            DebitABS.append(abs(item))
        else:
            DebitABS.append(item)
    for item in list(DF['Credit']):
        if item != '':
            CreditABS.append(abs(item))
        else:
            CreditABS.append(item)
    DF['Debit'] = DebitABS
    DF['Credit'] = CreditABS
    return DF
def updateDescriptionLogic(category,description,amount):
    updatedDescription=""
    if category=='Payment/Credit':
        updatedDescription="This is a payment of the CAPITAL ONE Credit Card Balance "
    elif category!='Payment/Credit' and amount>0:
        updatedDescription="This is a payment to a Vendor "+description
    elif category!='Payment/Credit' and amount<0:
        updatedDescription="This is a Refund to our Credit Card "+description
    else:
        updatedDescription=description
    return updatedDescription
    
    
    