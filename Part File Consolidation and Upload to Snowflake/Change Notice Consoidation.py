""" Part Change Notice Consolidation,  04/28/2023, Ryan Buzar

Takes several excel files and consolidates them into a single dataframe. Can either upload the table to a snowflake 
table or export the data in equally sized csv files for backup or use by other users that do not have snowflake access.


Functions:
    :get_excel_file_list:
        - Navigates to the file directory and creates a list of all files ending with the '.xlsx' file suffix
        Arguments: 
            filepath - File directory that of files that are desired to be consolidated
        Returns:
            - A list of files ending with the '.xlsx' file type

    :get_part_data:
        - Reads an excel file and loads the columns and data into a pandas dataframe and standardizes column names
        Arguments:
            file - Complete file name with file type suffix
            filepath - File directory that of files that are desired to be consolidated
            sheetname - Specifies the sheet name if there are multiple sheets within each Excel Workbook
        Returns:
            - A single dataframe from an imported excel file.

    :merge_and_clean_data:
        - Merges all of the dataframes in a list of dataframes and removes duplicates from the data
        Arguments:
            data_list - A list of dataframes
        Returns:
            - A single data frame with duplicates removed.

    :split_data_to_csv:
        - Splits the dataframe into equal parts and exports them into csv files in a given file path with a given file name.
        Arguments:
            df - A dataframe of data to be split into equal parts
            csvfilepath - Filepath to return the split csv files
            csvfilename - Filename to assign to the csv files
            n - Number of equal parts to split the dataframe into. 
        Returns:
            - n number of equally sized csv files

    :create_table:
        - SQL statement to create a table in snowflake with the given table name.
        Arguments:
            tablename - Desired table name
        Returns:
            - A complete SQL statement that will create a table in snowflake with the desired table name.
        
    :create_stage:
        - SQL statement that creates a temporary stage in snowflake with the given stage name.
        Arguments:
            stagename - desired stage name
        Returns:
            - A complete SQL statement that creates a temporary stage in snowflake with the given stage name.
        
    :clear_stage:
        - SQL statement that clears a temporary stage if an error occurred during an upload of files into a stage.
        Arguments:
            stagename - stage name to be cleared
        Returns:
            - A complete SQL statement that clears a given temporary stage.
        
    :load_files_to_stage:
        - SQL statement that uploads files from a local machine to a snowflake temporary stage
        Arguments:
            filepath - The filepath where the desired files are located to be uploaded to the stage
            filename - The filename that is being uploaded to the temporary snowflake stage
            stagename - The desired temorary snowflake stage that the files are to be uploaded to
        Returns:
            - A complete SQL statement that will upload the desired files to a given snowflake temporary stage
        
    :load_files_to_table:
        - Load files from a given temporary snowflake stage to a given table
        Arguments:
            tablename - The desired snowflake table that the files are to be uploaded to
            stagename - The desired temorary snowflake stage that the files are to be uploaded from
        Returns:
            - A complete SQL statement that will upload files from a given temporary snowflake stage to a given snowflake table.
        
    :sf_conection_from_df:
        - Establishes a snowflake connection with the user credentials, and executes SQL statements to upload data directly 
            from the dataframe to a snowflake table.
        Arguments:
            df - The dataframe that is to be loaded to the table.
            tablename - The desired snowflake table that the files are to be uploaded to
        Returns:
            - None

    :sf_connection_from_file:
        - Establishes a snowflake connection with the user credentials, and executes SQL statements to prepare the table,
            opens a temorary stage to upload the files from the local machine, and then uploads the files from the stage
            to the desired table.
        Arguments:
            tablename - The desired snowflake table that the files are to be uploaded to
            stagename - The desired temorary snowflake stage that the files are to be uploaded to and from
            filepath - The filepath where the desired files are located to be uploaded to the stage
            filename - The filename that is being uploaded to the temporary snowflake stage
        Returns:
            None
        
"""

import pandas as pd
import glob
import os
import time
import numpy as np
import snowflake.connector as sc

st = time.time()

# Changing names to SQL friendly & standardize column names for cleaning excel files later
names = {'New Side Part':'New_Side', 'Old Side Part': 'Old_Side', '(Old Side) Product Type New/Legacy/Both':'Old_New_Legacy_or_Other',
 '(New Side) Specific Service Part':'New_Specific_Service_Part', 'Firm Date':'Firm_Date', 'Phase':'Phase',
 '(New Side) Engineering Division':'Engineering_Division', 'Number':'Number', 'Type':'Type',
 '(Old Side) Last Build Date (MM/DD/YYYY)':'Old_Last_Build_Date'}

def get_excel_file_list(filepath):
    # Get a list of files in a given directory with a .xlsx file type
    # Ensures the filepath ends with a /
    if filepath[-1] != '/':
        filepath += '/'
    #Grabbing all .xlsx (Excel) files from Folder
    file_list = glob.glob(f"{filepath}*.xlsx")
    return file_list

def get_part_data(file, filepath, sheetname=None):
    # Ingest the excel file into a dataframe
    print('Loading file {0}...'.format(file))
    if sheetname == None:
        data = pd.read_csv(os.path.join(filepath, file))
    else:
        data = pd.read_csv(os.path.join(filepath, file), sheet_name=sheetname)
    # Standardize column names
    data.rename(columns=names, inplace=True)
    return data

def merge_and_clean_data(data_list):
    # Merge all DataFrame Objects to a Single Table
    df_master = pd.concat(data_list, axis=0, ignore_index=True)

    # Remove duplicates based on the 'Old Side Part' column and keep only the last value (newest)
    df_master.drop_duplicates(subset=['Old Side'], keep='last', inplace=True)
    return df_master


def split_data_to_csv(df, csvfilepath, csvfilename, n=4):
    # Alternately, upload to evenly sized .csv files for business users to reference until a Tableau dashboard has been developed.
    # Split the dataframe into n equal parts
    for id, df in enumerate(np.array_split(df, n)):
        # For each dataframe piece, output it to a csv file at the given file path with the given file name
        df.to_csv(f"{csvfilepath}/{csvfilename}_{id}.csv".format(id=id), index=None)

def create_table(tablename):
    #Create a new table, define the new columns datatype
    table_sql = f"""CREATE OR REPLACE TABLE {tablename} (New_Side varchar(20),
                                                          Old_Side varchar(20),
                                                          Old_New_Legacy_or_Other varchar(6)),
                                                          New_Specific_Service_Part varchar(3),
                                                          Firm_Date varchar(10),
                                                          Phase varchar(10),
                                                          Engineering_Division varchar(9),
                                                          Number (10),
                                                          Type varchar(10),
                                                          Old_Last_Build_Date varchar(10)
                                                          )
                    ;
                    """
    return table_sql

def create_stage(stagename):
    # Create a Stage to place files into. @~ would be "Your" Stage. Snowflake docs state that all users will have their own stage, COMPANY has not set it up this way.
    # You need to create your own stage.
    mkdir_stage = f"CREATE OR REPLACE STAGE {stagename} FILE_FORMAT = SANDBOX.CSV"
    return mkdir_stage

def clear_stage(stagename):
    #Use this to clear out the contents of the sandbox if you have to run the query again after any errors.
    rm_stage = f"REMOVE {stagename} PATTERN = '.*.*'"
    return rm_stage

def load_files_to_stage(filepath, filename, stagename):
    # Ensures the filepath ends with a /
    if filepath[-1] != '/':
        filepath += '/'
    #Grabbing all .csv files from a folder
    consolidated_list = glob.glob(f"{filepath}*.csv")
    # Load the csv files from the local file location into the stage
    for i in range(len(consolidated_list)):
        # SQL statement to upload file to a temporary snowflake stage
        put_to_sf_table= f"PUT file:{filepath}/{filename}_{i}.csv {stagename}"
        return put_to_sf_table

def load_files_to_table(tablename, stagename):
    # Copy the files from the stage into the new table, initialize the file format, set the delimiter for csv to ",", skip the header, conntinue on any errors.
    load_to_table = f'COPY INTO {tablename} FROM {stagename} file_format = (type = csv field_delimiter = "," skip_header = 1) on_error = continue'
    return load_to_table

def sf_connection_from_df(df, tablename):
    # Establish the connection with Snowflake, using browser authentication.
    # Loads sql query results at the cursor to a Pandas DataFrame.
    with sc.connect(
        user='user.user@company.com',
        account='company',
        authenticator="externalbrowser",
        role='ANALYST',
        warehouse='XSMALL_WH',
        database='DB',
        schema='SCHEMA'
        ) as conn, conn.cursor() as cur:
        # Create the table in snowflake.
        cur.execute(create_table())
        # Upload the dataframe to the table.
        df.to_sql(tablename, conn, index=False, if_exists='append')


def sf_connection_from_file(tablename, stagename, filepath, filename):
    # Establish the connection with Snowflake, using browser authentication.
    # Loads sql query results at the cursor to a Pandas DataFrame.
    with sc.connect(
        user='user.user@company.com',
        account='company',
        authenticator="externalbrowser",
        role='ANALYST',
        warehouse='XSMALL_WH',
        database='DB',
        schema='SCHEMA'
        ) as conn, conn.cursor() as cur:
        # Create the table in snowflake.
        cur.execute(create_table(tablename))
        # Create a temporary snowflake stage to upload the files to.
        cur.execute(create_stage(stagename))
        # Load the files from the local machine to the snowflake stage.
        cur.execute(load_files_to_stage(filepath, filename))
        # Load the files from the stage to the table.
        cur.execute(load_files_to_table(tablename))

if __name__ == '__main__':
    # Decide whether you would like to upload straight to snowflake or create backup csv files.
    decision = input("Would you like to upload the data straight to Snowflake, or create .csv files for a backup? (Answer - snowflake or csv): ")
    # Convert to all uppercase
    decision = decision.upper()
    # The file path to collect excel files from.
    file_path = "//company.com/file_directory/read_files"
    # The lis of files to consolidate.
    files_to_consolidate = get_excel_file_list(file_path)
    # Specify a sheet name if there are multiple sheets in the excel files.
    sheet_name = 'Part Sets'
    # Open a blank list to add to while reading from the individual documents
    df = []
    # Iterate over the file list.
    for file in files_to_consolidate:
        # Load each excel file to a dataframe
        part_data = get_part_data(file, file_path, sheetname=sheet_name)
        # append the dataframe to the a list for consolidation later.
        df.append(part_data)
    # Merge all dataframes in the list to a single dataframe and remove duplicates.
    master_dataframe = merge_and_clean_data()
    # Specify the table to be created
    table_name = 'DB.SCHEMA.SCHEMA.PART_CONSOLIDATION'
    # Specify the name of the temporary stage 
    stage_name = 'SANDBOX.User_Stage'
    # If the user specifies to upload directly to snowflake:
    if decision == 'SNOWFLAKE':
        # connect to snowflake and upload the dataframe to the table
        sf_connection_from_df(master_dataframe, tablename=table_name)
    else:
        # If the user specifies to create csv file backups:
        try:
            # Specify the file path to save the csv files
            csv_filepath = "//company.com/file_directory/consolidated_files"
            # Specify the file name for the csv files
            csv_filename = "Part_Sets_Consolidated"
            # Splits the data to equally sized csv files and exports to the given file path.
            split_data_to_csv(master_dataframe, csv_filepath, csv_filename, n=4)
            # Uploads the csv files to the given table
            sf_connection_from_file(table_name, stage_name, csv_filepath, csv_filename)
        except:
            # If there was an upload error, clear the temporary stage.
            clear_stage(stage_name)
    
    #List Number of Excel Sheets Loaded Successfully (Don't need to do this)
    print(str(len(df)) + f" files loaded of {len(files_to_consolidate)}")

et = time.time()

res = et-st
print('CPU execution time:', res, 'seconds')