"""
French Compliance Email Automation,  06/07/2023, Ryan Buzar

Classes:
:Vendor:
    Base class for the Vendor objects. Holds basic information about a vendor.
    Variables:
        :vendor_id: Vendor ID number. A string typically consisting of 5 integers
            followed by two letters. May also have a suffix of two additional integers.
        
        :data: a pandas dataframe consisting of the raw data pulled from a SQL query.
            The dataframe will include a vendor ID, vendor name, vendor contact names, 
            vendor contact email addresses, CS, primary  product director name 
            and email address. 

        :vendor_contacts: a dictionary keyed by ther person's name, with their email address as the value.

        :name: The vendor name

    Methods:
        :__init__: Initializes the variables, nothing special.

        :get_data: Queries the data specific to this vendor from the main dataframe pulled from get_all_data.

        :get_vendor_contacts: Queries the contacts specific to this vendor from the main dataframe pulled from
            get_all_data and compiles a dictionary keyed by the person's name with their email address as the value.

:French_Vendor:
    A child class of the Vendor class. Inherits all attributes and methods from the parent class.

    Variables:
        :french_status: The current french compliance status of this vendor in the Supplier Management System.

        :cs: The Strategic Business Unit that this vendor belongs to

        :cs_team: A dictionary of  contacts keyed by their name with their email addresses as their values.

    Methods:
        :__init__: Initializes the variables specific to this class as well as calls the constructor from the 
        parent class.

        :get_frenchstatus: Pulls the french compliance status from the dataframe pulled from the get_all_data
            function.

        :get_cs_team: Pulls the applicable  contacts from the  CS for this vendor from the dataframe 
            pulled from the get__data function and compiles a dictionary keyed by the person's name with their
            email address as the value.

        :create_email: Checks if the previous email was sent to the same recipients. If the recipients were 
            duplicated, the program moves to the next vendor.Creates a "To" and "Cc" list with correct formatting, 
            opens an outlook email object, inserts the To and Cc list, a subject, an HTML body, and attaches the 
             Supplier Guidelines PDF before sending the email.

Global Variables:
    last_to_list: A list of the last recipients an email was sent to. 

Functions:
    :get_all_data: Queries the applicable snowflake tables to retrieve vendor codes, vendor names, contacts,
        contact email addresses, the CS, and the product director name and email address. Places this info into a 
        pandas dataframe. Uses string formatting to prevent SQL injection.

    :get__data: Queries the applicable  snowflake tables to retrieve CS contacts and email addresses.

    :sf_connection: Opens the connection with snowflake, authenticates, executes the query, and then closes 
        the connection.

Program Function:
    Will pull all vendors from  Snowflake that have a french compliance status that is considered non-compliant
        along with all applicable contacts listed for the vendor and their information. Also pulls all applicable 
        CS contacts. Compiles this information for each vendor as it's own object, each having attributes such as: 
        vendor code, vendor name, french compliance status, vendor contacts, and  contacts. 
    
    The program then iterates over each vendor object and generates an email that will be sent to the vendor contacts, 
        CCd to the  CS team. The email will outline the notification to the vendor what action must be taken, as well 
        as attach a PDF copy of the  Packaging Standards, before sending the email.
    
    For optimization purposes, the program will also track the overall process time the program takes to complete.
"""
import pandas as pd
import snowflake.connector as sc
from datetime import date
import win32com.client
import time

# Mark the start of processing time for the program.
st = time.time()

# Used by create_email to tell if the current email list is a duplicate of the previous. 
global last_to_list
last_to_list = []


class Vendor(object):
    # Base class for the Vendor objects. Holds basic information about a vendor.
    def __init__(self, vendorid, vendorname):
        self.vendor_id = vendorid
        self.data = None
        self.vendor_contacts = {}
        self.name = vendorname
    
    def get_data(self):
        # pull a query from the french_master for all contacts in this vendor
        # and store in self.data
        self.data = french_master.query("VENDORID == @self.vendor_id")

    def get_vendor_contacts(self):
        # pull the vendor contacts from self.data and compile a dictionary 
        # keyed by the contact last name and first name, with the value as
        # their corresponding email address.
        self.vendor_contacts = {self.data.iloc[i]['VND_LASTNAME'] + ", " + 
                     self.data.iloc[i]['VND_FIRSTNAME']:
                     self.data.iloc[i]['VND_EMAIL']
                     for i in range(len(self.data))
                     }
        

class French_Vendor(Vendor):
    # A child class of the Vendor class. Inherits all attributes and methods 
    # from the parent class.
    def __init__(self, vendorid, vendorname):
        super().__init__(vendorid, vendorname)
        self.french_status = None
        self.cs = None
        self.cs_team = {}

    def get_frenchstatus(self):
        # pull a query from self.data and assign the frenchcomp status to 
        # this vendor.
        self.french_status = self.data.iloc[0]['FRENCHCOMP']

    def get_cs_team(self):
        # pull a query from the french_master for this vendor
        # and assign the frenchcomp status to this vendor.
        self.cs = self.data.iloc[0]['CS']
        prod_dir = {self.data.iloc[0]['CS_LASTNAME'] + ', ' + 
                    self.data.iloc[0]['CS_FIRSTNAME']: 
                    self.data.iloc[0]['CS_EMAIL']}
        
        # Add the product_director to the cs_team dictionary
        self.cs_team.update(prod_dir)

        # Map the product directors / CS number to their product managers and
        # product analysts.
        mapping = {
             'D10':('B10', 'M10'), 'D11':('B11', 'M11'),
             'D12':('B12', 'M12'), 'D14':('B14', 'M14'),
             'D15':('B15', 'M15'), 'D16':('B16', 'M16'),
             'D17':('B17', 'M17'), 'D18':('B18', 'M18'),
             'D20':('B20', 'M20'), 'D21':('B21', 'M21'),
             'D22':('B22',None), 'D99':(None, 'M99')
        }


        if self.cs in mapping:
            # the product manager is the "B", index 0 value from mapping
            prod_mgr_df = contacts.query("JCD == '{}'".format(mapping[self.cs][0]))
            # the product analyst is the "M", index 1 value from mapping
            prod_analyst_df = contacts.query("JCD == '{}'".format(mapping[self.cs][1]))
            # If there is not a product manager, skip to the next step.
            if mapping[self.cs][0] == None:
                pass
            else:
                # Assign the product manager name and email to a dictionary.
                prod_mgr = {prod_mgr_df.iloc[0]['LASTNAME'] + ', ' + 
                            prod_mgr_df.iloc[0]['FIRSTNAME']: 
                            prod_mgr_df.iloc[0]['EMAIL']} 
                # Add the product_manager to the cs_team dictionary
                self.cs_team.update(prod_mgr)
            # If there is no product analyst, skip to the next step.
            if mapping[self.cs][1] == None:
                 pass
            else:
                # Assign the product analyst name and email to a dictionary.
                prod_analyst = {prod_analyst_df.iloc[0]['LASTNAME'] + ', ' +
                                prod_analyst_df.iloc[0]['FIRSTNAME']:
                                prod_analyst_df.iloc[0]['EMAIL']}
                # Add the product_analyst to the cs_team dictionary
                self.cs_team.update(prod_analyst)
        else:
            # If the CS is not recognized, default to a blank dictionary 
            prod_mgr = {}
            prod_analyst = {}
    
    def create_email(self):
        # Creates a "To" and "Cc" list, then Creates an outlook email to the vendor.

        # Create a "to list" from the dataframe without duplicates      
        to_list = self.vendor_contacts.values()
        for i in to_list:
            global last_to_list
            # If any of the Vendor contacts were in the previous email, skip to the
            # next vendor
            if i in last_to_list:
                print("Vendor was contacted previously. Skipping.")
                continue
            else:
                # Update the previous contact list.
                last_to_list = [i for i in to_list]

                # Place a semi-colon on the end of every email address entry
                semi_colons_vnd = [x + '; ' for x in to_list]
                to_w_sc = ''.join(semi_colons_vnd)

                # Define a "CC" list
                cc_list = self.cs_team.values()
                # Place a semi-colon on the end of every email address entry
                semi_colons_cs = [x + '; ' for x in cc_list]
                cc_w_sc = ''.join(semi_colons_cs) 

                #Define the network directory of the  PAckaging Guidelines (as a raw string)
                dir_name = r'C:/Users/User.User/Downloads/Packaging Guidelines.pdf'

                # Create an email, fill out the "To", "CC", "Subject" and email body, and insert the attachemnt.
                outlook = win32com.client.Dispatch("Outlook.Application")
                MAIl = outlook.CreateItem(0)
                MAIl.To = str(to_w_sc)
                MAIl.CC = str(cc_w_sc)
                MAIl.Subject = f"""Retail Packaging Requirements for Compliance with the Charter 
                                   of French Language - {self.name} {self.vendor_id}"""
                MAIl.HTMLBody = f"""
                    Good afternoon,<br><br>
                    You were recently notified with the below details about updated Aftermarket Packaging 
                    Guidelines related to retail packaging requirements. <br><br> 
                    Your compliance with the Charter of French Language requires your immediate attention, 
                    as this is a legal requirement to do business in Quebec.  These requirements include 
                    translating both the label and instructions/pamphlets inside of the box into French. 
                    We are committed to complying with Quebec's legal requirements and expects 
                    all our supplier partners to do the same.<br><br>
                    The pertinent information for the retail packaging compliance requirements is located 
                    online. A PDF copy is attached<br><br>
 
                    Aftermarket Packaging Guidelines<br>
                    *As an additional reminder, the Packaging Guidelines state that both the 
                    label and instructions/pamphlets inside of the box need to include English, French 
                    and Spanish translations.<br><br>

                    Our distribution centers are infracting suppliers not in compliance with these 
                    requirements. Infractions are debited following the normal infraction process according 
                    to the published supplier guidelines.<br><br>

                    Please let me know if you have any questions.  Thank you for your continued support.
                    """
                # Select the non-primary email account in Outlook
                From = None
                for myEmailAddress in outlook.Session.Accounts:
                    if "@gmail.com" in str(myEmailAddress):
                        From = myEmailAddress
                        break

                if From != None:
                    MAIl._oleobj_.Invoke(*(64209,0,8,0,From))
                
                # Attach the PDF to the email
                print("Attaching PDF")
                MAIl.Attachments.Add(Source=dir_name)
                # Display the email before sending
                MAIl.Display() 

def get_all_data():
    # SQL used to query snowflake data for Vendor ID, Vendor Name, Vendor Contact Name,
    # Vendor Contact Role, Vendor Contact email, French Compliance Status, CS, 
    #  Contact Name,  Contact email. Uses python_snowflake_connector
    # to create a connection and authenticate. Uses String formatting to prevent 
    # SQL injection.
    vendor_details_sql = """WITH CTE AS (
        SELECT DISTINCT
            A.FIRSTNAME,
            A.LASTNAME,
            B.JCD,
            A.CNTID
        FROM 
            DB.SCHEMA.CNT AS A
        LEFT JOIN
            DB.SCHEMA.ROL AS B
        ON
            A.CNTID = B.CNTID
        WHERE
            JCD IN (
            'D10', 'D11', 'D12', 
            'D14', 'D15', 'D16',
            'D17', 'D18', 'D20',
            'D21', 'D22', 'D99'
            )
            AND A.BECODE ILIKE 'company'
            AND ID IS NOT NULL
            AND PDCLOC NOT ILIKE 'WEST VIRGINIA'
            AND ID NOT IN (
                'A', 'B', 'C',
                'D', 'E', 'F',
                'G', 'H', 'I'
                )
            AND LASTNAME NOT ILIKE 'John'
            AND LASTNAME NOT ILIKE 'ADAMS'
            AND LASTNAME NOT ILIKE 'ALEXANDER'
            AND LASTNAME NOT ILIKE 'HAMILTON'
            AND LASTNAME NOT ILIKE 'JEFFERSON'
            AND LASTNAME NOT ILIKE 'PAINE'
            AND LASTNAME NOT ILIKE 'SMITH'
            AND LASTNAME NOT ILIKE 'HANCOCK'
            AND LASTNAME NOT ILIKE 'FRANKLIN'
            AND LASTNAME NOT ILIKE 'FLOYD'
            AND LASTNAME NOT ILIKE 'HALL'
        ORDER BY
            RIGHT(JCD,2)
        )

        SELECT DISTINCT
            CONCAT(A.VNDID, A.VNDSFX) AS VENDORID,
            B.VNDNAM,
            A.FIRSTNAME AS VND_FIRSTNAME,
            A.LASTNAME AS VND_LASTNAME,
            D.ROLEDSC, 
            C.EMAIL AS VND_EMAIL,
            E.FRENCHCOMP,
            F.PROD_DIR AS CS,
            G.FIRSTNAME AS CS_FIRSTNAME,
            G.LASTNAME AS CS_LASTNAME,
            H.EMAIL AS CS_EMAIL
        FROM 
            DB.SCHEMA.CNT AS A
        LEFT JOIN 
            DB.SCHEMA.VNDR1 AS B
        ON
            CONCAT(A.VNDID, A.VNDSFX) = CONCAT(B.VNDID, B.VNDSFX)
        LEFT JOIN 
            DB.SCHEMA.EML AS C
        ON 
            A.CNTID = C.CNTID
        LEFT JOIN
            DB.SCHEMA.ROL AS D
        ON
            A.CNTID = D.CNTID
        LEFT JOIN 
            DB.SCHEMA.SPRF AS E
        ON 
            CONCAT(A.VNDID, A.VNDSFX) = CONCAT(E.VNDID, E.VNDSFX)
        LEFT JOIN
            DB.SCHEMA.VENDOR_MASTER AS F
        ON
            CONCAT(A.VNDID, A.VNDSFX) = CONCAT(F.VNDR_ID, F.VNDR_SFX)
        LEFT JOIN
            CTE AS G
        ON
            F.PROD_DIR = G.JCD
        LEFT JOIN
            DB.SCHEMA.EML AS H
        ON
            G.CNTID = H.CNTID
        WHERE 
            VENDORID IN (
                SELECT CONCAT(VNDID, VNDSFX) AS VENDORID
                FROM  DB.SCHEMA.SPRF
                WHERE 
                    FRENCHCOMP IS NOT NULL
                        )
            AND A.BETYPE ILIKE '%s' 
            AND A.CNTTYPE IN ('%s', '%s', '%s') 
            AND A.FIRSTNAME NOT ILIKE '%s' 
            AND A.FIRSTNAME NOT ILIKE '%s'
            AND A.LASTNAME NOT ILIKE '%s' 
            AND A.LASTNAME NOT ILIKE '%s'
            AND A.FIRSTNAME IS NOT NULL 
            AND A.LASTNAME IS NOT NULL
            AND B.STATIND ILIKE 'A'
            AND D.ROLEDSC IS NOT NULL 
            AND E.FRENCHCOMP IN ('%s', '%s', '%s')
            AND D.ROLEDSC IN ('%s', '%s', '%s')
        HAVING
            length(VENDORID) > 3
        ORDER BY 
            VENDORID
        ;""" % ('supplier', 'Primary', 'Secondary',
           'individual', '%VALID%', '%?%','%VALID%', 
           '%?%','Not Compliant', 'Infraction Logged', 
           'Pending Review', 'Supplier Infractions Contact *', 
           'Quality Manager', 'Account Manager *'
           )
    # Snowflake Connection
    vendor_details = sf_connection(vendor_details_sql)
    return vendor_details

def get_data():
    # SQL used to query snowflake data for  contacts such as Product Managers and Product Analysts.
    # Retrieves  Contact Name,  CS #, and  Contact emai. Uses python_snowflake_connector
    # to create a connection and authenticate. Uses String formatting to prevent SQL injection. 
    contacts_sql = """
        SELECT DISTINCT
            A.FIRSTNAME,
            A.LASTNAME,
            B.JCD,
            A.CNTID,
            C.EMAIL
        FROM 
            DB.SCHEMA.CNT AS A
        LEFT JOIN
            DB.SCHEMA.ROL AS B
        ON
            A.CNTID = B.CNTID
        LEFT JOIN
            DB.SCHEMA.EML AS C
        ON
            B.CNTID = C.CNTID
        WHERE
            JCD IN (
            'D10', 'B10', 'D11', 'B11', 'D12', 
            'B12', 'D14', 'B14', 'D15', 'B15',
            'D16', 'B16', 'D17', 'B17', 'D18',
            'B18', 'D20', 'B20', 'D21', 'B21',
            'D22', 'B22', 'D99', 'B99', 'M10',
            'M11', 'M12', 'M14', 'M15', 'M16',
            'M17', 'M18', 'M20', 'M21', 'M22',
            'M99'
            )
            AND A.BECODE ILIKE '%s'
            AND ID IS NOT NULL
            AND PDCLOC NOT ILIKE '%s'
            AND ID NOT IN ('%s', '%s', '%s',
            '%s', '%s', '%s', '%s', '%s', '%s')
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
            AND LASTNAME NOT ILIKE '%s'
        ORDER BY
            RIGHT(JCD,2)
            ;""" % ('company', 'WEST VIRGINIA', 'A', 'B',
                    'C', 'D', 'E', 'F',
                    'G', 'H', 'I', 'John', 
                    'ADAMS', 'ALEXANDER', 'HAMILTON', 
                    'JEFFERSON', 'PAINE', 'SMITH', 
                    'HANCOCK', 'FRANKLIN', 'FLOYD', 'HALL'
                    )
    # Snowflake Connection
    contacts = sf_connection(contacts_sql)
    return contacts

def sf_connection(sql, params=None):
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
                    cur.execute(sql, params)
                    pd_df = pd.read_sql(sql, conn, params=params)
    return pd_df

if __name__ == '__main__':
    print("importing the master vendor data file")
    french_master = get_all_data()
    print("Importing the company master data file")
    contacts = get_data()
    print("Creating vendor objects")
    vendor_dict = {french_master.iloc[i]['VENDORID']:french_master.iloc[i][ 'VNDNAM'] for i in range(len(french_master))}
    vendor_objs = [French_Vendor(k,v) for k,v in vendor_dict.items()]
    sample_objs = vendor_objs[0:1]

# When done with testing, remove sample_objs and replace with vendor_objs
    for h, i in enumerate(sample_objs):
        print(f"Vendor {h} of {len(sample_objs)} {(h/len(sample_objs)) * 100}% Complete")
        print(f"Importing vendor data for {i.vendor_id}")
        i.get_data()
        print(f"Getting vendor contacts for {i.vendor_id}")
        i.get_vendor_contacts()
        print(f"Getting French Compliance Status for {i.vendor_id}")
        i.get_frenchstatus()
        print(f"Getting  contacts for {i.vendor_id}")
        i.get_cs_team()
        print(f"Generating emai to vendor {i.vendor_id}")
        i.create_emai()
        print("Email sent successfully!")
        print("Moving to next vendor")

# Mark the end of processing time for the program.
et = time.time()

# Calculate overall process time for the program.
res = et-st

if res < 60:
    print(f'CPU execution time: {res:.4f} seconds')
else:
    print(f'CPU execution time: {res//60} minutes and {res%60:.4f} seconds')