# SQL-Python-Vendor-Email-Automation
Email Automation from Vendor data stored in a Snowflake database.

**Legal Stuff**
This is a copyright notice that intends to apply to the code retained within the SQL-Python-Vendor-Email-Automation repository "The Repository" that was originally posted by GitHub
user RyanBuzar "The Developer".

The developer has not included a license because they do not intend the code to be used publically. The code is part of an exhibition to potential employers. If you wish to use,
modify, "fork", copy, reproduce, or contribute to the code contained in the repository you must contact the developer whom will maintain and retain all copyright permission from any
contributors. The developer doesnot grant reciprocal copyright permission to any contributors unless otherwise explicitly agreed upon by the developer. The developer reserves the
right to seek the advice of legal counsel before entering into any such agreements.

**Program Functionality:**
Will pull all vendors from Snowflake that have a french compliance status that is considered non-compliant
along with all applicable contacts listed for the vendor and their information. Also pulls all applicable 
CS contacts. Compiles this information for each vendor as it's own object, each having attributes such as: 
vendor code, vendor name, french compliance status, vendor contacts, and contacts. 
    
The program then iterates over each vendor object and generates an email that will be sent to the vendor contacts, 
CCd to the  CS team. The email will outline the notification to the vendor what action must be taken, as well 
as attach a PDF copy of the  Packaging Standards, before sending the email.
    
For optimization purposes, the program will also track the overall process time the program takes to complete.

**Suggestions?**
If you have any suggestions that may improve the performance, readability, security of the code. Please do not hesitate to reach out with suggestions.
