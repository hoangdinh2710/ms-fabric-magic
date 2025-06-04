from sharepoint_fabric import SharePoint

# Define Credential
sharepoint_site_name='SHAREPOINT_SITE_NAME',
sharepoint_site=f'https://company.sharepoint.com/sites/{SHAREPOINT_SITE_NAME}/',
sharepoint_doc='Shared Documents', # Shared Documents is the default option, please change if needed
username='Email', # Your email
password='Password' # Your password, # This method is not very secure because you have to specify the password in your code, recommend to use keyvault instead. OR use service principle

# Define parameters
workspace_id = "workspace_id"
lakehouse_id = "lakehouse_id"
src_folder = f"/folder_path" # Example: if your file is in Share Documents/DATA/2025 then add "/DATA/2025""
src_file_name = f"EXAMPLE.xlsx"

# Optional for specific ABFSS path
#dest_folder = f"abfss://{'workspace_id'}@onelake.dfs.fabric.microsoft.com/{'lakehouse_id'}/Files/folder_path"

# Default path
dest_folder = f"Files/folder_path" # Define folder path in Lakehouse here
lh_file_path = f"{dest_folder}/{src_file_name}" # Use the same file name as source, but this could be changed

# Init a SharePoint object
sp = SharePoint(
    SHAREPOINT_SITE= sharepoint_site,
    SHAREPOINT_SITE_NAME= sharepoint_site_name,
    SHAREPOINT_DOC= sharepoint_doc,
    USERNAME= username,
    PASSWORD= password
    )
# Example get file from src folder in SharePoint and save to dest folder in Lakehouse
sp.get_file(file_n=src_file_name, src_folder=src_folder,dest_folder=dest_folder)
# Example get files from src folder in SharePoint and save to dest folder in Lakehouse
sp.get_files(src_folder=src_folder,dest_folder=dest_folder)