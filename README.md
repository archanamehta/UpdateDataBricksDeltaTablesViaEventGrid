# Update Azure Data Bricks Delta Tables Via Event Grid and Functions 


### Updating ADB Delta Tables ###

A Small solution that enables a user to populate a Databricks Delta table by uploading a comma-separated values (csv) file that 
describes a sales order. You'll build this solution by connecting together an Event Grid subscription, an Azure Function, 
and a Job in Azure Databricks.

In this tutorial, we will create the following 

1. Create Azure Data Lake Gen 2 Storage   
2. Create a Service Principal 
3. Add all Permissions to Storage Account 
4. Create a Sample CSV file and upload to Storage Account 
5. Create Azure Data Bricks Cluster and Python Notebook indicating code connecting to ADLS Gen 2 
6. Create an Event Grid subscription that calls an Azure Function.
7. Create an Azure Function that receives a notification from an event, and then runs the job in Azure Databricks.
8. Create a Databricks job that inserts a customer order into a Databricks Delta table that is located in the storage account.



Create a ResourceGroup call DataProcessingRG 



Create an ADLS Gen2 Storage Account called "processorderstore". Within this storage account create 
Container : data
Folder : input 





Create a job in Azure Databricks
In this section, you'll perform these tasks:

Create an Azure Databricks workspace.
Create a notebook.
Create and populate a Databricks Delta table.
Add code that inserts rows into the Databricks Delta table.
Create a Job.
Create an Azure Databricks workspace
In this section, you create an Azure Databricks workspace using the Azure portal.

In the Azure portal, select Create a resource > Analytics > Azure Databricks.


Create an Azure Databricks workspace
In this section, you create an Azure Databricks workspace using the Azure portal.

In the Azure portal, select Create a resource > Analytics > Azure Databricks.


Create a Spark cluster in Databricks
In the Azure portal, go to the Azure Databricks workspace that you created, and then select Launch Workspace.

You are redirected to the Azure Databricks portal. From the portal, select New > Cluster.




SAVE THE FOLLOWING CONFIGURATIONS 

---Azure Storage Account 
Storage Account Name : processordersstore
Storage Key : <Storage Key> 
Storage Connection : <Storage Connection String> 
  
--Create Application
Application Name : dataprocessing 
Application Id : <Application Id > 
Tenant Id : <Tenant Id> 
Object Id : <Object Id > 
client-Secret : <client Secret> 
Value : <Client Secret Value > 
  
  
--Azure Key Vault 
Key Vault : <URL> ie: https://archiekv.vault.azure.net/ 
Secret Name : adlsgen2secret
Secret Value : <Secret Value > 



Create and populate a Databricks Delta table
In the notebook that you created, copy and paste the following code block into the first cell, but don't run this code yet.







