# Update Azure Data Bricks Delta Tables Via Event Grid and Functions 


### Updating ADB Delta Tables ###
A Small solution that enables a user to populate a Databricks Delta table by uploading a comma-separated values (csv) file that 
describes a sales order. You'll build this solution by connecting together an Event Grid subscription, an Azure Function, 
and a Job in Azure Databricks.

In this tutorial, we will create the following Azure Services

1. Create an Azure ResourceGroup 
2. Create Azure Data Lake Gen 2 Storage   
2. Create a Service Principal 
3. Add all Permissions to Storage Account 
4. Create a Sample CSV file and upload to Storage Account 
5. Create Azure Data Bricks Cluster and Python Notebook indicating code connecting to ADLS Gen 2 
6. Create an Event Grid subscription that calls an Azure Function.
7. Create an Azure Function that receives a notification from an event, and then runs the job in Azure Databricks.
8. Create a Databricks job that inserts a customer order into a Databricks Delta table that is located in the storage account.
9. Create Azure Key Vault


### Create a ResourceGroup ie: DataProcessingRG ###
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateResourceGroup.png)
### Create an ADLS Gen2 Storage Account ### 
Create an ADLS Gen 2 Account called "processorderstore". Within this storage account create a Container called "data" and Folder called "input".  
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateADLSGen2Account.png)
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateStorageContainer.png)
### Create an Azure Databricks workspace ###
In this section, you create an Azure Databricks workspace using the Azure portal.

From the Azure portal, select Create a resource > Analytics > Azure Databricks.



### Create a Spark cluster in Databricks ###
In the Azure portal, go to the Azure Databricks workspace that you created, and then select Launch Workspace.

You are redirected to the Azure Databricks portal. From the portal, select New > Cluster.


### Create and populate a Databricks Delta table. ### 
In the Python notebook that you created, copy and paste the following code block into the first cell, but don't run this code yet.
This code creates a widget named source_file. Later, you'll create an Azure Function that calls this code and passes a file path to that widget. This code also authenticates your service principal with the storage account, and creates some variables that you'll use in other cells 


dbutils.widgets.text('source_file', "", "Source File")
spark.conf.set("fs.azure.account.auth.type.processordersstore.dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type.processordersstore.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.processordersstore.dfs.core.windows.net", "49c851f7-c7f9-49eb-9c4a-8bf6636a0311") 
spark.conf.set("fs.azure.account.oauth2.client.secret.processordersstore.dfs.core.windows.net", dbutils.secrets.get(scope = "adlsen2adbscope", key = "adlsgen2secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.processordersstore.dfs.core.windows.net", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
adlsPath = 'abfss://data@processordersstore.dfs.core.windows.net/'
inputPath = adlsPath + dbutils.widgets.get('source_file')
customerTablePath = adlsPath + 'delta-tables/customers'

Create a Mount of the Azure Storage 
dbutils.fs.mount(
	  source = "abfss://data@processordersstore.dfs.core.windows.net/",
	  mount_point = "/mnt/adlsgen2storearchie100",extra_configs = configs)
inputPath = "/mnt/adlsgen2storearchie100/data.csv"
customerTablePath = "/mnt/adlsgen2storearchie100/delta-tables/customers"


This code creates the Databricks Delta table in your storage account, and then loads some initial data from the csv file that you uploaded earlier.

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
inputSchema = StructType([
StructField("InvoiceNo", IntegerType(), True),
StructField("StockCode", StringType(), True),
StructField("Description", StringType(), True),
StructField("Quantity", IntegerType(), True),
StructField("InvoiceDate", StringType(), True),
StructField("UnitPrice", DoubleType(), True),
StructField("CustomerID", IntegerType(), True),
StructField("Country", StringType(), True)
])
rawDataDF = (spark.read.option("header", "true").schema(inputSchema).csv(adlsPath + 'input'))
(rawDataDF.write.mode("overwrite").format("delta").saveAsTable("customer_data", path=customerTablePath))


After this above code block successfully runs, remove this code block from your notebook.


This code inserts data into a temporary table view by using data from a csv file. The path to that csv file comes from the input widget that you created in an earlier step.

upsertDataDF = (spark.read.option("header", "true").csv(inputPath))
upsertDataDF.createOrReplaceTempView("customer_data_to_upsert")


The following code to merge the contents of the temporary table view with the Databricks Delta table.
%sql
MERGE INTO customer_data cd
USING customer_data_to_upsert cu
ON cd.CustomerID = cu.CustomerID
WHEN MATCHED THEN
  UPDATE SET
    cd.StockCode = cu.StockCode,
    cd.Description = cu.Description,
    cd.InvoiceNo = cu.InvoiceNo,
    cd.Quantity = cu.Quantity,
    cd.InvoiceDate = cu.InvoiceDate,
    cd.UnitPrice = cu.UnitPrice,
    cd.Country = cu.Country
WHEN NOT MATCHED
  THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
  VALUES (
    cu.InvoiceNo,
    cu.StockCode,
    cu.Description,
    cu.Quantity,
    cu.InvoiceDate,
    cu.UnitPrice,
    cu.CustomerID,
    cu.Country)
   
Select if rows from the file have been inserted 
   %sql select * from customer_data
    
### Create a job in Azure Databricks ### 
In this section, you'll perform these tasks:


# Create an Azure Function
Create an Azure Function that runs the Job.

In the upper corner of the Databricks workspace, choose the people icon, and then choose User settings.

In the Overview page of the Function App, click Configuration.




### SAVE THE FOLLOWING CONFIGURATIONS ### 

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

--Azure Data Bricks Scope 
Create ADB Scope URL : https://adb-410949980884417.17.azuredatabricks.net/?o=5135496090486482#secrets/createScope
ADB Scope : adlsen2adbscope
Resource Id : /subscriptions/<subscription id>/resourceGroups/DataProcessingRG/providers/Microsoft.KeyVault/vaults/archiekv

-- Granting the Service Principal permissions in ADLS Gen 2 --- 
az ad sp show --id <Application Id> --query objectId
Object Id : 791d4933-9de5-4ee8-a048-dbf69fba3a45
Go to Azure Storage Explorer and add the Above Object Id to ADLS Gen2 Folders via Manage Access 


-- Grant Roles to ADSL Gen 2




# Create a Job within Azure Data Bricks # 
Create a Job that runs the notebook that you created earlier. Later, you'll create an Azure Function that runs this job when an event is raised.

Click Jobs.

In the Jobs page, click Create Job.

Give the job a name, and then choose the upsert-order-data workbook.












