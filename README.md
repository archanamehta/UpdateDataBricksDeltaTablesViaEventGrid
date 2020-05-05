# Update Azure Data Bricks Delta Tables Via Event Grid and Functions 

This solution enables a user to populate a Databricks Delta table by uploading a comma-separated values (csv) file that 
describes a sales order to ADLS Gen 2 Azure Storage. This solution is implemented by connecting together an Event Grid subscription, an Azure Function, and a Job in Azure Databricks.
 When a file is uploaded/updated within Azure Storage ie: ADLS Gen 2, this execute Event Grid Trigger which calls the Azure Functions (Function Name : orderprocessing) end point. The Function executed Azure Databricks Job which calls the Azure Databricks Notebook . The code within the Azure DataBricks Notebook updated the Delta Tables.    

In this tutorial, we will create the following Azure Services

1. Create an Azure Resource Group 
2. Create Azure Data Lake Gen 2 Storage   
2. Create a Service Principal 
3. Add all necessary Permissions to Azure Storage Account 
4. Create a Sample CSV file and upload to Azure Storage Account 
5. Create Azure Data Bricks Cluster and Python Notebook which has code connecting to ADLS Gen 2 and updating Delta Tables 
6. Create an Event Grid subscription that calls an Azure Function.
7. Create an Azure Function that receives a notification from an Event Grid, and executed Azure Databricks Job.
8. Create Azure Key Vault

### Prerequisites ## 
* Create Service Principal: Once the service principal App has been registered save the Application id , Tenant id and Secret Details. In our case Service Prinicipal with the name orderprocessing has been created . Following this link to get details how to create Service Principal https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal

* Create Azure Key Vault: Once Azure Key Vault has been created ; Select "Access policies" and then click "Add Access Policy" to create a new Pplicy. Select the "key, secret, and certificate permissions" you want to grant your application. Select the service principal (in our case Service prinipal name : dataprocessing) created previously. Then select Add to add the access policy and Save to commit your changes. Save the value of DNS Name . In our case DNS Name:https://archiekv.vault.azure.net/

 ![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/AzureKeyVaultAccessPolicy.png)
 Create Azure Key Vault Secret and save the Name of the Secret. In our case Secret name is : adlsgen2secret. 
 
 ![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateAzureKeyVaultSecret.png)

* Create an ADLS Gen 2 Account called "processorderstore". Within this storage account create a Container called "data" and Folder called "input".  
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateADLSGen2Account.png)
* Make sure the Azure Storage Account created has "Storage Blob Data Owner" role assigned to the Service Prinipal. Following should be the Access Controls for Storage Account 

 ![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/AzureStorageAccessControl.png)

### SAVE THE FOLLOWING CONFIGURATIONS ### 
--- Azure Storage Account--- 
Storage Account Name : processordersstore
Storage Key : <Storage Key> 
Storage Connection : <Storage Connection String> 
  
--- Create Application --- 
Application Name : dataprocessing 
Application Id : <Application Id > 
Tenant Id : <Tenant Id> 
Object Id : <Object Id > 
client-Secret : <client Secret> 
Value : <Client Secret Value > 
    
--- Azure Key Vault --- 
Key Vault : <URL> ie: https://archiekv.vault.azure.net/ 
Secret Name : adlsgen2secret
Secret Value : <Secret Value > 

--- Azure Data Bricks Scope --- 
Create ADB Scope URL : https://adb-410949980884417.17.azuredatabricks.net/?o=5135496090486482#secrets/createScope
ADB Scope : adlsen2adbscope
Resource Id : /subscriptions/<subscription id>/resourceGroups/DataProcessingRG/providers/Microsoft.KeyVault/vaults/archiekv

--- Granting the Service Principal permissions in ADLS Gen 2 --- 
az ad sp show --id <Application Id> --query objectId
Object Id : 791d4933-9de5-4ee8-a048-dbf69fba3a45
Go to Azure Storage Explorer and add the Above Object Id to ADLS Gen2 Folders via Manage Access 


### Create a ResourceGroup ie: DataProcessingRG ###
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateResourceGroup.png)

### Create Datafiles within ADLS Gen2 Storage Account ### 
Create a sales order
First, create a csv file that describes a sales order, and then upload that file to the storage account. Later, you'll use the data from this file to populate the first row in our Databricks Delta table.

Paste the following text into a text editor to create the CSV file ie: data.csv
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,12/1/2010 8:26,2.55,17850,United Kingdom

![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateStorageContainer.png)

### Create an Azure Databricks workspace ###
In this section, you create an Azure Databricks workspace using the Azure portal.
From the Azure portal, select Create a resource > Analytics > Azure Databricks.
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateDataBricksClusterv1.png)

### Create a Spark cluster in Databricks ###
In the Azure portal, go to the Azure Databricks workspace that you created, and then select Launch Workspace.You are redirected to the Azure Databricks portal. From the portal, select New > Cluster.
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateDataBricksClusterv2.png)
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateDataBricksClusterv3.png)

### Create Azure Databricks Secret Scope ###
Based on the Azure Databricks Workspace Name,  generate the following URL to Create Secret Scope. In our case the name of the Databricks Workspace is "adb-410949980884417.17.azuredatabricks.net";  
Hence the generate URL is:https://adb-410949980884417.17.azuredatabricks.net/?o=5135496090486482#secrets/createScope 

Go to the following URL ie: https://<databricks-instance>#secrets/createScope to create Secret Scope. This URL is case sensitive; scope in createScope must be uppercase. 
	Enter the DNS name of the Azure Key Vault created earlier
	Enter the Resource Id as follows eg: /subscriptions/<Subscription Id>/resourceGroups/<Resource Group Name>/providers/Microsoft.KeyVault/vaults/<Azure Key Vault name >. In our case this is the Resouurce id : /subscriptions/<Subscription Id> /resourceGroups/DataProcessingRG/providers/Microsoft.KeyVault/vaults/archiekv
	
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateADBSecretScope.png)


## Create and populate a Databricks Delta table. ##
### Create a Python Notebook ###
Copy and paste the following code block into the first cell, but don't run this code yet. This code creates a widget named source_file. Later, you'll create an Azure Function that calls this code and passes a file path to that widget. This code also authenticates your service principal with the storage account, and creates some variables that you'll use in other cells 

dbutils.widgets.text('source_file', "", "Source File")
spark.conf.set("fs.azure.account.auth.type.processordersstore.dfs.core.windows.net", "OAuth") 
spark.conf.set("fs.azure.account.oauth.provider.type.processordersstore.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.processordersstore.dfs.core.windows.net", "<Application Id> ") 
spark.conf.set("fs.azure.account.oauth2.client.secret.processordersstore.dfs.core.windows.net", dbutils.secrets.get(scope = "adlsen2adbscope", key = "<Azure Key Vault Secret>"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.processordersstore.dfs.core.windows.net", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
adlsPath = 'abfss://data@processordersstore.dfs.core.windows.net/'
inputPath = adlsPath + dbutils.widgets.get('source_file')
customerTablePath = adlsPath + 'delta-tables/customers'

#Create a Mount of the Azure Storage 
dbutils.fs.mount(
	  source = "abfss://data@processordersstore.dfs.core.windows.net/",
	  mount_point = "/mnt/adlsgen2storearchie100",extra_configs = configs)
inputPath = "/mnt/adlsgen2storearchie100/data.csv"
customerTablePath = "/mnt/adlsgen2storearchie100/delta-tables/customers"


#### This code creates the Databricks Delta table Within your storage account, and then loads some initial data from the csv file that you uploaded earlier.

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
#### After this above code block successfully runs, remove this code block from your notebook. ####



#### This code inserts data into a temporary table view by using data from a csv file. The path to that csv file comes from the input widget that you created in an earlier step. ####

upsertDataDF = (spark.read.option("header", "true").csv(inputPath))
upsertDataDF.createOrReplaceTempView("customer_data_to_upsert")


#### The following code to merge the contents of the temporary table view with the Databricks Delta table. ####

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
   
#### Select if rows from the file have been inserted 
   %sql select * from customer_data
    
## Create a job within Azure Databricks ## 
Create a Job that runs the notebook that you created earlier. Later, you'll create an Azure Function that runs this job when an event is raised.

Click Jobs.In the Jobs page, click Create Job. Give the job a name, and then choose the upsert-order-data workbook. In our case the Job Name: upsert-order-data and NoteBook Name: dataprocessing

![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateADBSecretScope.png)


# Create an Azure Function
 In the upper corner of the Databricks workspace, choose the people icon, and then choose User settings. Click the Generate new token button, and then click the Generate button. Make sure to copy the token to safe place. Your Azure Function needs this token to authenticate with Databricks so that it can run the Job.
 
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/GenerateADBToken.png)

Select the Create a resource button found on the upper left corner of the Azure portal, then select Compute > Function App. In the Create page of the Function App, make sure to select .NET Core for the runtime stack, and make sure to configure an Application Insights instance.

In the New Function pane, name the function UpsertOrder, and then click the Create button. Replace the contents of the code file with this code, and then click the Save button: 

using "Microsoft.Azure.EventGrid"
using "Newtonsoft.Json"
using Microsoft.Azure.EventGrid.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

private static HttpClient httpClient = new HttpClient();

public static async Task Run(EventGridEvent eventGridEvent, ILogger log)
{
    log.LogInformation("Event Subject: " + eventGridEvent.Subject);
    log.LogInformation("Event Topic: " + eventGridEvent.Topic);
    log.LogInformation("Event Type: " + eventGridEvent.EventType);
    log.LogInformation(eventGridEvent.Data.ToString());

    if (eventGridEvent.EventType == "Microsoft.Storage.BlobCreated" | | eventGridEvent.EventType == "Microsoft.Storage.FileRenamed") {
        var fileData = ((JObject)(eventGridEvent.Data)).ToObject<StorageBlobCreatedEventData>();
        if (fileData.Api == "FlushWithClose") {
            log.LogInformation("Triggering Databricks Job for file: " + fileData.Url);
            var fileUrl = new Uri(fileData.Url);
            var httpRequestMessage = new HttpRequestMessage {
                Method = HttpMethod.Post,
                RequestUri = new Uri(String.Format("https://{0}/api/2.0/jobs/run-now", System.Environment.GetEnvironmentVariable("DBX_INSTANCE", EnvironmentVariableTarget.Process))),
                Headers = {
                    { System.Net.HttpRequestHeader.Authorization.ToString(), "Bearer " +  System.Environment.GetEnvironmentVariable ("DBX_PAT", EnvironmentVariableTarget.Process)},
                    { System.Net.HttpRequestHeader.ContentType.ToString (), "application/json" }
                },
                Content = new StringContent(JsonConvert.SerializeObject(new {
                    job_id = System.Environment.GetEnvironmentVariable ("DBX_JOB_ID", EnvironmentVariableTarget.Process) ,
                    notebook_params = new {
                        source_file = String.Join("", fileUrl.Segments.Skip(2))
                    }
                }))
             };
            var response = await httpClient.SendAsync(httpRequestMessage);
            response.EnsureSuccessStatusCode();
        }
    }
}
This code parses information about the storage event that was raised, and then creates a request message with url of the file that triggered the event. As part of the message, the function passes a value to the source_file widget that you created earlier. the function code sends the message to the Databricks Job and uses the token that you obtained earlier as authentication.



In the Overview page of the Function App, click Configuration. In our case the Application setting is as follows 
DBX_INSTANCE: <Azure Databricks Name> ie:adb-410949980884417.17.azuredatabricks.net
DBX_PAT: <This is the Value we saved from Azure DataBricks User Setting above> 
DBX_JOB_ID: 1 
	
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateAzureFunctionsConfigurations.png)


# Create Event Grid Subscription 
In this section, you'll create an Event Grid subscription that calls the Azure Function when files are uploaded to the storage account.
![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateEventGridSubscription.png)

![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateEventGridSubscriptionV2.png)

![HDInsight Kafka Schema Registry](https://github.com/archanamehta/UpdateDataBricksDeltaTablesViaEventGrid/blob/master/Images/CreateEventGridSubscriptionV3.png)


## Test the Event Grid subscription

Create a file named customer-order.csv, paste the following information into that file, and save it to your local computer.

File Name : customer-order.csv 
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536371,99999,EverGlow Single,228,1/1/2018 9:01,33.85,20993,Sierra Leone

#### In Storage Explorer, upload this file to the input folder of your storage account.

Uploading a file raises the Microsoft.Storage.BlobCreated event. Event Grid notifies all subscribers to that event. In our case, the Azure Function is the only subscriber. The Azure Function parses the event parameters to determine which event occurred. It then passes the URL of the file to the Databricks Job. The Databricks Job reads the file, and adds a row to the Databricks Delta table that is located your storage account.

#### To check if the job succeeded, open your databricks workspace, click the Jobs button, and then open your job.

#### Select the job to open the job page.

In a new workbook cell, run this query in a cell to see the updated delta table.
%sql select * from customer_data







