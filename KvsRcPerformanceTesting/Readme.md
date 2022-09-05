## PerformanceTestingApp
- Sample app that which contains KVS and RC Actor services that undergo migration.
- There is also a WebService that is used to perform different functions on the Actor Services. The WebServices listens on port 9033 and has multiple endpoints as defined in MigrationController.cs.
- The 2 Actor Services and the WebService are all placed on different Node types (as defined in the respective ServiceManifest.xml files).
- For migration, the KVS states are populated by -
  - Making a POST request to /api/migration/generateActors/{numActors}/{numStatesPerActors} on the WebService. 100K actors with 100 states make 1GB of KVS state. To get a larger state, the number of actors andthe number of states per actors can be increased accordingly.
  - Having actor states backed up on a storage account and invoke data loss on kvs actor service partition which will try to restore the actor states from the storage account. (check KVSActorService.OnDataLossAsync method)


## KVSToRCPerformanceTesterApp
- This app is responsibe for -
  - Setting the right migration config (for the particular testcase) in RC Actor Settings.xml
  - Deploying the PerformanceTestingApp.
  - Restoring actor states in KVS Actor Service.
  - Starting and monitoring migration
  - Fetching the performance results for the migration from log analytics workspace.
  - Pushing the test results to the storage account.


## How to Run a KVS to RC Migration Performace Test
- Setup the following resources in the Azure cluster
  - Service Fabric Cluster with 3 node types. Primary Node type should have all the default SF services and the WebService running. the KVS and RC actor services should be on the other 2 node types.
  Also make sure that the KVS Node types should at least 4 x 8 x size(KVSBackup.zip/actor state size) space in the SF managed disk as restoring actor states takes a lot of space.
  - Storage account that have 2 public access containers (backupcontainer - to store the backed up actor states and testresultscontainer - where the testing service will push the test results)
  - Log analytics workspace - that will collect performance counters for the KVS and RC actor services. 
    - Create a log analytics workspace. 
    - Open the Legacy agents management tab in the Log analytics workspace page. 
    - In windows performance counters, add "Process(*)\Working Set" and "Processor(*)\% Processor Time" counters.
    - In KVS/RC node types VMSS page, go to the Insights tab.
    - Click on enable and choose the created log analytics workspace. This operation generally fails on the first time. Trying it again would succeed.
  - Application insights - This will be used to see performance testing logs from the KVSToRCPerformanceTesterApp. All the logs are pushed in the "Traces" table.
- Set all the required values in Constants.cs.
- Build the PerformanceTestingApp and place the complete app package in KVSToRCPerformanceTesterService/PackageRoot/Data/AppPackage.
- Place the testcases.json file in KVSToRCPerformanceTesterService/PackageRoot/Data
- Set the RunAsPolicy in the ApplicationManifest.xml to the Local User Account that was used to create/setup the log analytics workspace. Currenty, log analytics workspace requires AAD authentication which requires the log analytics workspace to be registered with an AAD app. The above RunAsPolicy helps to avoid that.
check [Azure Monitor Query client libraries](https://devblogs.microsoft.com/azure-sdk/announcing-the-new-azure-monitor-query-client-libraries/), [DefaultAzureCredential ](https://docs.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet).
- Deploy the KVSToRCPerformanceTesterApp on local cluster and the test run will start automatically.