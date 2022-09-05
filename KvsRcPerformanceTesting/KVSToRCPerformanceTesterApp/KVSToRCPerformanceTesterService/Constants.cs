using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVSToRCPerformanceTesterService
{
    public static class Constants
    {
        public static string clusterEndpoint = "CLUSTER_ENDPOINT";  // Ex - kvsrcmigrationcluster.westus.cloudapp.azure.com
        public static string clientCertThumb = "CLIENT_CERTIFICATE_THUMBPRINT";
        public static string serverCertThumb = "SERVER_CERTIFICATE_THUMBPRINT";
        public static string clusterCommonName = $"www.{clusterEndpoint}";
        public static string clusterConnectionUrl = $"{clusterEndpoint}:19000";

        // kvs actor state backup constants
        public static string BackupStorageAccountConnectionString = "AZURE_STORAGE_ACCOUNT_CONNECTION_STRING";
        public static string BackupContainerName = "backupcontainer";
        public static string KVSBackupZipFileName = "KVSActorBackup.zip";

        // test result container name
        public static string TestResultsContainerName = "testresultscontainer";

        // Test logs application insights key
        public static string ApplicationInsightsInstrumentationKey = "AZURE_APP_INSIGHTS_INSTRUMENTATION_KEY";

        // Number of Processors of VMs on which RC Actor Service is running
        public static int NumProcessors = 8;

        // Number of actors present in the KVS Service
        public static int NumActors = 100000;

        public static string appPackagePath = ""; // This is set to the app package present in the Service Data folder
        public static string imageStoreConnectionString = "fabric:ImageStore";
        public static string appPackagePathInImageStore = "PerformanceTestingAppType";
        public static string appType = "PerformanceTestingAppType";
        public static string applicationUri = "fabric:/PerformanceTestingApp";
        public static string kvsActorServiceName = "KVSActorService";
        public static string kvsActorServiceType = "KVSActorServiceType";
        public static string kvsActorServiceUri = applicationUri + "/" + kvsActorServiceName;
        public static string rcActorServiceName = "RCActorService";
        public static string rcActorServiceType = "RCActorServiceType";
        public static string rcActorServiceUri = applicationUri + "/" + rcActorServiceName;
        public static string webServiceName = "WebService";
        public static string webServiceType = "WebServiceType";
        public static string webServiceUri = applicationUri + "/" + webServiceName;
        public static int webServicePort = 9033;
        
        public static string webServiceMigrationApiEndpoint = $"http://{clusterEndpoint}:{webServicePort}/api/migration";
        public static int ActorStateChangeRate = 20; // number of actor state changes per second

        // log analytics workspace contants;
        public static string AzureLogAnalyticsWorkspaceId = "AZURE_LOG_ANALYTICS_WORKSPACE_ID";
    }
}
