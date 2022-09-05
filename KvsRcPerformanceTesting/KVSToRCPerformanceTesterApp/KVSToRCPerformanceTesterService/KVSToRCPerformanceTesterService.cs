using System.Fabric;
using System.Fabric.Description;
using System.Fabric.Query;
using System.Security.Cryptography.X509Certificates;
using System.Xml;
using Azure;
using Azure.Identity;
using Azure.Monitor.Query;
using Azure.Storage.Blobs;
using Microsoft.ServiceFabric.Actors.Migration;
using Microsoft.ServiceFabric.Data.Collections;
using Newtonsoft.Json;
using static KVSToRCPerformanceTesterService.Constants;

namespace KVSToRCPerformanceTesterService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class KVSToRCPerformanceTesterService : Microsoft.ServiceFabric.Services.Runtime.StatefulService
    {
        private IReliableDictionary<string, string> testingMetadataDictionary;

        public KVSToRCPerformanceTesterService(StatefulServiceContext context)
            : base(context)
        {}

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            this.testingMetadataDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, string>>("TestingMetadataDictionary");

            var credentials = GetCredentials(clientCertThumb, serverCertThumb, clusterCommonName);
            FabricClient fabricClient = new FabricClient(credentials, clusterConnectionUrl);
            Utils.Log("Created FabricClient");

            string dataPackagePath = this.ServiceContext.CodePackageActivationContext.GetDataPackageObject("Data").Path;
            // Constants.appPackagePath
            appPackagePath = Path.Join(dataPackagePath, "AppPackage");
            string testCasesJsonFilePath = Path.Join(dataPackagePath, "testcases.json");

            TestCase[] testcases;
            using (StreamReader r = new StreamReader(testCasesJsonFilePath))
            {
                testcases = JsonConvert.DeserializeObject<TestCase[]>(r.ReadToEnd());
            }

            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Utils.Log("Cancellation Requested");
                    break;
                }

                int currentTestCase;
                int currentPhase;
                int testRetries;
                using (var tx = this.StateManager.CreateTransaction())
                {
                    currentTestCase = int.Parse(await this.testingMetadataDictionary.GetOrAddAsync(tx, "CurrentTestCaseIndex", "0"));
                    currentPhase = int.Parse(await this.testingMetadataDictionary.GetOrAddAsync(tx, "CurrentPhase", "0"));
                    testRetries = int.Parse(await this.testingMetadataDictionary.GetOrAddAsync(tx, "CurrentTestCaseRetriesLeft", "2"));
                    await tx.CommitAsync();
                }

                if (currentTestCase == testcases.Length)
                {
                    // All test cases completed
                    break;
                }

                if (currentPhase == 0)
                {
                    Utils.Log("-----------------------------------------------------------------------------------------------------------------");
                    Utils.Log($"Starting TestCase: {testcases[currentTestCase].TestId}");
                }
                else
                {
                    Utils.Log($"Resuming TestCase: {testcases[currentTestCase].TestId} from phase: {currentPhase}");
                }

                await StartOrResumeMigrationForTestCase(fabricClient, testcases, currentTestCase, currentPhase, cancellationToken);
            }

            Utils.Log("-----------------------------------------------------------------------------------------------------------------");
            Utils.Log("-----------------------------------------------------------------------------------------------------------------");
            Utils.Log("Test Run Completed");
            Utils.Log("-----------------------------------------------------------------------------------------------------------------");
            Utils.Log("-----------------------------------------------------------------------------------------------------------------");
        }

        static X509Credentials GetCredentials(string clientCertThumb, string serverCertThumb, string name)
        {
            X509Credentials xc = new X509Credentials();
            xc.StoreLocation = StoreLocation.CurrentUser;
            xc.StoreName = "My";
            xc.FindType = X509FindType.FindByThumbprint;
            xc.FindValue = clientCertThumb;
            xc.RemoteCommonNames.Add(name);
            xc.RemoteCertThumbprints.Add(serverCertThumb);
            xc.ProtectionLevel = ProtectionLevel.EncryptAndSign;
            return xc;
        }

        private async Task StartOrResumeMigrationForTestCase(FabricClient fabricClient, TestCase[] testCases, int testCaseIndex, int phase, CancellationToken cancellationToken)
        {
            TestCase testCase = testCases[testCaseIndex];
            try
            {
                switch (phase)
                {
                    case 0:
                        // Prepare Test Case
                        PrepareTestCase(testCase);
                        
                        // Register Application Package
                        await RegisterApplicationPackage(fabricClient);
                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "1");
                            await tx.CommitAsync();
                        }

                        cancellationToken.ThrowIfCancellationRequested();
                        goto case 1;

                    case 1:
                        // Create KVS Actor Service
                        await CreateKVSActorService(fabricClient);

                        // Create Web Service
                        await CreateWebService(fabricClient);

                        // wait for some time for the services to come up
                        Utils.Log("Waiting for services to come up");
                        await WaitTillServiceIsup(fabricClient, kvsActorServiceUri, cancellationToken);
                        await WaitTillServiceIsup(fabricClient, webServiceUri, cancellationToken);

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "2");
                            await tx.CommitAsync();
                        }
                        
                        cancellationToken.ThrowIfCancellationRequested();
                        goto case 2;

                    case 2:
                        // Get the num actors
                        long returnedActors = await ActorProxyMethods.GetActorsCountAsync(fabricClient);

                        if (returnedActors == NumActors)
                        {
                            Utils.Log($"Num Actors : {returnedActors}");
                            Utils.Log("Actor data already restored. Continue.");
                        }
                        else
                        {
                            // populate Actor data
                            // restore KVS service from backup
                            Guid dataRestoreGuid = Guid.NewGuid();
                            await fabricClient.TestManager.StartPartitionDataLossAsync(dataRestoreGuid, PartitionSelector.PartitionKeyOf(new Uri(kvsActorServiceUri), 0), DataLossMode.FullDataLoss);

                            PartitionDataLossProgress progress = await fabricClient.TestManager.GetPartitionDataLossProgressAsync(dataRestoreGuid);
                            while (progress.State != TestCommandProgressState.Completed)
                            {
                                await Task.Delay(TimeSpan.FromSeconds(10));
                                progress = await fabricClient.TestManager.GetPartitionDataLossProgressAsync(dataRestoreGuid);

                                cancellationToken.ThrowIfCancellationRequested();
                            }

                            Utils.Log("Waiting for kvs actor state to restore");
                            await WaitTillServiceIsup(fabricClient, kvsActorServiceUri, cancellationToken);
                            Utils.Log("Kvs Actor state restored");

                            // Get the num actors
                            returnedActors = await ActorProxyMethods.GetActorsCountAsync(fabricClient);
                            Utils.Log($"Num Actors : {returnedActors}");
                        }

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "3");
                            await tx.CommitAsync();
                        }

                        cancellationToken.ThrowIfCancellationRequested();
                        goto case 3;

                    case 3:
                        // Create RC Actor Service
                        await CreateRCActorService(fabricClient);

                        // wait for some time for the services to come up
                        Utils.Log("Waiting for services to come up");
                        await WaitTillServiceIsup(fabricClient, rcActorServiceUri, cancellationToken);


                        // start actor state changer and poll RCActorService primary replica
                        CancellationTokenSource migrationTokenSource = new CancellationTokenSource();
                        CancellationToken migrationToken = migrationTokenSource.Token;
                        Task actorStateChangerTask = ActorStateChanger.Run(ActorStateChangeRate, migrationToken);
                        Task<bool> pollPrimaryNodeAsyncTask = PollPrimaryNodeAsync(fabricClient, rcActorServiceUri, migrationToken);

                        Utils.Log("Migration Started.");
                        Utils.Log("Waiting for Migration to complete");
 
                        MigrationResult migrationResult = await GetMigrationStatusOnCompletion(cancellationToken);

                        migrationTokenSource.Cancel();
                        await actorStateChangerTask;
                        bool didPrimaryMovedDuringMigration = !(await pollPrimaryNodeAsyncTask);

                        if (migrationResult == null)
                        {
                            throw new Exception("Failed to get the migration status");
                        }

                        if (migrationResult.Status == MigrationState.Aborted)
                        {
                            // wait before deleting application for logs to propogate.
                            await Task.Delay(TimeSpan.FromMinutes(1));

                            throw new Exception("Migration Aborted");
                        }
                        Utils.Log(migrationResult);

                        if (didPrimaryMovedDuringMigration)
                        {
                            throw new FabricException("RCActorService Primary replica moved during migration");
                        }

                        string migrationResultString = migrationResult.ToString();

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseMigrationResult", migrationResultString);
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "4");
                            await tx.CommitAsync();
                        }

                        cancellationToken.ThrowIfCancellationRequested();
                        goto case 4;

                    case 4:

                        // Azure monitor perf sampling takes some time
                        // I have set the sampling rate to 10 seconds.
                        await Task.Delay(TimeSpan.FromSeconds(30));

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            migrationResultString = (await this.testingMetadataDictionary.TryGetValueAsync(tx, "CurrentTestCaseMigrationResult")).Value;
                            migrationResult = JsonConvert.DeserializeObject<MigrationResult>(migrationResultString);
                        }

                        Utils.Log("Fetching perf counters for migration");
                        MigrationPerformanceResult migrationPerformanceResult = await GetMigrationPerformacneResults(migrationResult);
                        Utils.Log(migrationPerformanceResult);

                        string migrationPerformanceResultString = migrationPerformanceResult.ToString();

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseMigrationPerformanceResult", migrationPerformanceResultString);
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "5");
                            await tx.CommitAsync();
                        }

                        cancellationToken.ThrowIfCancellationRequested();
                        goto case 5;
                    case 5:
                        // Log the results
                        TestResult testResult = new TestResult();
                        testResult.TestId = testCase.TestId;
                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            testResult.MigrationResult = JsonConvert.DeserializeObject<MigrationResult>((await this.testingMetadataDictionary.TryGetValueAsync(tx, "CurrentTestCaseMigrationResult")).Value);
                            testResult.MigrationPerformanceResult = JsonConvert.DeserializeObject<MigrationPerformanceResult>((await this.testingMetadataDictionary.TryGetValueAsync(tx, "CurrentTestCaseMigrationPerformanceResult")).Value);
                            await tx.CommitAsync();
                        }

                        string testCaseResultFileName = $"Test_[{testCase.Index}]_{testCase.TestId}.json";
                        string testCaseResultFilePath = Path.Join(Path.GetTempPath(), testCaseResultFileName);
                        File.WriteAllText(testCaseResultFilePath, testResult.ToString());

                        // create blob client
                        BlobServiceClient blobServiceClient = new BlobServiceClient(BackupStorageAccountConnectionString);
                        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(TestResultsContainerName);
                        BlobClient blobClient = containerClient.GetBlobClient(testCaseResultFileName);

                        await blobClient.UploadAsync(testCaseResultFilePath, true);

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.AddOrUpdateAsync(tx, "CurrentPhase", "6", (_, __) => "6");
                            await tx.CommitAsync();
                        }

                        cancellationToken.ThrowIfCancellationRequested();
                        goto case 6;

                    case 6:
                        // Delete the application and remove application package
                        await UnRegisterApplicationPackage(fabricClient);

                        using (var tx = this.StateManager.CreateTransaction())
                        {
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseIndex", (testCaseIndex + 1).ToString());
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseRetriesLeft", "2");
                            await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "0");
                            await tx.CommitAsync();
                        }

                        Utils.Log($"Test {testCase.TestId} completed successfully");
                        Utils.Log("-----------------------------------------------------------------------------------------------------------------");

                        cancellationToken.ThrowIfCancellationRequested();
                        break;
                    default:
                        break;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                Utils.Log("Test Run Cancelled");
                throw;
            }
            catch (FabricException ex)
            {
                Utils.Log($"Test {testCase.TestId} failed due to exception {ex}");

                int retriesLeft = 0;
                using (var tx = this.StateManager.CreateTransaction())
                {
                    retriesLeft = int.Parse((await this.testingMetadataDictionary.TryGetValueAsync(tx, "CurrentTestCaseRetriesLeft")).Value);
                    await tx.CommitAsync();
                }

                if (retriesLeft > 0)
                {
                    // retry test
                    Utils.Log($"Retrying test: {testCase.TestId}");
                    using (var tx = this.StateManager.CreateTransaction())
                    {
                        await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseRetriesLeft", (retriesLeft - 1).ToString());
                        await tx.CommitAsync();
                    }
                }
                else
                {
                    // Delete the application and remove application package
                    await UnRegisterApplicationPackage(fabricClient);

                    Utils.Log($"Aboting Test: {testCase.TestId}");
                    using (var tx = this.StateManager.CreateTransaction())
                    {
                        await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseIndex", (testCaseIndex + 1).ToString());
                        await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "0");
                        await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseRetriesLeft", "2");
                        await tx.CommitAsync();
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (Exception ex)
            {
                Utils.Log($"Test {testCase.TestId} failed due to exception {ex}");
                Utils.Log("-----------------------------------------------------------------------------------------------------------------");

                // Delete the application and remove application package
                await UnRegisterApplicationPackage(fabricClient);

                using (var tx = this.StateManager.CreateTransaction())
                {
                    await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseIndex", (testCaseIndex + 1).ToString());
                    await this.testingMetadataDictionary.SetAsync(tx, "CurrentTestCaseRetriesLeft", "2");
                    await this.testingMetadataDictionary.SetAsync(tx, "CurrentPhase", "0");
                    await tx.CommitAsync();
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        private void PrepareTestCase(TestCase testCase)
        {
            // Make test case
            string rcSettingsXmlFilePath = Path.Join(appPackagePath, @"RCActorPkg\Config\Settings.xml");

            XmlDocument xmlDocument = new XmlDocument();
            xmlDocument.Load(rcSettingsXmlFilePath);

            string sfNamespace = "http://schemas.microsoft.com/2011/01/fabric";
            XmlNamespaceManager nsmgr = new XmlNamespaceManager(xmlDocument.NameTable);
            nsmgr.AddNamespace("sf", sfNamespace);

            var migrationSettingsSection = xmlDocument.SelectSingleNode("//sf:Section[@Name='RCActorServiceMigrationConfig']", nsmgr);
            List<XmlNode> childNodesToBeRemoved = new List<XmlNode>();
            for (int i = 0; i < migrationSettingsSection.ChildNodes.Count; i++)
            {
                var childNode = migrationSettingsSection.ChildNodes.Item(i);
                var chldNodeNameAttribute = childNode.Attributes.GetNamedItem("Name");
                if (chldNodeNameAttribute.Value != "SourceServiceUri" && chldNodeNameAttribute.Value != "TargetServiceUri" && chldNodeNameAttribute.Value != "ExceptionExclusionListForAbort")
                {
                    childNodesToBeRemoved.Add(childNode);
                }
            }

            foreach (var childNode in childNodesToBeRemoved)
            {
                migrationSettingsSection.RemoveChild(childNode);
            }

            XmlElement CopyPhaseParallelismParameter = xmlDocument.CreateElement("Parameter", sfNamespace);
            CopyPhaseParallelismParameter.SetAttribute("Name", "CopyPhaseParallelism");
            CopyPhaseParallelismParameter.SetAttribute("Value", testCase.CopyPhaseParallelism.ToString());

            XmlElement ItemsPerChunkParameter = xmlDocument.CreateElement("Parameter", sfNamespace);
            ItemsPerChunkParameter.SetAttribute("Name", "KeyValuePairsPerChunk");
            ItemsPerChunkParameter.SetAttribute("Value", testCase.KeyValuePairsPerChunk.ToString());

            XmlElement ItemsPerEnumerationParameter = xmlDocument.CreateElement("Parameter", sfNamespace);
            ItemsPerEnumerationParameter.SetAttribute("Name", "ChunksPerEnumeration");
            ItemsPerEnumerationParameter.SetAttribute("Value", testCase.ChunksPerEnumeration.ToString());

            XmlElement DowntimeThresholdParameter = xmlDocument.CreateElement("Parameter", sfNamespace);
            DowntimeThresholdParameter.SetAttribute("Name", "DowntimeThreshold");
            DowntimeThresholdParameter.SetAttribute("Value", testCase.DowntimeThreshold.ToString());

            migrationSettingsSection.AppendChild(CopyPhaseParallelismParameter);
            migrationSettingsSection.AppendChild(ItemsPerChunkParameter);
            migrationSettingsSection.AppendChild(ItemsPerEnumerationParameter);
            migrationSettingsSection.AppendChild(DowntimeThresholdParameter);

            xmlDocument.Save(rcSettingsXmlFilePath);

        }

        private async Task RegisterApplicationPackage(FabricClient fabricClient)
        {
            // Copy Application Package
            fabricClient.ApplicationManager.CopyApplicationPackage(imageStoreConnectionString, appPackagePath, appPackagePathInImageStore);
            Utils.Log("Copied application package to imagestore");

            try
            {
                // Register/Provision the Application Type
                await fabricClient.ApplicationManager.ProvisionApplicationAsync(appPackagePathInImageStore);
                Utils.Log("Provisioning application package");
            }
            catch (FabricElementAlreadyExistsException)
            {
                Utils.Log("application package already provisioned. Continue.");
            }

            try
            {
                // Create the Application
                ApplicationDescription appDesc = new ApplicationDescription(new Uri(applicationUri), appType, "1.0.0");
                await fabricClient.ApplicationManager.CreateApplicationAsync(appDesc);
                Utils.Log($"{applicationUri} application created");
            }
            catch (FabricElementAlreadyExistsException)
            {
                Utils.Log($"Application with name {applicationUri} already present. Continue.");
            }
        }

        private async Task CreateKVSActorService(FabricClient fabricClient)
        {
            ServiceList serviceList = await fabricClient.QueryManager.GetServiceListAsync(new Uri(applicationUri));

            if (serviceList.FirstOrDefault(s => s.ServiceName == new Uri(kvsActorServiceUri)) == null)
            {
                StatefulServiceDescription kvsActorServiceDesc = new StatefulServiceDescription();
                kvsActorServiceDesc.ApplicationName = new Uri(applicationUri);
                kvsActorServiceDesc.PartitionSchemeDescription = new UniformInt64RangePartitionSchemeDescription(1, -9223372036854775808, 9223372036854775807);
                kvsActorServiceDesc.ServiceName = new Uri(kvsActorServiceUri);
                kvsActorServiceDesc.ServiceTypeName = kvsActorServiceType;
                kvsActorServiceDesc.HasPersistedState = true;
                kvsActorServiceDesc.MinReplicaSetSize = 3;
                kvsActorServiceDesc.TargetReplicaSetSize = 3;

                await fabricClient.ServiceManager.CreateServiceAsync(kvsActorServiceDesc);
                Utils.Log($"{kvsActorServiceUri} service created");
            }
            else
            {
                Utils.Log($"{kvsActorServiceUri} service already created. Continue.");
            }
        }

        private async Task CreateWebService(FabricClient fabricClient)
        {
            ServiceList serviceList = await fabricClient.QueryManager.GetServiceListAsync(new Uri(applicationUri));

            if (serviceList.FirstOrDefault(s => s.ServiceName == new Uri(webServiceUri)) == null)
            {
                StatelessServiceDescription webServiceDesc = new StatelessServiceDescription();
                webServiceDesc.ApplicationName = new Uri(applicationUri);
                webServiceDesc.PartitionSchemeDescription = new SingletonPartitionSchemeDescription();
                webServiceDesc.InstanceCount = 1;
                webServiceDesc.ServiceName = new Uri(webServiceUri);
                webServiceDesc.ServiceTypeName = webServiceType;

                await fabricClient.ServiceManager.CreateServiceAsync(webServiceDesc);
                Utils.Log($"{webServiceUri} service created");
            }
            else
            {
                Utils.Log($"{webServiceUri} service already created. Continue.");
            }
        }

        private async Task CreateRCActorService(FabricClient fabricClient)
        {
            ServiceList serviceList = await fabricClient.QueryManager.GetServiceListAsync(new Uri(applicationUri));

            if (serviceList.FirstOrDefault(s => s.ServiceName == new Uri(rcActorServiceUri)) == null)
            {
                StatefulServiceDescription rcActorServiceDesc = new StatefulServiceDescription();
                rcActorServiceDesc.ApplicationName = new Uri(applicationUri);
                rcActorServiceDesc.PartitionSchemeDescription = new UniformInt64RangePartitionSchemeDescription(1, -9223372036854775808, 9223372036854775807);
                rcActorServiceDesc.ServiceName = new Uri(rcActorServiceUri);
                rcActorServiceDesc.ServiceTypeName = rcActorServiceType;
                rcActorServiceDesc.HasPersistedState = true;
                rcActorServiceDesc.MinReplicaSetSize = 3;
                rcActorServiceDesc.TargetReplicaSetSize = 3;

                await fabricClient.ServiceManager.CreateServiceAsync(rcActorServiceDesc);
                Utils.Log($"{rcActorServiceUri} service created");
            }
            else
            {
                Utils.Log($"{rcActorServiceUri} service already created. Continue.");
            }
        }

        private async Task WaitTillServiceIsup(FabricClient fabricClient, string serviceUri, CancellationToken cancellationToken)
        {
            var startTime = DateTime.UtcNow;
            while (true)
            {
                ServicePartitionList partitions = await fabricClient.QueryManager.GetPartitionListAsync(new Uri(serviceUri));
                bool allPartitionsReady = true;
                foreach (Partition partition in partitions)
                {
                    allPartitionsReady = allPartitionsReady && partition.PartitionStatus == ServicePartitionStatus.Ready;
                }
                if (allPartitionsReady)
                {
                    break;
                }
                await Task.Delay(TimeSpan.FromSeconds(30));
                cancellationToken.ThrowIfCancellationRequested();

                if (DateTime.UtcNow - startTime >= TimeSpan.FromMinutes(10))
                {
                    throw new Exception($"WaitTillServiceisUp Timed out for {serviceUri}");
                }
            }

            Utils.Log($"{serviceUri} is ready");
        }

        private async Task<MigrationResult> GetMigrationStatusOnCompletion(CancellationToken cancellationToken)
        {
            int numRetries = 2;
            MigrationResult result = null;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    result = await ActorProxyMethods.GetMigrationStatusAsync();
                }
                catch (TaskCanceledException)
                {
                }
                catch (JsonSerializationException)
                {
                }
                catch (Exception)
                {
                    if (--numRetries == 0)
                    {
                        break;
                    }
                }
                await Task.Delay(TimeSpan.FromSeconds(20));
            }
            while (result.CurrentPhase != MigrationPhase.Completed && result.Status != MigrationState.Aborted);

            return result;
        }



        private async Task<bool> PollPrimaryNodeAsync(FabricClient fabricClient, string serviceUri, CancellationToken cancellationToken)
        {
            string primaryNodeName = null;
            while (true)
            {
                ServicePartitionList partitions = await fabricClient.QueryManager.GetPartitionListAsync(new Uri(serviceUri));

                var replicaList = await fabricClient.QueryManager.GetReplicaListAsync(partitions[0].PartitionInformation.Id);
                foreach (var replica in replicaList)
                {
                    if (((StatefulServiceReplica)replica).ReplicaRole == ReplicaRole.Primary)
                    {
                        string currentNodeName = replica.NodeName;
                        if (primaryNodeName == null)
                        {
                            primaryNodeName = currentNodeName;
                        }
                        else if (primaryNodeName != currentNodeName)
                        {
                            return false;
                        }
                    }
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                await Task.Delay(TimeSpan.FromMinutes(1));
            }

            return true;
        }

        private bool IsBackupAvailable()
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(BackupStorageAccountConnectionString);
            BlobContainerClient blobContainerClient = blobServiceClient.GetBlobContainerClient(BackupContainerName);

            return blobContainerClient.Exists();
        }

        private async Task<MigrationPerformanceResult> GetMigrationPerformacneResults(MigrationResult migrationResult)
        {
            MigrationPerformanceResult performanceResult = new MigrationPerformanceResult();
            performanceResult.StartDateTimeUTC = migrationResult.StartDateTimeUTC;
            performanceResult.EndDateTimeUTC = migrationResult.EndDateTimeUTC;
            performanceResult.Duration = ((TimeSpan)(migrationResult.EndDateTimeUTC - migrationResult.StartDateTimeUTC)).TotalSeconds;

            performanceResult.TotalCpuUsageResults = new CpuUsageResults[2];
            performanceResult.TotalCpuUsageResults[0] = await GetCpuUsageResultsAsync("KVSActor", migrationResult.StartDateTimeUTC.ToString(), migrationResult.EndDateTimeUTC.ToString());
            performanceResult.TotalCpuUsageResults[1] = await GetCpuUsageResultsAsync("RCActor", migrationResult.StartDateTimeUTC.ToString(), migrationResult.EndDateTimeUTC.ToString());

            performanceResult.TotalRamUsageResults = new RamUsageResults[2];
            performanceResult.TotalRamUsageResults[0] = await GetRamUsageResultsAsync("KVSActor", migrationResult.StartDateTimeUTC.ToString(), migrationResult.EndDateTimeUTC.ToString());
            performanceResult.TotalRamUsageResults[1] = await GetRamUsageResultsAsync("RCActor", migrationResult.StartDateTimeUTC.ToString(), migrationResult.EndDateTimeUTC.ToString());

            performanceResult.PhaseResults = new PhasePerformanceResult[migrationResult.PhaseResults.Length];
            for (int i = 0; i < migrationResult.PhaseResults.Length; i++)
            {
                PhaseResult phaseResult = migrationResult.PhaseResults[i];
                PhasePerformanceResult phasePerformanceResult = new PhasePerformanceResult();
                performanceResult.PhaseResults[i] = phasePerformanceResult;

                phasePerformanceResult.StartDateTimeUTC = phaseResult.StartDateTimeUTC;
                phasePerformanceResult.EndDateTimeUTC = phaseResult.EndDateTimeUTC;
                phasePerformanceResult.Duration = ((TimeSpan)(phaseResult.EndDateTimeUTC - phaseResult.StartDateTimeUTC)).TotalSeconds;
                phasePerformanceResult.Phase = phaseResult.Phase;

                phasePerformanceResult.PerPhaseCpuUsageResults = new CpuUsageResults[2];
                phasePerformanceResult.PerPhaseCpuUsageResults[0] = await GetCpuUsageResultsAsync("KVSActor", phaseResult.StartDateTimeUTC.ToString(), phaseResult.EndDateTimeUTC.ToString());
                phasePerformanceResult.PerPhaseCpuUsageResults[1] = await GetCpuUsageResultsAsync("RCActor", phaseResult.StartDateTimeUTC.ToString(), phaseResult.EndDateTimeUTC.ToString());

                phasePerformanceResult.PerPhaseRamUsageResults = new RamUsageResults[2];
                phasePerformanceResult.PerPhaseRamUsageResults[0] = await GetRamUsageResultsAsync("KVSActor", phaseResult.StartDateTimeUTC.ToString(), phaseResult.EndDateTimeUTC.ToString());
                phasePerformanceResult.PerPhaseRamUsageResults[1] = await GetRamUsageResultsAsync("RCActor", phaseResult.StartDateTimeUTC.ToString(), phaseResult.EndDateTimeUTC.ToString());
            }

            return performanceResult;
        }

        private async Task<CpuUsageResults> GetCpuUsageResultsAsync(string ActorName, string startDateTime, string endDateTime)
        {
            // assumption: there was no failover during migration
            string Query =
                $@"Perf
                | where TimeGenerated >= datetime('{startDateTime}') and TimeGenerated <= datetime('{endDateTime}')
                | where ( ObjectName == 'Process' ) and CounterName == '% Processor Time'
                | where InstanceName == '{ActorName}'
                | summarize AvgCpuUsage = avg(CounterValue)/{NumProcessors}, PeakCpuUsage = max(CounterValue)/{NumProcessors}, MedianCpuUsage = percentile(CounterValue, 50)/{NumProcessors} by Computer, InstanceName 
                | sort by AvgCpuUsage
                | project AvgCpuUsage, PeakCpuUsage, MedianCpuUsage, Actor = InstanceName";

            var client = new LogsQueryClient(new DefaultAzureCredential());

            Response<IReadOnlyList<CpuUsageResults>> response = await client.QueryWorkspaceAsync<CpuUsageResults>(
                AzureLogAnalyticsWorkspaceId,
                Query,
                new QueryTimeRange(TimeSpan.FromDays(1000)),
                new LogsQueryOptions
                {
                    ServerTimeout = TimeSpan.FromMinutes(10)
                });

            // there may be no response if the duration is very less (lower than 5 seconds)
            if (response.Value.Count == 0)
            {
                CpuUsageResults zeroCpuUsageResult = new CpuUsageResults();
                zeroCpuUsageResult.Actor = ActorName;
                zeroCpuUsageResult.PeakCpuUsage = 0;
                zeroCpuUsageResult.AvgCpuUsage = 0;
                zeroCpuUsageResult.MedianCpuUsage = 0;

                return zeroCpuUsageResult;
            }

            // taking only primary replica values, ignoring secondary replica values
            return response.Value[0];
        }

        private async Task<RamUsageResults> GetRamUsageResultsAsync(string ActorName, string startDateTime, string endDateTime)
        {
            // assumption: there was no failover during migration
            string Query =
                $@"Perf
                | where TimeGenerated >= datetime('{startDateTime}') and TimeGenerated <= datetime('{endDateTime}')
                | where ( ObjectName == 'Process' ) and CounterName == 'Working Set'
                | where InstanceName == '{ActorName}'
                | summarize AvgRamUsage = avg(CounterValue)/1024/1024, PeakRamUsage = max(CounterValue)/1024/1024, MedianRamUsage = percentile(CounterValue, 50)/1024/1024 by Computer, InstanceName
                | sort by AvgRamUsage
                | project AvgRamUsage, PeakRamUsage, MedianRamUsage, Actor = InstanceName";

            var client = new LogsQueryClient(new DefaultAzureCredential());

            Response<IReadOnlyList<RamUsageResults>> response = await client.QueryWorkspaceAsync<RamUsageResults>(
                AzureLogAnalyticsWorkspaceId,
                Query,
                new QueryTimeRange(TimeSpan.FromDays(1000)),
                new LogsQueryOptions
                {
                    ServerTimeout = TimeSpan.FromMinutes(10)
                });

            // there may be no response if the duration is very less (lower than 5 seconds)
            if (response.Value.Count == 0)
            {
                RamUsageResults zeroRamUsageResult = new RamUsageResults();
                zeroRamUsageResult.Actor = ActorName;
                zeroRamUsageResult.PeakRamUsage = 0;
                zeroRamUsageResult.AvgRamUsage = 0;
                zeroRamUsageResult.MedianRamUsage = 0;
                return zeroRamUsageResult;
            }

            // taking only primary replica values, ignoring secondary replica values
            return response.Value[0];
        }

        private static async Task UnRegisterApplicationPackage(FabricClient fabricClient)
        {
            try
            {
                DeleteApplicationDescription deleteApplicationDescription = new DeleteApplicationDescription(new Uri(applicationUri));
                deleteApplicationDescription.ForceDelete = true;
                await fabricClient.ApplicationManager.DeleteApplicationAsync(deleteApplicationDescription);
            }
            catch (FabricElementNotFoundException)
            {
            }
            Utils.Log($"Application {applicationUri} deleted");

            try
            {
                await fabricClient.ApplicationManager.UnprovisionApplicationAsync(new UnprovisionApplicationTypeDescription(appType, "1.0.0"));
            }
            catch (FabricElementNotFoundException)
            {
            }
            Utils.Log($"App type: {appType} unprovisioned");

            fabricClient.ApplicationManager.RemoveApplicationPackage(imageStoreConnectionString, appPackagePathInImageStore);
        }

    }
}
