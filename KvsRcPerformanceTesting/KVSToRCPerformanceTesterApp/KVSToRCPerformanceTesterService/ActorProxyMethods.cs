using KVSActor.Interfaces;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Client;
using Microsoft.ServiceFabric.Actors.Migration;
using Microsoft.ServiceFabric.Actors.Query;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Migration.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static KVSToRCPerformanceTesterService.Constants;

namespace KVSToRCPerformanceTesterService
{
    internal static class ActorProxyMethods
    {
        static string PerformanceTestingApplicationName = "fabric:/PerformanceTestingApp";
        static string KVSActorServiceName = "KVSActorService";
        static string RCActorServiceName = "RCActorService";

        public static async Task GenerateActorsAsync(long numActors, int numStatesPerActor)
        {
            //string serviceUri = PerformanceTestingApplicationName + "/" + KVSActorServiceName;
            //Random generator = new Random();

            //int numThreads = 128;
            //List<Task> taskList = new List<Task>();

            //Func<long, Task> actorCreationFunc = async (num) =>
            //{
            //    for (int i = 0; i < num; i++)
            //    {
            //        ActorId actorId = ActorId.CreateRandom();
            //        IKVSActor proxy = ActorProxy.Create<IKVSActor>(actorId, new Uri(serviceUri)/*, this.ListnerName*/);
            //        await proxy.CreateRandomStatesAsync(numStatesPerActor);
            //    };
            //};

            //for (int i = 0; i < numThreads; i++)
            //{
            //    taskList.Add(Task.Run(async () => await actorCreationFunc(numActors / numThreads)));
            //}

            //taskList.Add(Task.Run(async () => await actorCreationFunc(numActors % numThreads)));

            //await Task.WhenAll(taskList);

            string generateActorsUrl = $"{webServiceMigrationApiEndpoint}/generateActors/{numActors}/{numStatesPerActor}";
            await HttpClientUtility.PostAsync(generateActorsUrl);
        }

        public static async Task<long> GetActorsCountAsync(FabricClient fabricClient)
        {
            //string serviceUri = PerformanceTestingApplicationName + "/" + KVSActorServiceName;

            //ServicePartitionList partitions = await fabricClient.QueryManager.GetPartitionListAsync(new Uri(serviceUri));

            //long count = 0;
            //foreach (Partition partition in partitions)
            //{
            //    long partitionKey = ((Int64RangePartitionInformation)partition.PartitionInformation).LowKey;
            //    IActorService actorServiceProxy = ActorServiceProxy.Create(new Uri(serviceUri), partitionKey/*, this.ListnerName*/);

            //    ContinuationToken continuationToken = null;

            //    do
            //    {
            //        PagedResult<ActorInformation> page = await actorServiceProxy.GetActorsAsync(continuationToken, CancellationToken.None);

            //        count += page.Items.LongCount();

            //        continuationToken = page.ContinuationToken;
            //    }
            //    while (continuationToken != null);
            //}

            //return count;

            return Utils.ConvertToInt(await HttpClientUtility.GetAsync($"{webServiceMigrationApiEndpoint}/numActors"));
        }

        public static async Task<MigrationResult> GetMigrationStatusAsync()
        {
            //string serviceUri = PerformanceTestingApplicationName + "/" + RCActorServiceName;

            //HttpCommunicationClientFactory communicationFactory = new HttpCommunicationClientFactory();

            //ServicePartitionClient<HttpCommunicationClient> partitionClient
            //                    = new ServicePartitionClient<HttpCommunicationClient>(communicationFactory, new Uri(serviceUri), new ServicePartitionKey(0), TargetReplicaSelector.PrimaryReplica, "Migration Listener");

            //var result = await partitionClient.InvokeWithRetryAsync(
            //    async (client) =>
            //    {
            //        return await client.HttpClient.GetAsync(new Uri(client.Url + $"/RCMigration/GetMigrationStatus"));
            //    });

            //var jsonString = await result.Content.ReadAsStringAsync();

            //return JsonConvert.DeserializeObject<MigrationResult>(jsonString);

            string migrationStatusUrl = $"{webServiceMigrationApiEndpoint}/getMigrationStatus";
            return JsonConvert.DeserializeObject<MigrationResult>(await HttpClientUtility.GetAsync(migrationStatusUrl));
        }

        public static async Task ChangeActorStateAsync(ActorId[] actorIds, string[] newstates)
        {
            //string serviceUri = PerformanceTestingApplicationName + "/" + KVSActorServiceName;

            //for (int i = 0; i < actorIds.Length; i++)
            //{
            //    try
            //    {
            //        ActorId actorId = actorIds[i];
            //        IKVSActor proxy = ActorProxy.Create<IKVSActor>(actorId, new Uri(serviceUri)/*, this.ListnerName*/);
            //        await proxy.SetCountAsync(newstates[i], CancellationToken.None);
            //    }
            //    catch (Exception)
            //    {
            //        // gracefully catch Microsoft.ServiceFabric.Actors.Migration.Exceptions.ActorCallsDisallowedException
            //    }
            //};
        }
    }
}
