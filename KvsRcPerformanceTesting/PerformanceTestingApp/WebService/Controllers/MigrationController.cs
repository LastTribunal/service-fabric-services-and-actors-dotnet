using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Client;
using Microsoft.ServiceFabric.Actors.Query;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Client;
using Migration.Interfaces;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http;
using KVSActor.Interfaces;
using Microsoft.ServiceFabric.Services.Remoting.Client;

namespace WebService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MigrationController : ControllerBase
    {
        private readonly FabricClient fabricClient;
        private readonly StatelessServiceContext serviceContext;
        private readonly string KVSActorServiceName = "KVSActorService";
        private readonly string RCActorServiceName = "RCActorService";

        public MigrationController(StatelessServiceContext serviceContext, FabricClient fabricClient)
        {
            this.serviceContext = serviceContext;
            this.fabricClient = fabricClient;
        }

        [HttpGet("numActors")]
        public async Task<IActionResult> GetActorsCountAsync()
        {
            try
            {
                string serviceUri = this.serviceContext.CodePackageActivationContext.ApplicationName + "/" + this.KVSActorServiceName;

                ServicePartitionList partitions = await this.fabricClient.QueryManager.GetPartitionListAsync(new Uri(serviceUri));

                long count = 0;
                foreach (Partition partition in partitions)
                {
                    long partitionKey = ((Int64RangePartitionInformation)partition.PartitionInformation).LowKey;
                    IActorService actorServiceProxy = ActorServiceProxy.Create(new Uri(serviceUri), partitionKey/*, this.ListnerName*/);

                    ContinuationToken continuationToken = null;

                    do
                    {
                        PagedResult<ActorInformation> page = await actorServiceProxy.GetActorsAsync(continuationToken, CancellationToken.None);

                        count += page.Items.LongCount();

                        continuationToken = page.ContinuationToken;
                    }
                    while (continuationToken != null);
                }

                return this.Ok(count);
            }
            catch (Exception ex)
            {
                return this.Ok(ex.Message);
            }
        }

        [HttpGet("GetActorServiceName")]
        public async Task<IActionResult> GetActorServiceNameAsync()
        {
            try
            {
                string serviceUri = this.serviceContext.CodePackageActivationContext.ApplicationName + "/" + this.RCActorServiceName;
                ActorId actorId = ActorId.CreateRandom();
                IKVSActor proxy = ActorProxy.Create<IKVSActor>(actorId, new Uri(serviceUri)/*, this.ListnerName*/);
                return this.Ok(await proxy.GetServiceNameAsync());
            }
            catch (Exception ex)
            {
                return this.Ok(ex.Message);
            }
        }


        [HttpPost("generateActors/{num}/{numStatesPerActor}")]
        public async Task<IActionResult> GenerateActorAsync(int num, int numStatesPerActor)
        {
            try
            {
                string serviceUri = this.serviceContext.CodePackageActivationContext.ApplicationName + "/" + this.KVSActorServiceName;
                Random generator = new Random();

                int numThreads = 128;
                List<Task> taskList = new List<Task>();

                Func<int, Task> actorCreationFunc = async (num) =>
                {
                    for (int i = 0; i < num; i++)
                    {
                        ActorId actorId = ActorId.CreateRandom();
                        IKVSActor proxy = ActorProxy.Create<IKVSActor>(actorId, new Uri(serviceUri)/*, this.ListnerName*/);
                        await proxy.CreateRandomStatesAsync(numStatesPerActor);
                    };
                };

                for (int i = 0; i < numThreads; i++)
                {
                    taskList.Add(Task.Run(async () => await actorCreationFunc(num / numThreads)));
                }

                taskList.Add(Task.Run(async () => await actorCreationFunc(num % numThreads)));

                await Task.WhenAll(taskList);

                return this.Ok($"Created {num} actors");
            }
            catch (Exception ex)
            {
                return this.Ok(ex.Message);
            }
        }

        [HttpGet("getMigrationStatus")]
        public async Task<IActionResult> GetMigrationStatusAsync()
        {
            try
            {
                string serviceUri = this.serviceContext.CodePackageActivationContext.ApplicationName + "/" + this.RCActorServiceName;

                HttpCommunicationClientFactory communicationFactory = new HttpCommunicationClientFactory();

                ServicePartitionClient<HttpCommunicationClient> partitionClient
                                 = new ServicePartitionClient<HttpCommunicationClient>(communicationFactory, new Uri(serviceUri), new ServicePartitionKey(0), TargetReplicaSelector.PrimaryReplica, "Migration Listener");

                var result = await partitionClient.InvokeWithRetryAsync(
                    async (client) =>
                    {
                        return await client.HttpClient.GetAsync(new Uri(client.Url + $"/RCMigration/GetMigrationStatus"));
                    });

                var jsonString = await result.Content.ReadAsStringAsync();

                return this.Ok(jsonString);
            }
            catch (Exception ex)
            {
                return this.Ok(ex.Message);
            }
        }

        [HttpPost("performKVSBackup")]
        public async Task<IActionResult> PerformKVSBackupAsync()
        {
            string serviceUri = this.serviceContext.CodePackageActivationContext.ApplicationName + "/" + this.KVSActorServiceName;

            IKVSActorService kvsActorService = ServiceProxy.Create<IKVSActorService>(new Uri(serviceUri), new ServicePartitionKey(0), TargetReplicaSelector.PrimaryReplica, "V2Listener");

            await kvsActorService.PerFormBackupAsync();

            return this.Ok();
        }
    }
}
