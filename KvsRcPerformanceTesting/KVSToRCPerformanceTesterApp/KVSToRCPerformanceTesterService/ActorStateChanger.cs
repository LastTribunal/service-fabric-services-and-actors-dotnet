using Microsoft.ServiceFabric.Actors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KVSToRCPerformanceTesterService
{
    internal static class ActorStateChanger
    {
        public static async Task Run(int numActorChanges, CancellationToken cancellationToken)
        {
            //ActorId[] actorIds = new ActorId[numActorChanges];
            //for (int i = 0; i < numActorChanges; i++)
            //{
            //    actorIds[i] = ActorId.CreateRandom();
            //}

            //Random generator = new Random();

            while (true)
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    //int[] newStates = new int[numActorChanges];
                    //for (int i = 0; i < numActorChanges; i++)
                    //{
                    //    newStates[i] = generator.Next();
                    //}

                    //await ActorProxyMethods.ChangeActorStateAsync(actorIds, newStates);

                    await ActorProxyMethods.GenerateActorsAsync(1, numActorChanges);
                }
                catch (OperationCanceledException)
                {
                    Utils.Log("Cancelation triggered. Exitiing Actor state changer");
                    break;
                }
                catch (HttpRequestException e)
                {
                    // ignore any Exceptions
                    Utils.LogVerbose($"HttpRequestException inside ActorStateChanger: {e}");
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }
    }
}
