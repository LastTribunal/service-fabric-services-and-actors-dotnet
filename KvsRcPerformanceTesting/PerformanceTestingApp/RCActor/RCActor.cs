using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Runtime;
using Microsoft.ServiceFabric.Actors.Client;
using Migration.Interfaces;
using Microsoft.ServiceFabric.Actors.Runtime.Migration;

namespace RCActor
{
    /// <remarks>
    /// This class represents an actor.
    /// Every ActorID maps to an instance of this class.
    /// The StatePersistence attribute determines persistence and replication of actor state:
    ///  - Persisted: State is written to disk and replicated.
    ///  - Volatile: State is kept in memory only and replicated.
    ///  - None: State is kept in memory only and not replicated.
    /// </remarks>
    [StateMigration(StateMigration.Target)]
    [StatePersistence(StatePersistence.Persisted)]
    internal class RCActor : Actor, IKVSActor
    {
        /// <summary>
        /// Initializes a new instance of RCActor
        /// </summary>
        /// <param name="actorService">The Microsoft.ServiceFabric.Actors.Runtime.ActorService that will host this actor instance.</param>
        /// <param name="actorId">The Microsoft.ServiceFabric.Actors.ActorId for this actor instance.</param>
        public RCActor(ActorService actorService, ActorId actorId) 
            : base(actorService, actorId)
        {
        }

        /// <summary>
        /// This method is called whenever an actor is activated.
        /// An actor is activated the first time any of its methods are invoked.
        /// </summary>
        protected override Task OnActivateAsync()
        {
            ActorEventSource.Current.ActorMessage(this, "Actor activated.");

            // The StateManager is this actor's private state store.
            // Data stored in the StateManager will be replicated for high-availability for actors that use volatile or persisted state storage.
            // Any serializable object can be saved in the StateManager.
            // For more information, see https://aka.ms/servicefabricactorsstateserialization

            return this.StateManager.TryAddStateAsync("count", 0);
        }

        public async Task CreateRandomStatesAsync(int numStates)
        {
            Random generator = new Random();

            for (int i = 0; i < numStates; i++)
            {
                string actorState = Guid.NewGuid().ToString();
                int actorStateValue = generator.Next();

                await this.StateManager.AddOrUpdateStateAsync(actorState, actorStateValue, (_, __) => actorStateValue);
            }
        }

        /// <summary>
        /// TODO: Replace with your own actor method.
        /// </summary>
        /// <returns></returns>
        Task<int> IKVSActor.GetCountAsync(CancellationToken cancellationToken)
        {
            return this.StateManager.GetStateAsync<int>("count", cancellationToken);
        }

        /// <summary>
        /// TODO: Replace with your own actor method.
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        Task IKVSActor.SetCountAsync(int count, CancellationToken cancellationToken)
        {
            // Requests are not guaranteed to be processed in order nor at most once.
            // The update function here verifies that the incoming count is greater than the current count to preserve order.
            return this.StateManager.AddOrUpdateStateAsync("count", count, (key, value) => count > value ? count : value, cancellationToken);
        }

        Task<string> IKVSActor.GetServiceNameAsync()
        {
            return Task.FromResult(this.ActorService.Context.ServiceName.ToString());
        }
    }
}
