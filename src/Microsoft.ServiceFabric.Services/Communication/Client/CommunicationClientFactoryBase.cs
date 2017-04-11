﻿// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------
namespace Microsoft.ServiceFabric.Services.Communication.Client
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Services.Client;

    /// <summary>
    /// Provides the base implementation of ICommunicationClientFactory for creating communication clients to talk to service fabric services. Extend the
    /// CommunicationClientFactoryBase class to create communication clients for custom transport implementations. This class maintains a cache of communication
    /// clients and attempts to reuse the clients for requests to the same service endpoint.
    /// </summary>
    /// <typeparam name="TCommunicationClient">The type of communication client</typeparam>
    public abstract class CommunicationClientFactoryBase<TCommunicationClient> : ICommunicationClientFactory<TCommunicationClient>
        where TCommunicationClient : ICommunicationClient
    {
        private const string TraceType = "CommunicationClientFactoryBase";
        private readonly IServicePartitionResolver servicePartitionResolver;
        private readonly List<IExceptionHandler> exceptionHandlers;
        private readonly CommunicationClientCache<TCommunicationClient> cache;
        private readonly string traceId;
        private readonly Random random;
        private object randomLock;

        /// <summary>
        /// Gets the ServicePartitionResolver used by the client factory for resolving the service endpoint.
        /// </summary>
        /// <value>ServicePartitionResolver</value>
        public IServicePartitionResolver ServiceResolver
        {
            get { return this.servicePartitionResolver; }
        }

        /// <summary>
        /// Gets the custom exception handlers for handling exceptions on the client to service communication channel.
        /// </summary>
        /// <value>List of Exception handlers</value>
        public IEnumerable<IExceptionHandler> ExceptionHandlers
        {
            get { return this.exceptionHandlers; }
        }

        /// <summary>
        /// Gets the diagnostics trace identifier for this component.
        /// </summary>
        /// <value>Trace identifier</value>
        protected string TraceId
        {
            get { return this.traceId; }
        }

        /// <summary>
        /// Initializes a new instance of the communication client factory.
        /// </summary>
        /// <param name="servicePartitionResolver">Optional ServicePartitionResolver</param>
        /// <param name="exceptionHandlers">Optional Custom exception handlers for the exceptions on the Client to Service communication channel</param>
        /// <param name="traceId">Identifier to use in diagnostics traces from this component </param>
        protected CommunicationClientFactoryBase(
            IServicePartitionResolver servicePartitionResolver = null,
            IEnumerable<IExceptionHandler> exceptionHandlers = null,
            string traceId = null)
        {
            this.random = new Random();
            this.randomLock = new object();
            this.traceId = traceId ?? Guid.NewGuid().ToString();

            this.servicePartitionResolver = servicePartitionResolver ?? ServicePartitionResolver.GetDefault();

            this.exceptionHandlers = new List<IExceptionHandler>();
            if (exceptionHandlers != null)
            {
                this.exceptionHandlers.AddRange(exceptionHandlers);
            }

            this.cache = new CommunicationClientCache<TCommunicationClient>(this.traceId);

            ServiceTrace.Source.WriteInfo(
                TraceType,
                "{0} constructor",
                this.traceId);
        }
        
        /// <summary>
        /// Event handler that is fired when the Communication client connects to the service endpoint.
        /// </summary>
        public event EventHandler<CommunicationClientEventArgs<TCommunicationClient>> ClientConnected;

        /// <summary>
        /// Event handler that is fired when the Communication client disconnects from the service endpoint.
        /// </summary>
        public event EventHandler<CommunicationClientEventArgs<TCommunicationClient>> ClientDisconnected;

        /// <summary>
        /// Resolves a partition of the specified service containing one or more communication listeners and returns a client to communicate 
        /// to the endpoint corresponding to the given listenerName. 
        /// 
        /// The endpoint of the service is of the form - {"Endpoints":{"Listener1":"Endpoint1","Listener2":"Endpoint2" ...}}
        /// </summary>
        /// <param name="serviceUri">Uri of the service to resolve</param>
        /// <param name="partitionKey">Key that identifies the partition to resolve</param>
        /// <param name="targetReplicaSelector">Specifies which replica in the partition identified by the partition key, the client should connect to</param>
        /// <param name="listenerName">Specifies which listener in the endpoint of the chosen replica, to which the client should connect to</param>
        /// <param name="retrySettings">Specifies the retry policy that should be used for exceptions that occur when creating the client.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        /// the CommunicationClient(<see cref="ICommunicationClient" />) object.
        /// </returns>
        public async Task<TCommunicationClient> GetClientAsync(
            Uri serviceUri,
            ServicePartitionKey partitionKey,
            TargetReplicaSelector targetReplicaSelector,
            string listenerName,
            OperationRetrySettings retrySettings,
            CancellationToken cancellationToken)
        {
            var previousRsp = await this.ServiceResolver.ResolveAsync(
                serviceUri,
                partitionKey,
                ServicePartitionResolver.DefaultResolveTimeout,
                retrySettings.MaxRetryBackoffIntervalOnTransientErrors,
                cancellationToken);

            return await this.GetClientAsync(
                previousRsp,
                targetReplicaSelector,
                listenerName,
                retrySettings,
                cancellationToken);
        }

        /// <summary>
        /// Gets or Creates the CommunicationClient for the specified listener name by resolving based on the given previousRsp.
        /// </summary>
        /// <param name="previousRsp">Previous ResolvedServicePartition value</param>
        /// <param name="targetReplica">Specifies which replica in the partition identified by the partition key, the client should connect to</param>
        /// <param name="listenerName">Specifies which listener in the endpoint of the chosen replica, to which the client should connect to</param>
        /// <param name="retrySettings">Specifies the retry policy that should be used for exceptions that occur when creating the client.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        /// the CommunicationClient(<see cref="ICommunicationClient" />) object.
        /// </returns>
        public async Task<TCommunicationClient> GetClientAsync(
            ResolvedServicePartition previousRsp,
            TargetReplicaSelector targetReplica,
            string listenerName,
            OperationRetrySettings retrySettings,
            CancellationToken cancellationToken)
        {
            bool doResolve = false;
            var endpoint = this.GetEndpoint(previousRsp, targetReplica);
            CommunicationClientCacheEntry<TCommunicationClient> cacheEntry;
            if (this.cache.TryGetClientCacheEntry(
                previousRsp.Info.Id,
                endpoint,
                listenerName,
                out cacheEntry))
            {
                await cacheEntry.Semaphore.WaitAsync(cancellationToken);

                try
                {
                    TCommunicationClient validClient;
                    var clientValid = this.ValidateLockedClientCacheEntry(
                        cacheEntry,
                        previousRsp,
                        out validClient);

                    if (clientValid)
                    {
                        return validClient;
                    }
                    else
                    {
                        ServiceTrace.Source.WriteInfo(
                            TraceType,
                            "{0} Client not valid in Cached entry for ListenerName : {1} Address : {2} Role : {3}",
                            this.traceId,
                            listenerName,
                            endpoint.Address,
                            endpoint.Role);
                    }
                }
                finally
                {
                    cacheEntry.Semaphore.Release();
                }

                //
                // There was a cache hit, but the communication client in the cache is invalid. 
                // This could happen for these 2 cases,
                // 1. The endpoint and RSP information is valid, but there are no active users for the 
                //    communication client so the last reference to the client was GC'd.
                // 2. There was an exception during communication to the endpoint, and the ReportOperationException
                //    code path and the communication client was invalidated.
                //
                doResolve = true;
            }

            //
            // We did not find a cache entry or a valid client in our cache, so attempt to create a new client.
            //
            var newClient = await this.CreateClientWithRetriesAsync(
                previousRsp,
                targetReplica,
                listenerName,
                retrySettings,
                doResolve,
                cancellationToken);

            if (newClient != null)
            {
                this.OnClientConnected(newClient);
            }

            return newClient;
        }

        /// <summary>
        /// Handles the exceptions that occur in the CommunicationClient when sending a message to the Service
        /// </summary>
        /// <param name="client">Communication client</param>
        /// <param name="exceptionInformation">Information about the exception that occurred when communicating with the service.</param>
        /// <param name="retrySettings">Specifies the retry policy that should be used for handling the reported exception.</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// A <see cref="System.Threading.Tasks.Task">Task</see> that represents outstanding operation. The result of the Task is
        /// a <see cref="OperationRetryControl" /> object that determines
        /// how the retry policy for this exception.
        /// </returns>
        public async Task<OperationRetryControl> ReportOperationExceptionAsync(
            TCommunicationClient client,
            ExceptionInformation exceptionInformation,
            OperationRetrySettings retrySettings,
            CancellationToken cancellationToken)
        {
            var partitionId = client.ResolvedServicePartition.Info.Id;
            var entry = this.cache.GetOrAddClientCacheEntry(
                partitionId,
                client.Endpoint,
                client.ListenerName,
                client.ResolvedServicePartition);

            var faultedClient = default(TCommunicationClient);
            OperationRetryControl retval;

            await entry.Semaphore.WaitAsync(cancellationToken);
            try
            {
                ExceptionHandlingResult exceptionHandlingResult;
                var handled = this.HandleReportedException(
                    exceptionInformation,
                    retrySettings,
                    out exceptionHandlingResult);
                if (handled && (exceptionHandlingResult is ExceptionHandlingRetryResult))
                {
                    var retryResult = (ExceptionHandlingRetryResult) exceptionHandlingResult;

                    if (!retryResult.IsTransient && (ReferenceEquals(client, entry.Client)))
                    {
                        // The endpoint isn't valid if it is a re-triable error and not transient.
                        this.AbortClient(entry.Client);
                        faultedClient = entry.Client;
                        entry.Client = default(TCommunicationClient);
                        entry.Rsp = null;
                    }

                    retval = new OperationRetryControl()
                    {
                        ShouldRetry = true,
                        IsTransient = retryResult.IsTransient,
                        RetryDelay = retryResult.RetryDelay,
                        Exception = null,
                        ExceptionId = retryResult.ExceptionId,
                        MaxRetryCount = retryResult.MaxRetryCount
                    };
                }
                else
                {
                    retval = new OperationRetryControl()
                    {
                        ShouldRetry = false,
                        RetryDelay = Timeout.InfiniteTimeSpan,
                        Exception = exceptionInformation.Exception
                    };

                    var throwResult = exceptionHandlingResult as ExceptionHandlingThrowResult;
                    if ((throwResult != null) && (throwResult.ExceptionToThrow != null))
                    {
                        retval.Exception = throwResult.ExceptionToThrow;
                    }
                }
            }
            finally
            {
                entry.Semaphore.Release();
            }

            if (faultedClient != null)
            {
                this.OnClientDisconnected(faultedClient);
            }

            return retval;
        }

        /// <summary>
        /// Returns true if the client is still valid. Connection oriented transports can use this method to indicate that the client is no longer
        /// connected to the service.
        /// </summary>
        /// <param name="client">the communication client</param>
        /// <returns>true if the client is valid, false otherwise</returns>
        protected abstract bool ValidateClient(TCommunicationClient client);

        /// <summary>
        /// Returns true if the client is still valid and connected to the endpoint specified in the parameter.
        /// </summary>
        /// <param name="endpoint">Specifies the expected endpoint to which we think the client is connected to</param>
        /// <param name="client">the communication client</param>
        /// <returns>true if the client is valid, false otherwise</returns>
        protected abstract bool ValidateClient(
            string endpoint,
            TCommunicationClient client);

        /// <summary>
        /// Creates a communication client for the given endpoint address.
        /// </summary>
        /// <param name="endpoint">listener address where the replica is listening</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>The communication client that was created</returns>
        protected abstract Task<TCommunicationClient> CreateClientAsync(
            string endpoint,
            CancellationToken cancellationToken);

        /// <summary>
        /// Aborts the given client
        /// </summary>
        /// <param name="client">Communication client</param>
        protected abstract void AbortClient(
            TCommunicationClient client);

        private async Task<TCommunicationClient> CreateClientWithRetriesAsync(
            ResolvedServicePartition previousRsp,
            TargetReplicaSelector targetReplicaSelector,
            string listenerName,
            OperationRetrySettings retrySettings,
            bool doInitialResolve,
            CancellationToken cancellationToken)
        {
            var doResolve = doInitialResolve;
            var currentRetryCount = 0;
            string currentExceptionId = null;

            while (true)
            {
                ExceptionHandlingResult result;
                Exception actualException;
                try
                {
                    if (doResolve)
                    {
                        var rsp = await this.ServiceResolver.ResolveAsync(
                            previousRsp,
                            ServicePartitionResolver.DefaultResolveTimeout,
                            retrySettings.MaxRetryBackoffIntervalOnTransientErrors,
                            cancellationToken);
                        previousRsp = rsp;
                    }

                    var endpoint = this.GetEndpoint(previousRsp, targetReplicaSelector);
                    var cacheEntry = await this.GetAndLockClientCacheEntry(previousRsp.Info.Id, endpoint, listenerName, previousRsp, cancellationToken);

                    TCommunicationClient client;
                    try
                    {
                        var clientValid = this.ValidateLockedClientCacheEntry(
                            cacheEntry,
                            previousRsp,
                            out client);

                        if (!clientValid)
                        {
                            ServiceTrace.Source.WriteInfo(
                                TraceType,
                                "{0} Creating Client for connecting to ListenerName : {1} Address : {2} Role : {3}",
                                this.traceId,
                                listenerName,
                                cacheEntry.GetEndpoint(),
                                cacheEntry.Endpoint.Role);

                            cacheEntry.Rsp = previousRsp;
                            client = await this.CreateClientAsync(cacheEntry.GetEndpoint(), cancellationToken);
                            cacheEntry.Client = client;
                            client.ResolvedServicePartition = cacheEntry.Rsp;
                            client.ListenerName = cacheEntry.ListenerName;
                            client.Endpoint = cacheEntry.Endpoint;
                        }
                        else
                        {
                            ServiceTrace.Source.WriteInfo(
                                TraceType,
                                "{0} Found valid client for ListenerName : {1} Address : {2} Role : {3}",
                                this.traceId,
                                listenerName,
                                endpoint.Address,
                                endpoint.Role);
                        }
                    }
                    finally
                    {
                        cacheEntry.Semaphore.Release();
                    }

                    return client;
                }
                catch (Exception e)
                {
                    ServiceTrace.Source.WriteInfo(
                        TraceType,
                        "{0} Exception While CreatingClient {1}",
                        this.traceId,
                        e);

                    if (!this.HandleReportedException(
                        new ExceptionInformation(e, targetReplicaSelector),
                        retrySettings,
                        out result))
                    {
                        throw;
                    }

                    var throwResult = result as ExceptionHandlingThrowResult;
                    if (throwResult != null)
                    {
                        if (ReferenceEquals(e, throwResult.ExceptionToThrow)) throw;
                        throw throwResult.ExceptionToThrow;
                    }

                    // capture the exception so that we can throw based on the retry policy
                    actualException = e;
                }

                var retryResult = (ExceptionHandlingRetryResult) result;
                if (!Utility.ShouldRetryOperation(
                    retryResult.ExceptionId,
                    retryResult.MaxRetryCount,
                    ref currentExceptionId,
                    ref currentRetryCount))
                {
                    ServiceTrace.Source.WriteInfo(
                        TraceType,
                        "{0} Retry count for exception id {1} exceeded the retry limit : {2}, throwing exception - {3}",
                        this.traceId,
                        retryResult.ExceptionId,
                        retryResult.MaxRetryCount,
                        actualException);

                    throw new AggregateException(actualException);
                }

                doResolve = !retryResult.IsTransient;
                await Task.Delay(retryResult.RetryDelay, cancellationToken);
            }
        }

        private bool HandleReportedException(
            ExceptionInformation exceptionInformation,
            OperationRetrySettings retrySettings,
            out ExceptionHandlingResult result)
        {
            var aggregateException = exceptionInformation.Exception as AggregateException;
            if (aggregateException == null)
            {
                return this.TryHandleException(
                    exceptionInformation,
                    retrySettings,
                    out result);
            }

            foreach (var innerException in aggregateException.Flatten().InnerExceptions)
            {
                if (this.TryHandleException(
                    new ExceptionInformation(innerException, exceptionInformation.TargetReplica),
                    retrySettings,
                    out result))
                {
                    return true;
                }
            }

            result = null;
            return false;
        }

        private bool TryHandleException(
            ExceptionInformation exceptionInformation,
            OperationRetrySettings retrySettings,
            out ExceptionHandlingResult result)
        {
            foreach (var handler in this.exceptionHandlers)
            {
                if (handler.TryHandleException(
                    exceptionInformation,
                    retrySettings,
                    out result))
                {
                    return true;
                }
            }

            result = null;
            return false;
        }

        private void OnClientDisconnected(TCommunicationClient faultedClient)
        {
            var clientDisconnectedEvent = this.ClientDisconnected;
            if (clientDisconnectedEvent != null)
            {
                clientDisconnectedEvent(
                    this,
                    new CommunicationClientEventArgs<TCommunicationClient>()
                    {
                        Client = faultedClient
                    });
            }
        }

        private void OnClientConnected(TCommunicationClient newClient)
        {
            var clientCreatedEvent = this.ClientConnected;
            if (clientCreatedEvent != null)
            {
                clientCreatedEvent(
                    this,
                    new CommunicationClientEventArgs<TCommunicationClient>()
                    {
                        Client = newClient
                    });
            }
        }

        private ResolvedServiceEndpoint GetEndpoint(
            ResolvedServicePartition rsp,
            TargetReplicaSelector targetReplica)
        {
            if (rsp.Endpoints.Count == 0)
            {
                // We should never get to this condition, if we are using complaint based
                // service resolution.
                throw new FabricServiceNotFoundException(
                    string.Format(
                        CultureInfo.CurrentCulture,
                        SR.ErrorServiceDoesNotExist,
                        rsp.ServiceName));
            }

            var endpoint = rsp.Endpoints.First();
            if (endpoint.Role == ServiceEndpointRole.Stateless)
            {
                if (targetReplica != TargetReplicaSelector.RandomInstance)
                {
                    throw new ArgumentException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            SR.ErrorCommunicationTargetSelectorInvalidStateless,
                            targetReplica));
                }

                return rsp.Endpoints.ElementAt(this.NextRandom(rsp.Endpoints.Count));
            }

            // endpoint role is stateful

            if (targetReplica == TargetReplicaSelector.PrimaryReplica)
            {
                return rsp.GetEndpoint();
            }

            if (targetReplica == TargetReplicaSelector.RandomReplica)
            {
                return rsp.Endpoints.ElementAt(this.NextRandom(rsp.Endpoints.Count));
            }

            if (targetReplica == TargetReplicaSelector.RandomSecondaryReplica)
            {
                var secondaryEndpoints = rsp.Endpoints.Where(rsEndpoint => rsEndpoint.Role != ServiceEndpointRole.StatefulPrimary);
                if (!secondaryEndpoints.Any())
                {
                    // This can happen if the stateful service partition has the min and target replica set size as 1.
                    throw new ArgumentException(
                        string.Format(
                            CultureInfo.CurrentCulture,
                            SR.ErrorCommunicationTargetSelectorEndpointNotFound,
                            rsp.ServiceName,
                            rsp.Info.Id,
                            targetReplica));
                }

                return secondaryEndpoints.ElementAt(this.NextRandom(secondaryEndpoints.Count()));
            }

            throw new ArgumentException(
                string.Format(
                    CultureInfo.CurrentCulture,
                    SR.ErrorCommunicationTargetSelectorInvalidStateful,
                    targetReplica));
        }

        private int NextRandom(int upperBound)
        {
            int rand = 0;
            lock(this.randomLock)
            {
                rand = this.random.Next(upperBound);
            }

            return rand;
        }

        private async Task<CommunicationClientCacheEntry<TCommunicationClient>> GetAndLockClientCacheEntry(
            Guid partitionId,
            ResolvedServiceEndpoint endpoint,
            string listenerName,
            ResolvedServicePartition rsp,
            CancellationToken cancellationToken)
        {
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var entry = this.cache.GetOrAddClientCacheEntry(partitionId, endpoint, listenerName, rsp);

                await entry.Semaphore.WaitAsync(cancellationToken);
                if (entry.IsInCache)
                {
                    return entry;
                }

                //
                // The cache entry has been removed from the cache before the cache entry lock could be acquired.
                // So get a new entry from the cache.
                //
                entry.Semaphore.Release();
            } while (true);
        }

        //
        // This method validates if client stored in the cache entry is valid. If the client is valid,
        // this method sets the 'client' out param and returns true. If the client is not valid, it returns
        // false.
        //
        private bool ValidateLockedClientCacheEntry(
            CommunicationClientCacheEntry<TCommunicationClient> cacheEntry,
            ResolvedServicePartition rsp,
            out TCommunicationClient client)
        {
            client = cacheEntry.Client;
            var faultedClient = default(TCommunicationClient);

            // check if we have a cached client
            if (client != null)
            {
                // we have a cached client, check when was it created
                if (cacheEntry.Rsp.CompareVersion(rsp) >= 0)
                {
                    // it was created with the same RSP or higher version RSP
                    // check if the client is still valid and not faulted
                    if (!this.ValidateClient(client))
                    {
                        // client is not valid, abort the client, and recreate
                        this.AbortClient(client);
                        faultedClient = client;
                        cacheEntry.Client = default(TCommunicationClient);
                        client = default(TCommunicationClient);
                    }
                    else
                    {
                        // client is valid, return the valid client
                        return true;
                    }
                }
                else
                {
                    // we have cached client, but it was created with older version of RSP
                    // check if the client is still valid
                    if (this.ValidateClient(cacheEntry.GetEndpoint(), client))
                    {
                        // the client is valid, but was initially created with an older version of RSP,
                        // this could happen in cases when services listen on the same endpoint after failover.
                        // replace the RSP in the entry and client, so that we use the right RSP incase of complaint
                        // based resolution in future.
                        cacheEntry.Rsp = rsp;
                        client.ResolvedServicePartition = rsp;
                        return true;
                    }
                    else
                    {
                        // the client is not valid, abort the client
                        this.AbortClient(client);
                        faultedClient = client;
                        cacheEntry.Client = default(TCommunicationClient);
                        client = default(TCommunicationClient);
                    }
                }
            }

            if (faultedClient != null)
            {
                this.OnClientDisconnected(faultedClient);
            }

            return false;
        }
    }
}