using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Runtime;
using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KVSActor.Interfaces;
using System.IO.Compression;
using System.IO;

namespace KVSActor
{
    internal class KVSActorService : ActorService, IKVSActorService
    {
        private string BackupContainerName;
        private string BackupZipFileName;
        private string AZURE_STORAGE_CONNECTION_STRING;

        public KVSActorService(StatefulServiceContext context, ActorTypeInformation actorTypeInfo, Func<ActorService, ActorId, ActorBase> actorFactory = null, Func<ActorBase, IActorStateProvider, IActorStateManager> stateManagerFactory = null, IActorStateProvider stateProvider = null, ActorServiceSettings settings = null) : base(context, actorTypeInfo, actorFactory, stateManagerFactory, stateProvider, settings)
        {
            var performanceTestingConfig = this.ServiceContext.CodePackageActivationContext.GetConfigurationPackageObject("Config").Settings.Sections["PerformanceTestingConfig"];
            this.BackupContainerName = performanceTestingConfig.Parameters["BackupContainerName"].Value;
            this.BackupZipFileName = performanceTestingConfig.Parameters["KVSBackupZipFileName"].Value;
            this.AZURE_STORAGE_CONNECTION_STRING = performanceTestingConfig.Parameters["BackupStorageAccountConnectionString"].Value;
        }

        public async Task PerFormBackupAsync()
        {
            BackupDescription myBackupDescription = new BackupDescription(BackupOption.Full, this.BackupCallbackAsync);
            await this.BackupAsync(myBackupDescription);
        }

        private async Task<bool> BackupCallbackAsync(BackupInfo backupInfo, CancellationToken cancellationToken)
        {
            // create blob client
            BlobServiceClient blobServiceClient = new BlobServiceClient(AZURE_STORAGE_CONNECTION_STRING);
            BlobContainerClient containerClient = await blobServiceClient.CreateBlobContainerAsync(BackupContainerName, PublicAccessType.BlobContainer);
            BlobClient blobClient = containerClient.GetBlobClient(BackupZipFileName);

            // compress the backup folder
            string localBackupZipFilePath = Path.Join(Path.GetTempPath(), BackupZipFileName);
            if (File.Exists(localBackupZipFilePath))
            {
                File.Delete(localBackupZipFilePath);
            }
            ZipFile.CreateFromDirectory(backupInfo.Directory, localBackupZipFilePath);

            // upload the zip to blob storage
            await blobClient.UploadAsync(localBackupZipFilePath, true);

            return true;
        }

        protected override async Task<bool> OnDataLossAsync(RestoreContext restoreCtx, CancellationToken cancellationToken)
        {
            // create blob client
            BlobServiceClient blobServiceClient = new BlobServiceClient(AZURE_STORAGE_CONNECTION_STRING);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(BackupContainerName);
            BlobClient blobClient = containerClient.GetBlobClient(BackupZipFileName);

            // download backup zip to temp folder
            string localBackupZipFilePath = Path.Join(Path.GetTempPath(), BackupZipFileName);
            if (File.Exists(localBackupZipFilePath))
            {
                File.Delete(localBackupZipFilePath);
            }

            try
            {
                await blobClient.DownloadToAsync(localBackupZipFilePath);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            // extract backup zip
            string extractPath = Path.Join(Path.GetTempPath(), "KVSActorBackup");
            if (File.Exists(extractPath))
            {
                File.Delete(extractPath);
            }
            ZipFile.ExtractToDirectory(localBackupZipFilePath, extractPath);

            // delete local zip path
            File.Delete(localBackupZipFilePath);

            // do state backup
            var restoreDescription = new RestoreDescription(extractPath);
            await restoreCtx.RestoreAsync(restoreDescription);

            return true;
        }
    }
}
