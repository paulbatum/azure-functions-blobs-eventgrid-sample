using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using FuncBlobs.Formats;
using FuncBlobs.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace FuncBlobs.LoadGenerator
{
    public static class BlobWriter
    {
        private static CsvUtils csvUtils = new CsvUtils();
        private static ThermostatLogFormatter formatter = new ThermostatLogFormatter();
        private static ConcurrentDictionary<string, CloudBlobClient> blobClients = new ConcurrentDictionary<string, CloudBlobClient>();

        [FunctionName("BlobWriter")]
        public static async Task Run(
            [QueueTrigger("%LoadQueueName%", Connection = "AzureWebJobsStorage")] LoadJob job,
            ILogger log)
        {
            var blobClient = blobClients.GetOrAdd(job.Account, (accName) =>
            {
                string connectionEnvironmentVariableName = $"STORAGECONNECTION_{job.Account}";
                string connectionString = Environment.GetEnvironmentVariable(connectionEnvironmentVariableName)
                    ?? throw new Exception($"Cannot find connection string environment variable '{connectionEnvironmentVariableName}'.");

                CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
                return account.CreateCloudBlobClient();
            });

            var container = blobClient.GetContainerReference(job.Container);

            var inputBlock = new BufferBlock<ThermostatLog>();            

            var sendBlock = new ActionBlock<ThermostatLog>(async thermostatLog =>
            {
                var filename = $"{thermostatLog.GetFileIdentifier()}.csv";

                var blob = container.GetBlockBlobReference(filename);
                blob.Properties.ContentType = "text/csv";

                using (var stream = await blob.OpenWriteAsync())
                using (var writer = new StreamWriter(stream))
                {
                    csvUtils.ToCsv(writer, thermostatLog.Readings);
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 16 });

            inputBlock.LinkTo(sendBlock, new DataflowLinkOptions { PropagateCompletion = true });

            var random = new Random();
            for (int i = 0; i < job.Size; i++)
                inputBlock.Post(ThermostatLog.GenerateRandomLog(random));

            inputBlock.Complete();
            await sendBlock.Completion;
        }
    }
}
