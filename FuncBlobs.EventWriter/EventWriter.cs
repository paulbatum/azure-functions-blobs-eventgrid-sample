using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FuncBlobs.Formats;
using FuncBlobs.Models;
using Microsoft.Azure.EventGrid;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FuncBlobs.EventWriter
{
    public static class EventWriter
    {        
        private static readonly string containerName = Environment.GetEnvironmentVariable("ContainerName") ?? "container01";
        private static EventGridSubscriber subscriber = new EventGridSubscriber();
        private static ThermostatLogFormatter formatter = new ThermostatLogFormatter();
        private static ConcurrentDictionary<string, CloudBlobClient> blobClients = new ConcurrentDictionary<string, CloudBlobClient>();

        [FunctionName("EventWriter")]
        public static async Task Run(
            [EventHubTrigger("%HubName%", Connection = "EventsConnectionString")] EventData[] events,
            [CosmosDB("%CosmosDatabaseName%", "%CosmosCollectionName%", ConnectionStringSetting = "CosmosConnectionString")] IAsyncCollector<string> thermostatLogOutput,
            [Queue("blobevents-deadletter")] IAsyncCollector<string> deadLetterOutput,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventGridPayload in events)
            {
                // Safe to assume that our event hub will only have UTF8 messages
                var messageBody = Encoding.UTF8.GetString(eventGridPayload.Body.Array, eventGridPayload.Body.Offset, eventGridPayload.Body.Count);

                try
                {                    
                    var eventGridEvents = subscriber.DeserializeEventGridEvents(messageBody);
                    await ProcessEvents(eventGridEvents, thermostatLogOutput, log);
                }
                catch(Exception e)
                {
                    // We need to keep processing the rest of the batch - deadletter this particular failure and continue
                    await deadLetterOutput.AddAsync(messageBody);                    
                    exceptions.Add(e);
                }                
            }

            exceptions.ThrowIfAny();
        }

        private static async Task ProcessEvents(IEnumerable<EventGridEvent> eventGridEvents, IAsyncCollector<string> thermostatLogOutput, ILogger log)
        {            
            foreach (EventGridEvent e in eventGridEvents)
            {
                if (e.EventType != EventTypes.StorageBlobCreatedEvent)
                {
                    log.LogWarning($"Recieved event other than blob create, got: '{e.EventType}', skipping.");
                    continue;
                }

                if (e.Subject.StartsWith($"/blobServices/default/containers/{containerName}") == false)
                {
                    log.LogInformation($"Received event for unmonitored container, skipping.");
                    continue;
                }

                var blobCreatedEvent = (StorageBlobCreatedEventData)e.Data;

                var blobUri = new Uri(blobCreatedEvent.Url);
                var filename = blobUri.Segments.Last();
                if (filename.EndsWith(".csv") == false)
                {
                    log.LogWarning($"Recieved event for non-csv file '{filename}', skipping.");
                    continue;
                }


                string accountName = e.Topic.Substring(e.Topic.LastIndexOf('/') + 1);

                var blobClient = blobClients.GetOrAdd(accountName, (accName) =>
                {
                    string connectionEnvironmentVariableName = $"STORAGECONNECTION_{accName}";
                    string connectionString = Environment.GetEnvironmentVariable(connectionEnvironmentVariableName)
                        ?? throw new Exception($"Cannot find connection string environment variable '{connectionEnvironmentVariableName}'.");

                    CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
                    return account.CreateCloudBlobClient();
                });

                var blob = await blobClient.GetBlobReferenceFromServerAsync(blobUri);
                using (var stream = await blob.OpenReadAsync(AccessCondition.GenerateEmptyCondition(), new BlobRequestOptions(), new OperationContext()))
                using (var reader = new StreamReader(stream))
                {
                    ThermostatLog thermostatLog = ThermostatLog.ParseFileIdentifier(Path.GetFileNameWithoutExtension(filename));

                    var csvUtils = new CsvUtils();
                    thermostatLog.Readings = csvUtils.FromCsv(reader);
                    string json = formatter.ToJson(thermostatLog);

                    await thermostatLogOutput.AddAsync(json);
                    await thermostatLogOutput.FlushAsync();
                    log.LogInformation($"Wrote {filename} to cosmos.");
                }              
            }


        }
    }

    public static class EventWriterExtensions
    {
        public static void ThrowIfAny(this IEnumerable<Exception> exceptions)
        {
            if (exceptions.Count() > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count() == 1)
                throw exceptions.Single();
        }
    }
}
