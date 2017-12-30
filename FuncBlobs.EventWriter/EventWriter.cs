using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FuncBlobs.Formats;
using FuncBlobs.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FuncBlobs.EventWriter
{
    public static class EventWriter
    {
        private static ThermostatLogFormatter formatter = new ThermostatLogFormatter();
        private static ConcurrentDictionary<string, CloudBlobClient> blobClients = new ConcurrentDictionary<string, CloudBlobClient>();

        [FunctionName("EventWriter")]
        public static async Task Run(
            [EventHubTrigger("%HubName%", Connection = "EventsConnectionString")] EventData[] events,
            [DocumentDB("pbatum-funcblob-store", "thermostat-logs",  ConnectionStringSetting = "CosmosConnectionString")] IAsyncCollector<string> thermostatLogOutput,
            TraceWriter log)
        {
            foreach(var eventGridPayload in events)
            {
                log.Info("EVENT");                
                var payloadString = Encoding.UTF8.GetString(eventGridPayload.GetBytes());
                var eventArray = JToken.Parse(payloadString);
                foreach(dynamic e in eventArray)
                {                    
                    string eventType = e.eventType;                    
                    if (eventType != "Microsoft.Storage.BlobCreated")
                    {
                        log.Warning($"Recieved event other than blob create, got: '{eventType}', skipping.");
                        continue;
                    }

                    var blobUri = new Uri((string)e.data.url);
                    var filename = blobUri.Segments.Last();
                    if (filename.EndsWith(".csv") == false)
                    {
                        log.Warning($"Recieved event for non-csv file '{filename}', skipping.");
                        continue;
                    }

                    string topic = e.topic;
                    string accountName = topic.Substring(topic.LastIndexOf('/') + 1);

                    var blobClient = blobClients.GetOrAdd(accountName, (accName) =>
                    {
                        string connectionEnvironmentVariableName = $"STORAGECONNECTION_{accountName}";
                        string connectionString = Environment.GetEnvironmentVariable(connectionEnvironmentVariableName)
                            ?? throw new Exception($"Cannot find connection string environment variable '{connectionEnvironmentVariableName}'.");

                        CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
                        return account.CreateCloudBlobClient();
                    });
                    
                    var blob = await blobClient.GetBlobReferenceFromServerAsync(blobUri);
                    using (var stream = await blob.OpenReadAsync())
                    using (var reader = new StreamReader(stream))
                    {                                                
                        var csvUtils = new CsvUtils();

                        ThermostatLog thermostatLog = ThermostatLog.ParseFileIdentifier(Path.GetFileNameWithoutExtension(filename));                        
                        thermostatLog.Readings = csvUtils.FromCsv(reader);
                        string json = formatter.ToJson(thermostatLog);    
                        
                        await thermostatLogOutput.AddAsync(json);
                        await thermostatLogOutput.FlushAsync();
                        log.Info($"Wrote {filename} to cosmos.");
                    }
                }
            }

        }
    }
}
