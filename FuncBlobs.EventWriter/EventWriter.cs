using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
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
        [FunctionName("EventWriter")]
        public static async Task Run([EventHubTrigger("%HubName%", Connection = "EventsConnectionString")] EventData[] events, TraceWriter log)
        {
            foreach(var eventGridPayload in events)
            {
                log.Info("EVENT");                
                var payloadString = Encoding.UTF8.GetString(eventGridPayload.GetBytes());
                var eventArray = JToken.Parse(payloadString);
                foreach(dynamic e in eventArray)
                {
                    if (e.eventType != "Microsoft.Storage.BlobCreated")
                        throw new Exception("Expect to only recieve blob create events, got: " + e.eventType);

                    string topic = e.topic;
                    string url = e.data.url;
                    string accountName = topic.Substring(topic.LastIndexOf('/') + 1);
                    log.Info($"URL: {url}");
                    var uri = new Uri(url);

                    var connectionEnvironmentVariableName = $"STORAGECONNECTION_{accountName}";
                    var connectionString = Environment.GetEnvironmentVariable(connectionEnvironmentVariableName)
                        ?? throw new Exception($"Cannot find connection string environment variable '{connectionEnvironmentVariableName}'.");

                    CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
                    var blobClient = account.CreateCloudBlobClient();
                    var blob = await blobClient.GetBlobReferenceFromServerAsync(uri);
                    var stream = await blob.OpenReadAsync();
                    var reader = new StreamReader(stream);
                    var contents = await reader.ReadToEndAsync();
                    log.Info(contents);
                }
            }

        }
    }
}
