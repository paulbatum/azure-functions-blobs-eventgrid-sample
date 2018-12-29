using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FuncBlobs.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.SignalRService;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FuncBlobs.DataProcessing
{
    public static class ThermostatLogProcessor
    {

        [FunctionName("negotiate")]
        public static SignalRConnectionInfo GetSignalRInfo(
            [HttpTrigger(AuthorizationLevel.Anonymous)] HttpRequest req,
            [SignalRConnectionInfo(HubName = "readings", ConnectionStringSetting = "AzureSignalRConnectionString")] SignalRConnectionInfo connectionInfo)
        {
            return connectionInfo;
        }



        [FunctionName("ThermostatLogProcessor")]
        public static async Task Run(
            [CosmosDBTrigger(
                databaseName: "%ThermostatLogDatabaseName%",
                collectionName: "%ThermostatLogCollectionName%",
                ConnectionStringSetting = "CosmosConnectionString",
                LeaseCollectionName = "leases",
                LeaseCollectionPrefix = "%ThermostatLogLeasePrefix%",
                CreateLeaseCollectionIfNotExists = true,
                StartFromBeginning = false)] IReadOnlyList<Document> input, 
            [SignalR(HubName = "readings", ConnectionStringSetting = "AzureSignalRConnectionString")] IAsyncCollector<SignalRMessage> signalRMessages,
            ILogger log)
        {
            foreach (var doc in input)
            {
                var thermostatLog = JsonConvert.DeserializeObject<ThermostatLog>(doc.ToString());
                var min = thermostatLog.Readings.Select(x => x.Temp).Min();
                var avg = thermostatLog.Readings.Select(x => x.Temp).Average();                
                var max = thermostatLog.Readings.Select(x => x.Temp).Max();

                var readingString = $"MinTemp: {min:F2}, AvgTemp: {avg:F2}, MaxTemp: {max:F2}";
                log.LogInformation($"{thermostatLog.DeviceId}, {readingString}");

                var message = new SignalRMessage
                {
                    Target = "newReading",
                    Arguments = new[] { new { sender = thermostatLog.DeviceId.ToString(), text = readingString } }
                };

                await signalRMessages.AddAsync(message);                
            }
        }
    }
}
