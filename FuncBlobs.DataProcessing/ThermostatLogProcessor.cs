using System.Collections.Generic;
using System.Linq;
using FuncBlobs.Models;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FuncBlobs.DataProcessing
{
    public static class ThermostatLogProcessor
    {
        [FunctionName("ThermostatLogProcessor")]
        public static void Run(
            [CosmosDBTrigger(
                databaseName: "%ThermostatLogDatabaseName%",
                collectionName: "%ThermostatLogCollectionName%",
                ConnectionStringSetting = "CosmosConnectionString",
                LeaseCollectionName = "leases",
                LeaseCollectionPrefix = "%ThermostatLogLeasePrefix%",
                CreateLeaseCollectionIfNotExists = true,
                StartFromBeginning = false)] IReadOnlyList<Document> input, 
            ILogger log)
        {
            foreach (var doc in input)
            {
                var thermostatLog = JsonConvert.DeserializeObject<ThermostatLog>(doc.ToString());
                var min = thermostatLog.Readings.Select(x => x.Temp).Min();
                var avg = thermostatLog.Readings.Select(x => x.Temp).Average();                
                var max = thermostatLog.Readings.Select(x => x.Temp).Max();

                log.LogInformation($"{thermostatLog.DeviceId}, MinTemp: {min:F2}, AvgTemp: {avg:F2}, MaxTemp: {max:F2}");
            }

            //foreach (var thermostatLog in input)
            //{
            //    log.LogInformation($"{thermostatLog.DeviceId}, {thermostatLog.LogTimestamp}, AvgTemp: {thermostatLog.Readings.Select(x => x.Temp).Average()}");
            //}
        }
    }
}
