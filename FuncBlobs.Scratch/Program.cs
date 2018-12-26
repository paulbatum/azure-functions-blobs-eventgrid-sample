using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FuncBlobs.Formats;
using FuncBlobs.Models;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace FuncBlobs.Scratch
{
    class Program
    {
        static void Main(string[] args)
        {
            var csvPath = WriteRandomCsv();
            //var jsonPath = WriteJsonFromCsv(csvPath);
            //ParseEventGridPayload();

            //ProcessEventHub().Wait();
            Console.WriteLine("Done.");
            Console.ReadLine();
        }

        public static async Task ProcessEventHub()
        {
            var localSettingsPath = @"D:\code\azure-functions-blobs-eventgrid-sample\FuncBlobs.EventWriter\local.settings.json";
            dynamic settings = JObject.Parse(File.ReadAllText(localSettingsPath));

            string hubName = settings.Values.HubName;
            string eventHubConnectionString = settings.Values.EventsConnectionString;
            string storageConnectionString = settings.Values.AzureWebJobsStorage;

            var host = new EventProcessorHost(
                hubName,
                PartitionReceiver.DefaultConsumerGroupName,
                eventHubConnectionString,
                storageConnectionString,
                "scratchcontainer");

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            Console.WriteLine("Registering processor...");
            await host.RegisterEventProcessorAsync<ScratchProcessor>();
            stopwatch.Stop();

            Console.WriteLine($"Registered. Took {stopwatch.ElapsedMilliseconds}ms.");
        }

        private class ScratchProcessor : IEventProcessor
        {
            public Task CloseAsync(PartitionContext context, CloseReason reason)
            {                
                return Task.CompletedTask;
            }

            public Task OpenAsync(PartitionContext context)
            {
                return Task.CompletedTask;
            }

            public Task ProcessErrorAsync(PartitionContext context, Exception error)
            {
                throw error;
            }

            public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
            {
                foreach(var m in messages)
                {
                    Console.WriteLine(Encoding.UTF8.GetString(m.Body.Array));
                }

                return Task.CompletedTask;
            }
        }

        public static void ParseEventGridPayload()
        {
            var eventGridFile = Path.Combine(Directory.GetCurrentDirectory(), "temp", "eventgrid.json");
            var json = File.ReadAllText(eventGridFile);
            var eventArray = JToken.Parse(json);
            foreach(dynamic e in eventArray)
            {
                Console.WriteLine($"Event type: {e.eventType}");
                Console.WriteLine($"Uri: {e.data.url}");
            }
        }

        public static string WriteRandomCsv()
        {
            var random = new Random();
            var log = ThermostatLog.GenerateRandomLog(random);            

            var path = Path.Combine(Directory.GetCurrentDirectory(), "temp", log.GetFileIdentifier() + ".csv");            
            var utils = new CsvUtils();

            using (var stream = File.OpenWrite(path))
            using (var writer = new StreamWriter(stream))
            {
                utils.ToCsv(writer, log.Readings);
            }

            Console.WriteLine($"Wrote: {path}");
            return path;
        }

        public static string WriteJsonFromCsv(string path)
        {
            var utils = new CsvUtils();

            using (var csvReadStream = File.OpenRead(path))
            using (var reader = new StreamReader(csvReadStream))                   
            {
                List<ThermostatReading> records = utils.FromCsv(reader);
                var components = Path.GetFileNameWithoutExtension(path).Split('_');


                var log = ThermostatLog.ParseFileIdentifier(Path.GetFileNameWithoutExtension(path));
                log.Readings = records;                

                string json = JsonConvert.SerializeObject(log, Formatting.Indented, new JsonSerializerSettings()
                {
                    ContractResolver = new CamelCasePropertyNamesContractResolver(),
                    DateFormatHandling = DateFormatHandling.IsoDateFormat,
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc
                });

                var outputPath = Path.ChangeExtension(path, "json");
                File.WriteAllText(outputPath, json);
                Console.WriteLine($"Wrote: {outputPath}");
                return outputPath;
            }
        }                
    }
}
