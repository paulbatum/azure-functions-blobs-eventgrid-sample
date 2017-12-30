using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using FuncBlobs.Formats;
using FuncBlobs.Models;
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
            var jsonPath = WriteJsonFromCsv(csvPath);
            //ParseEventGridPayload();
            Console.ReadLine();
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
            var log = new ThermostatLog
            {
                DeviceId = Guid.NewGuid(),
                LogTimestamp = DateTime.UtcNow,
            };
            
            var random = new Random();

            for(int i = 0; i < 24; i++)
            {
                log.Readings.Add(new ThermostatReading
                {
                    Timestamp = DateTimeOffset.UtcNow.AddDays(-1).AddHours(i),
                    Temp = random.Next(10, 30) + random.NextDouble(),
                    TempScale = "C",
                    Humidity = random.Next(0, 100) + random.NextDouble()
                });
            }

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
