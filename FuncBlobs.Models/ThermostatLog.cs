using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace FuncBlobs.Models
{
    public class ThermostatLog
    {
        public string Id { get; set; }
        public Guid DeviceId { get; set; }
        public DateTimeOffset LogTimestamp { get; set; }   
        public List<ThermostatReading> Readings { get; set; }

        public ThermostatLog()
        {
            Readings = new List<ThermostatReading>();
        }

        public static ThermostatLog ParseFileIdentifier(string fileIdentifier)
        {
            var components = fileIdentifier.Split('_');

            return new ThermostatLog
            {
                Id = fileIdentifier,
                DeviceId = Guid.Parse(components[0]),
                LogTimestamp = DateTimeOffset.ParseExact(components[1], "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),                
            };
        }

        public static ThermostatLog GenerateRandomLog(Random random)
        {
            var log = new ThermostatLog
            {
                DeviceId = Guid.NewGuid(),
                LogTimestamp = DateTime.UtcNow,
            };

            for (int i = 0; i < 24; i++)
            {
                log.Readings.Add(new ThermostatReading
                {
                    Timestamp = DateTimeOffset.UtcNow.AddDays(-1).AddHours(i),
                    Temp = random.Next(10, 30) + random.NextDouble(),
                    TempScale = "C",
                    Humidity = random.Next(0, 100) + random.NextDouble()
                });
            }

            return log;
        }

        public string GetFileIdentifier() => $"{DeviceId}_{LogTimestamp:yyyyMMddHHmmss}";
        
    }
}
