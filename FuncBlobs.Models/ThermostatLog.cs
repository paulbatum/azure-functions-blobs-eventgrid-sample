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
                LogTimestamp = DateTimeOffset.ParseExact(components[0], "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                DeviceId = Guid.Parse(components[1])
            };
        }

        public string GetFileIdentifier() => $"{LogTimestamp:yyyyMMddHHmmss}_{DeviceId}";
        
    }
}
