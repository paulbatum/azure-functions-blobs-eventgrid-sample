using System;

namespace FuncBlobs.Models
{
    public class ThermostatReading
    {
        public DateTimeOffset Timestamp { get; set; }
        public double Temp { get; set; }
        public string TempScale { get; set; }
        public double Humidity { get; set; }
    }
}
