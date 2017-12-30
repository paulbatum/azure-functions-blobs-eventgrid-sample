using System;
using System.Collections.Generic;
using System.Text;
using FuncBlobs.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace FuncBlobs.Formats
{
    public class ThermostatLogFormatter
    {
        private JsonSerializerSettings settings;

        public ThermostatLogFormatter()
        {
            settings = new JsonSerializerSettings()
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                DateFormatHandling = DateFormatHandling.IsoDateFormat,
                DateTimeZoneHandling = DateTimeZoneHandling.Utc
            };
        }

        public string ToJson(ThermostatLog log)
        {
            return JsonConvert.SerializeObject(log, settings);
        }
    }
}
