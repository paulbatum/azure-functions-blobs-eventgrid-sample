﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;
using FuncBlobs.Models;

namespace FuncBlobs.Formats
{
    public class CsvUtils
    {
        private Configuration csvConfiguration;

        public CsvUtils()
        {
            var dateTimeOffsetOptions = new TypeConverterOptions
            {
                Formats = new string[] { "u" }
            };

            csvConfiguration = new Configuration();            
            csvConfiguration.TypeConverterOptionsCache.AddOptions<DateTimeOffset>(dateTimeOffsetOptions);
            csvConfiguration.AutoMap(typeof(ThermostatReading));            
        }

        public void ToCsv(TextWriter writer, IEnumerable<ThermostatReading> readings)
        {
            using (var csvWriter = new CsvWriter(writer, csvConfiguration))
            {
                csvWriter.WriteRecords(readings);
            }
        }

        public List<ThermostatReading> FromCsv(TextReader reader)
        {
            using (var csvReader = new CsvReader(reader, csvConfiguration, leaveOpen: true))
            {
                return csvReader.GetRecords<ThermostatReading>().ToList();
            }
        }
    }
}
