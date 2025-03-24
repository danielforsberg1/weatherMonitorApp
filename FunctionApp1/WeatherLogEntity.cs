using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FunctionApp1
{
    public class WeatherLogEntity : TableEntity
    {
        public WeatherLogEntity() { }

        public WeatherLogEntity(DateTime timestamp)
        {
            PartitionKey = timestamp.ToString("yyyyMM");
            RowKey = timestamp.ToString("yyyyMMddHHmmss");
        }

        public string Status { get; set; }
        public string ErrorMessage { get; set; }
        public string BlobName { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
