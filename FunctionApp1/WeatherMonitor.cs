using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;
using FunctionApp1;

namespace WeatherMonitorApp
{
    public class WeatherMonitor
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string ApiKey = Environment.GetEnvironmentVariable("WeatherApiKey");
        private static readonly string StorageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        private static readonly string TableName = "WeatherLogs";
        private static readonly string ContainerName = "weather-payloads";

        [FunctionName("FetchWeatherData")]
        public static async Task FetchWeatherData([TimerTrigger("0 */1 * * * *")] TimerInfo timerInfo, ILogger log)
        {
            log.LogInformation($"Weather data fetch function executed at: {DateTime.Now}");

            var timestamp = DateTime.UtcNow;
            var weatherLogEntity = new WeatherLogEntity(timestamp)
            {
                Timestamp = timestamp
            };

            try
            {
                var (table, container) = await InitializeStorageAsync();

                var content = await FetchWeatherDataAsync();
                var blobName = await SavePayloadToBlobAsync(container, content, timestamp);

                weatherLogEntity.Status = "Success";
                weatherLogEntity.BlobName = blobName;
            }
            catch (Exception ex)
            {
                log.LogError($"Error fetching weather data: {ex.Message}");
                weatherLogEntity.Status = "Failure";
                weatherLogEntity.ErrorMessage = ex.Message;
            }

            await SaveLogEntryAsync(weatherLogEntity, log);
        }

        [FunctionName("GetWeatherLogs")]
        public static async Task<IActionResult> GetWeatherLogs([HttpTrigger(AuthorizationLevel.Function, "get", Route = "logs")] HttpRequest req, ILogger log)
        {
            log.LogInformation("GetWeatherLogs function processed a request");

            if (!TryParseDateRange(req, out DateTime fromDate, out DateTime toDate, out IActionResult errorResult))
            {
                return errorResult;
            }

            try
            {
                var logs = await RetrieveLogsAsync(fromDate, toDate);
                return new OkObjectResult(logs.OrderByDescending(e => e.Timestamp).ToList());
            }
            catch (Exception ex)
            {
                log.LogError($"Error retrieving logs: {ex.Message}");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }

        [FunctionName("GetWeatherPayload")]
        public static async Task<IActionResult> GetWeatherPayload([HttpTrigger(AuthorizationLevel.Function, "get", Route = "payload/{logId}")] HttpRequest req, string logId, ILogger log)
        {
            log.LogInformation($"GetWeatherPayload function processed a request for log ID: {logId}");

            if (string.IsNullOrEmpty(logId) || logId.Length != 14)
            {
                return new BadRequestObjectResult("Invalid log ID format");
            }

            try
            {
                var logEntity = await RetrieveLogEntryAsync(logId);
                if (logEntity == null)
                {
                    return new NotFoundObjectResult($"Log entry with ID {logId} not found");
                }

                if (string.IsNullOrEmpty(logEntity.BlobName))
                {
                    return new NotFoundObjectResult("No payload available for this log entry");
                }

                var blobContent = await RetrieveBlobContentAsync(logEntity.BlobName);
                var weatherData = JsonConvert.DeserializeObject(blobContent);

                return new OkObjectResult(weatherData);
            }
            catch (Exception ex)
            {
                log.LogError($"Error retrieving payload: {ex.Message}");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }

        private static async Task<(CloudTable, CloudBlobContainer)> InitializeStorageAsync()
        {
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(TableName);
            await table.CreateIfNotExistsAsync();

            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(ContainerName);
            await container.CreateIfNotExistsAsync();

            return (table, container);
        }

        private static async Task<string> FetchWeatherDataAsync()
        {
            var response = await httpClient.GetAsync($"https://api.openweathermap.org/data/2.5/weather?q=London&appid={ApiKey}");
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync();
        }

        private static async Task<string> SavePayloadToBlobAsync(CloudBlobContainer container, string content, DateTime timestamp)
        {
            var blobName = $"{timestamp:yyyyMMddHHmmss}.json";
            var blockBlob = container.GetBlockBlobReference(blobName);
            await blockBlob.UploadTextAsync(content);
            return blobName;
        }

        private static async Task SaveLogEntryAsync(WeatherLogEntity weatherLogEntity, ILogger log)
        {
            try
            {
                var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
                var tableClient = storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                var insertOperation = TableOperation.Insert(weatherLogEntity);
                await table.ExecuteAsync(insertOperation);

                log.LogInformation("Log entry saved successfully");
            }
            catch (Exception ex)
            {
                log.LogError($"Error saving log entry: {ex.Message}");
            }
        }

        private static bool TryParseDateRange(HttpRequest req, out DateTime fromDate, out DateTime toDate, out IActionResult errorResult)
        {
            fromDate = default;
            toDate = default;
            errorResult = null;

            string fromDateParam = req.Query["from"];
            string toDateParam = req.Query["to"];

            if (string.IsNullOrEmpty(fromDateParam) || string.IsNullOrEmpty(toDateParam))
            {
                errorResult = new BadRequestObjectResult("Please provide 'from' and 'to' date parameters in ISO format (yyyy-MM-ddTHH:mm:ss)");
                return false;
            }

            if (!DateTime.TryParse(fromDateParam, out fromDate) || !DateTime.TryParse(toDateParam, out toDate))
            {
                errorResult = new BadRequestObjectResult("Invalid date format. Please use ISO format (yyyy-MM-ddTHH:mm:ss)");
                return false;
            }

            return true;
        }

        private static async Task<List<WeatherLogEntity>> RetrieveLogsAsync(DateTime fromDate, DateTime toDate)
        {
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(TableName);

            var fromDateString = fromDate.ToString("yyyyMMddHHmmss");
            var toDateString = toDate.ToString("yyyyMMddHHmmss");

            var results = new List<WeatherLogEntity>();
            var startMonth = new DateTime(fromDate.Year, fromDate.Month, 1);
            var endMonth = new DateTime(toDate.Year, toDate.Month, 1);

            for (var month = startMonth; month <= endMonth; month = month.AddMonths(1))
            {
                var partitionKey = month.ToString("yyyyMM");

                var query = new TableQuery<WeatherLogEntity>()
                    .Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                            TableOperators.And,
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, fromDateString),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, toDateString)
                            )
                        )
                    );

                var segmentedResult = await table.ExecuteQuerySegmentedAsync(query, null);
                results.AddRange(segmentedResult.Results);
            }

            return results;
        }

        private static async Task<WeatherLogEntity> RetrieveLogEntryAsync(string logId)
        {
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var tableClient = storageAccount.CreateCloudTableClient();
            var table = tableClient.GetTableReference(TableName);

            string yearMonth = logId.Substring(0, 6);
            var retrieveOperation = TableOperation.Retrieve<WeatherLogEntity>(yearMonth, logId);
            var result = await table.ExecuteAsync(retrieveOperation);

            return result.Result as WeatherLogEntity;
        }

        private static async Task<string> RetrieveBlobContentAsync(string blobName)
        {
            var storageAccount = CloudStorageAccount.Parse(StorageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(ContainerName);
            var blockBlob = container.GetBlockBlobReference(blobName);

            if (!await blockBlob.ExistsAsync())
            {
                throw new FileNotFoundException($"Payload blob {blobName} not found");
            }

            return await blockBlob.DownloadTextAsync();
        }
    }
}