using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources
{
    [ExtensionDescription(
        "Provides access to other Nexus databases.",
        "https://github.com/Apollo3zehn/nexus-sources-federation",
        "https://github.com/Apollo3zehn/nexus-sources-federation")]
    public class Federation : IDataSource
    {
        private DataSourceContext _context = default!;
        private Api.NexusClient _nexusClient = default!;
        private string _mountPath = default!;

        private static JsonSerializerOptions _jsonSerializerOptions;

        static Federation()
        {
            _jsonSerializerOptions = new JsonSerializerOptions();
            _jsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
        }

        public Task SetContextAsync(DataSourceContext context, ILogger logger, CancellationToken cancellationToken)
        {
            _context = context;

            // http client
            if (_context.ResourceLocator is null)
                throw new Exception("The resource locator must be set.");

            var httpClient = new HttpClient()
            {
                BaseAddress = _context.ResourceLocator
            };

            // token
            var token = _context.SourceConfiguration.GetStringValue($"access-token");

            if (token is null)
                throw new Exception("The access-token property is not set.");

            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("bearer", token);

            _nexusClient = new Api.NexusClient(httpClient);

            // mount-path
            var mountPath = _context.SourceConfiguration.GetStringValue($"mount-path");

            if (mountPath is null)
                throw new Exception("The mount-path property is not set.");

            _mountPath = mountPath;

            return Task.CompletedTask;
        }

        public async Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
        {
            if (path == "/")
                path = _mountPath + "/";

            var catalogInfos = await _nexusClient.Catalogs.GetChildCatalogInfosAsync(GetOriginalCatalogId(path), cancellationToken);

            return catalogInfos
                .Where(catalogInfo => catalogInfo.Id != _mountPath)
                .Select(catalogInfo => new CatalogRegistration(GetExtendedCatalogId(catalogInfo.Id), catalogInfo.Title, IsTransient: true))
                .ToArray();
        }

        public async Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var resourceCatalog = await _nexusClient.Catalogs.GetAsync(GetOriginalCatalogId(catalogId));
            resourceCatalog = resourceCatalog with { Id = catalogId };

            var jsonString = JsonSerializer.Serialize(resourceCatalog, _jsonSerializerOptions);
            var actualResourceCatalog = JsonSerializer.Deserialize<ResourceCatalog>(jsonString, _jsonSerializerOptions)!;

            return actualResourceCatalog;
        }

        public async Task<double> GetAvailabilityAsync(string catalogId, DateTime begin, DateTime end, CancellationToken cancellationToken)
        {
            var availability = await _nexusClient.Catalogs
                .GetAvailabilityAsync(GetOriginalCatalogId(catalogId), begin, end, (end - begin), cancellationToken);

            return availability.Data[0];
        }

        public async Task<(DateTime Begin, DateTime End)> GetTimeRangeAsync(string catalogId, CancellationToken cancellationToken)
        {
            var timeRange = await _nexusClient.Catalogs
                .GetTimeRangeAsync(GetOriginalCatalogId(catalogId), cancellationToken);

            return (timeRange.Begin, timeRange.End);
            }

        public async Task ReadAsync(DateTime begin, DateTime end, ReadRequest[] requests, ReadDataHandler readData, IProgress<double> progress, CancellationToken cancellationToken)
        {
            foreach (var request in requests)
            {
                var streamResponse = await _nexusClient.Data.GetStreamAsync(GetOriginalCatalogId(request.CatalogItem.ToPath()), begin, end, cancellationToken);
                var stream = await streamResponse.GetStreamAsync();
                var targetBuffer = request.Data;

                while (targetBuffer.Length > 0)
                {
                    var bytesRead = await stream.ReadAsync(targetBuffer);
                    targetBuffer = targetBuffer.Slice(bytesRead);
                }

                request.Status.Span.Fill(1);
            }
        }

        private string GetExtendedCatalogId(string catalogId)
        {
            return _mountPath + catalogId;
        }

        private string GetOriginalCatalogId(string catalogId)
        {
            return catalogId.Substring(_mountPath.Length);
        }
    }
}
