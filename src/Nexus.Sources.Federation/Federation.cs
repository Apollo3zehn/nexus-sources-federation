using System.Net.Http.Headers;
using System.Text.Json;
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
        private string _rootCatalogId = default!;

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
            var token = _context.SourceConfiguration.GetStringValue($"bearer-token");

            if (token is null)
                throw new Exception("The bearer-token property is not set.");

            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("bearer", token);

            _nexusClient = new Api.NexusClient(httpClient);

            // root-catalog-id
            var rootCatalogId = _context.SourceConfiguration.GetStringValue($"root-catalog-id");

            if (rootCatalogId is null)
                throw new Exception("The root-catalog-id property is not set.");

            _rootCatalogId = rootCatalogId;

            return Task.CompletedTask;
        }

        public async Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
        {
            var catalogInfos = await _nexusClient.Catalogs.GetChildCatalogInfosAsync("", cancellationToken);

            return catalogInfos
                .Select(catalogInfo => new CatalogRegistration(GetRealCatalogId(catalogInfo.Id), catalogInfo.Title, IsTransient: true))
                .ToArray();
        }

        public async Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var resourceCatalog = await _nexusClient.Catalogs.GetAsync(GetRealCatalogId(catalogId));
            var jsonString = JsonSerializer.Serialize(resourceCatalog);
            var actualResourceCatalog = JsonSerializer.Deserialize<ResourceCatalog>(jsonString)!;

            return actualResourceCatalog;
        }

        public async Task<double> GetAvailabilityAsync(string catalogId, DateTime begin, DateTime end, CancellationToken cancellationToken)
        {
            var availability = await _nexusClient.Catalogs
                .GetAvailabilityAsync(GetRealCatalogId(catalogId), begin, end, (end - begin), cancellationToken);

            return availability.Data[0];
        }

        public async Task<(DateTime Begin, DateTime End)> GetTimeRangeAsync(string catalogId, CancellationToken cancellationToken)
        {
            var timeRange = await _nexusClient.Catalogs
                .GetTimeRangeAsync(GetRealCatalogId(catalogId), cancellationToken);

            return (timeRange.Begin, timeRange.End);
        }

        public async Task ReadAsync(DateTime begin, DateTime end, ReadRequest[] requests, ReadDataHandler readData, IProgress<double> progress, CancellationToken cancellationToken)
        {
            foreach (var request in requests)
            {
                var streamResponse = await _nexusClient.Data.GetStreamAsync(request.CatalogItem.ToPath(), begin, end, cancellationToken);
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

        private string GetRealCatalogId(string catalogId)
        {
            return _rootCatalogId + catalogId;
        }
    }
}
