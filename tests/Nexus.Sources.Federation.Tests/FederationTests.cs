using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Nexus.Api;
using Nexus.Extensibility;
using Xunit;

namespace Nexus.Sources.Tests;

public class FederationTests
{
    [Theory]
    [InlineData("/", "/", "/TEST_CATALOG")]
    [InlineData("/src", "/", "/TEST_CATALOG")]
    [InlineData("src", "/", "/TEST_CATALOG")]
    [InlineData("/src/", "/", "/TEST_CATALOG")]
    [InlineData("/src", "/mnt", "/mnt/TEST_CATALOG")]
    [InlineData("/src/", "/mnt", "/mnt/TEST_CATALOG")]
    [InlineData("/src", "/mnt/", "/mnt/TEST_CATALOG")]
    [InlineData("/", "/mnt", "/mnt/TEST_CATALOG")]
    [InlineData("/", "/mnt/", "/mnt/TEST_CATALOG")]
    [InlineData("/", "mnt/", "/mnt/TEST_CATALOG")]
    [InlineData(default, "mnt", "/mnt/TEST_CATALOG")]
    [InlineData("src", "", "/TEST_CATALOG")]
    public async Task ProvidesCatalogRegistrations(string? sourcePath, string mountPoint, string expectedCatalogId)
    {
        // arrange
        var catalogsClient = Mock.Of<ICatalogsClient>();

        Mock.Get(catalogsClient)
            .Setup(catalog => catalog.GetChildCatalogInfosAsync(
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()
            ))
            .ReturnsAsync((string catalogId, CancellationToken cancellationToken) => 
            {
                Assert.True(catalogId.StartsWith('/'));

                var childCatalogId = catalogId == "/"
                    ? catalogId + "TEST_CATALOG"
                    : catalogId + "/" + "TEST_CATALOG";

                var catalog = new CatalogInfo(childCatalogId, default, default, default, default, default, default, default, default, default, default, default!, default, default);

                return new List<CatalogInfo>() { catalog };
            });

        var nexusClient = Mock.Of<INexusClient>();

        Mock.Get(nexusClient)
            .SetupGet(client => client.Catalogs)
            .Returns(catalogsClient);

        var dataSource = new Federation() { CreateNexusClient = _ => nexusClient } as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri("https://example.com"),
            SystemConfiguration: default!,
            SourceConfiguration: new Dictionary<string, JsonElement>()
            {
                ["access-token"] = JsonSerializer.SerializeToElement(""),
                ["source-path"] = JsonSerializer.SerializeToElement(sourcePath),
                ["mount-point"] = JsonSerializer.SerializeToElement(mountPoint),
            },
            RequestConfiguration: default!);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalogRegistrations = await dataSource.GetCatalogRegistrationsAsync("/", CancellationToken.None);

        // assert
        Assert.Equal(expectedCatalogId, catalogRegistrations.First().Path);
    }
}