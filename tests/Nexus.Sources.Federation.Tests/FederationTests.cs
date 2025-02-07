using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Nexus.Api;
using Nexus.Api.V1;
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
    [InlineData("src", default, "/TEST_CATALOG")]
    public async Task ProvidesCatalogRegistrations(string? sourcePath, string? mountPoint, string expectedCatalogId)
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
                Assert.True(catalogId == "/" || !catalogId.EndsWith('/'));

                var childCatalogId = catalogId == "/"
                    ? catalogId + "TEST_CATALOG"
                    : catalogId + "/" + "TEST_CATALOG";

                var catalog = new CatalogInfo(childCatalogId, default, default, default, default, default, default, default, default, default, default!, default!);

                return new List<CatalogInfo>() { catalog };
            });

        var nexusClient = Mock.Of<INexusClient>();

        Mock.Get(nexusClient)
            .SetupGet(client => client.V1.Catalogs)
            .Returns(catalogsClient);

        var dataSource = (IDataSource<FederationSettings>)new Federation() { CreateNexusClient = _ => nexusClient };

        var context = new DataSourceContext<FederationSettings>(
            ResourceLocator: new Uri("https://example.com"),
            SourceConfiguration: new FederationSettings(
                AccessToken: "",
                SourcePath: sourcePath,
                MountPoint: mountPoint,
                IncludePattern: default
            ),
            RequestConfiguration: default!
        );

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalogRegistrations = await dataSource.GetCatalogRegistrationsAsync("/", CancellationToken.None);

        // assert
        Assert.Equal(expectedCatalogId, catalogRegistrations.First().Path);
    }

    [Fact]
    public async Task ProvidesNestedCatalogRegistrations()
    {
        var expectedCatalogId = "/mnt/name";
        var searchPath = "/mnt/"; /* with trailing slash because Nexus does the same! */

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
                Assert.True(!catalogId.EndsWith('/'));

                var childCatalogId = expectedCatalogId;
                var catalog = new CatalogInfo(childCatalogId, default, default, default, default, default, default, default, default, default, default!, default!);

                return new List<CatalogInfo>() { catalog };
            });

        var nexusClient = Mock.Of<INexusClient>();

        Mock.Get(nexusClient)
            .SetupGet(client => client.V1.Catalogs)
            .Returns(catalogsClient);

        var dataSource = (IDataSource<FederationSettings>)new Federation() { CreateNexusClient = _ => nexusClient };

        var context = new DataSourceContext<FederationSettings>(
            ResourceLocator: new Uri("https://example.com"),
            SourceConfiguration: new FederationSettings(AccessToken: "", default, default, default),
            RequestConfiguration: default!
        );

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalogRegistrations = await dataSource.GetCatalogRegistrationsAsync(searchPath, CancellationToken.None);

        // assert
        Assert.Equal(expectedCatalogId, catalogRegistrations.First().Path);
    }
}