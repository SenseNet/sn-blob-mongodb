using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;

namespace MongoDbBlobStorage.Tests
{
    [TestClass]
    public class WithoutDbOperationTests : TestBase
    {
        public override TestContext TestContext { get; set; }

        [TestMethod]
        public void ParseData()
        {
            var serialized = BlobStorageContext.SerializeBlobProviderData(
                new MongoDbBlobProviderData { FileIdentifier = "42-asdf", BlobSize = 987654321L, ChunkSize = 123456 });

            var parsed = new MongoDbBlobProvider().ParseData(serialized);
            Assert.IsNotNull(parsed);
            var data = parsed as MongoDbBlobProviderData;
            Assert.IsNotNull(data);

            Assert.AreEqual("42-asdf", data.FileIdentifier);
            Assert.AreEqual(987654321L, data.BlobSize);
            Assert.AreEqual(123456, data.ChunkSize);
        }
    }
}
