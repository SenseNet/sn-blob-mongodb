using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;

namespace MongoDbBlobStorage.Tests
{
    [TestClass]
    public class ProviderTests : TestBase
    {
        public override TestContext TestContext { get; set; }

        [TestMethod]
        public void Provider_Allocate()
        {
            const int versionId = 1111;
            const int propTypeId = 1;
            var token = TestBlobStorage.StartChunk(versionId, propTypeId, 0x7FFFFFFFFFFFFFFF);

            const int binPropId = versionId*10 + propTypeId;
            const int fileId = binPropId*2;
            Assert.IsTrue(token.StartsWith($"{versionId}|{propTypeId}|{binPropId}|{fileId}"));
        }

        [TestMethod]
        public void Provider_WriteChunk()
        {
            var sizeLimitOriginal = SenseNet.Configuration.BlobStorage.MinimumSizeForBlobProviderInBytes;

            try
            {
                new PrivateType(typeof(SenseNet.Configuration.BlobStorage)).SetStaticFieldOrProperty("MinimumSizeForBlobProviderInBytes", 1);

                const int versionId = 1111;
                const int propTypeId = 1;
                const long fullSize = 30;
                var token = TestBlobStorage.StartChunk(versionId, propTypeId, fullSize);

                var data = new[]
                {
                    new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A},
                    new byte[] {0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14},
                    new byte[] {0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E}
                };

                // writing chunks
                for (var i = 0; i < data.Length; i++)
                    BlobStorageClient.WriteChunk(versionId, token, data[i], i * 10, fullSize);

                // reading back
                using (var reader = BlobStorageClient.GetStreamForRead(token))
                {
                    Assert.AreEqual(30L, reader.Length);

                    for (var i = 1; i < 30; i++)
                        Assert.AreEqual(i, reader.ReadByte());
                }
            }
            finally
            {
                new PrivateType(typeof(SenseNet.Configuration.BlobStorage)).SetStaticFieldOrProperty("MinimumSizeForBlobProviderInBytes", sizeLimitOriginal);
            }
        }

    }
}
