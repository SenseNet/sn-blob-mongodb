using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;

namespace MongoDbBlobStorage.Tests
{
    [TestClass]
    public class ChunkReaderStreamTests : TestBase
    {
        public override TestContext TestContext { get; set; }

        private class TestBlobProvider : MongoDbBlobProvider
        {
            private readonly Dictionary<string, byte[][]> _chunks;

            public int CountOfChunksLoaded { get; private set; }

            public TestBlobProvider()
            {
            }

            public TestBlobProvider(Dictionary<string, byte[][]> chunks)
            {
                _chunks = chunks;
            }

            internal override byte[] LoadChunk(string fileIdentifier, int chunkIndex, IMongoCollection<BsonDocument> collection)
            {
                CountOfChunksLoaded++;
                return _chunks[fileIdentifier][chunkIndex];
            }
        }

        private readonly Dictionary<string, byte[][]> _chunksInDb = new Dictionary<string, byte[][]>
        {
            { "file-41", new[] {
                new byte[] { 0xF7, 0xF7, 0xF7, 0xF7, 0xF7, 0xF7, 0xF7, 0xF7, 0xF7, 0xF7 },
                new byte[] { 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8, 0xF8 } }
            },
            { "file-42", new[] {
                new byte[] { 0xF9, 0xF9, 0xF9, 0xF9, 0xF9, 0xF9, 0xF9, 0xF9, 0xF9, 0xF9 },
                new byte[] { 0xFA, 0xFA, 0xFA, 0xFA, 0xFA, 0xFA, 0xFA, 0xFA, 0xFA, 0xFA },
                new byte[] { 0xFB, 0xFB, 0xFB, 0xFB, 0xFB } }
            },
            { "file-43", new[] {
                new byte[] { 0xFC, 0xFC, 0xFC, 0xFC, 0xFC, 0xFC, 0xFC, 0xFC, 0xFC, 0xFC },
                new byte[] { 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD },
                new byte[] { 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE, 0xFE },
                new byte[] { 0xFF, 0xFF, 0xFF } }
            }
        };


        [TestMethod]
        public void ReadStream_ReadByte_LoadChunkCount()
        {
            var data = new MongoDbBlobProviderData { BlobSize = 25L, ChunkSize = 10, FileIdentifier = "file-42" };
            var provider = new TestBlobProvider(_chunksInDb);

            using (var stream = new ChunkReaderStream(data, null, provider))
                while (stream.ReadByte() >= 0) ;

            Assert.AreEqual(3, provider.CountOfChunksLoaded);
        }

        [TestMethod]
        public void ReadStream_Seek_ReadByte()
        {
            var data = new MongoDbBlobProviderData { BlobSize = 25L, ChunkSize = 10, FileIdentifier = "file-42" };
            var provider = new TestBlobProvider(_chunksInDb);
            var result = new int[3];
            using (var stream = new ChunkReaderStream(data, null, provider))
            {
                for (var i = 0; i < result.Length; i++)
                {
                    stream.Seek(i * 10, SeekOrigin.Begin);
                    result[i] = stream.ReadByte();
                }
            }

            var expected = "0x00F9, 0x00FA, 0x00FB";
            var actual = string.Join(", ", result.Select(i => "0x" + i.ToString("X4")));

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void ReadStream_OverSeek_ReadByte()
        {
            var data = new MongoDbBlobProviderData { BlobSize = 25L, ChunkSize = 10, FileIdentifier = "file-42" };
            var provider = new TestBlobProvider(_chunksInDb);
            var result = new int[5];
            using (var stream = new ChunkReaderStream(data, null, provider))
            {
                for (var i = 0; i < result.Length; i++)
                {
                    stream.Seek(i * 10, SeekOrigin.Begin);
                    result[i] = stream.ReadByte();
                }
            }

            var expected = "0x00F9, 0x00FA, 0x00FB, 0xFFFFFFFF, 0xFFFFFFFF";
            var actual = string.Join(", ", result.Select(i => "0x" + i.ToString("X4")));

            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void ReadStream_OverSeek_ReadMore()
        {
            var data = new MongoDbBlobProviderData { BlobSize = 25L, ChunkSize = 10, FileIdentifier = "file-42" };
            var provider = new TestBlobProvider(_chunksInDb);

            byte[] buffer;
            int count;
            using (var stream = new ChunkReaderStream(data, null, provider))
            {
                buffer = new byte[5];
                stream.Seek(17, SeekOrigin.Begin);
                count = stream.Read(buffer, 0, 5);
                Assert.AreEqual(5, count);
                Assert.AreEqual("0x00FA, 0x00FA, 0x00FA, 0x00FB, 0x00FB",
                                 string.Join(", ", buffer.Select(i => "0x" + i.ToString("X4"))));

                buffer = new byte[5];
                stream.Seek(22, SeekOrigin.Begin);
                count = stream.Read(buffer, 0, 5);
                Assert.AreEqual(3, count);
                Assert.AreEqual("0x00FB, 0x00FB, 0x00FB, 0x0000, 0x0000",
                    string.Join(", ", buffer.Select(i => "0x" + i.ToString("X4"))));

                buffer = new byte[5];
                stream.Seek(25, SeekOrigin.Begin);
                count = stream.Read(buffer, 0, 5);
                Assert.AreEqual(0, count);
                Assert.AreEqual("0x0000, 0x0000, 0x0000, 0x0000, 0x0000",
                    string.Join(", ", buffer.Select(i => "0x" + i.ToString("X4"))));

                buffer = new byte[5];
                stream.Seek(42, SeekOrigin.Begin);
                count = stream.Read(buffer, 0, 5);
                Assert.AreEqual(0, count);
                Assert.AreEqual("0x0000, 0x0000, 0x0000, 0x0000, 0x0000",
                    string.Join(", ", buffer.Select(i => "0x" + i.ToString("X4"))));
            }

        }

    }
}
