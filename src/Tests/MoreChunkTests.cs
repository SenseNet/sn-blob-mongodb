using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;

namespace MongoDbBlobStorage.Tests
{
    [TestClass]
    public class MoreChunkTests : TestBase
    {
        public override TestContext TestContext { get; set; }

        private const int ChunkSize = 10;
        private static volatile int _fileId;

        [TestMethod]
        public void ReadAll()
        {
            ReadAll_TheTest(++_fileId, ChunkSize, 9);
            ReadAll_TheTest(++_fileId, ChunkSize, 10);
            ReadAll_TheTest(++_fileId, ChunkSize, 11);
            ReadAll_TheTest(++_fileId, ChunkSize, 29);
            ReadAll_TheTest(++_fileId, ChunkSize, 30);
            ReadAll_TheTest(++_fileId, ChunkSize, 42);
        }
        private static void ReadAll_TheTest(int fileId, int chunkSize, int length)
        {
            var input = GetTestBytes(length);
            var providerData = AddToProviderViaChunks(fileId, input, chunkSize);
            var stream = GetStreamFromProvider(fileId, providerData);

            var output = new byte[stream.Length];
            stream.Read(output, 0, output.Length);

            Assert.AreEqual(BitConverter.ToString(input), BitConverter.ToString(output));
        }

        /*-------------------------------------------------------------------------------------------------*/

        [TestMethod]
        public void Read()
        {
            Read_TheTest(++_fileId, ChunkSize, 5, 0, 7);
            Read_TheTest(++_fileId, ChunkSize, 10, 0, 7);
            Read_TheTest(++_fileId, ChunkSize, 10, 3, 7);
            Read_TheTest(++_fileId, ChunkSize, 42, 2, 7);
            Read_TheTest(++_fileId, ChunkSize, 42, 12, 7);
            Read_TheTest(++_fileId, ChunkSize, 42, 0, 14);
            Read_TheTest(++_fileId, ChunkSize, 42, 5, 14);
            Read_TheTest(++_fileId, ChunkSize, 42, 0, 24);
            Read_TheTest(++_fileId, ChunkSize, 42, 5, 24);
            Read_TheTest(++_fileId, ChunkSize, 42, 15, 24);
            Read_TheTest(++_fileId, ChunkSize, 42, 35, 24);
        }
        private static void Read_TheTest(int fileId, int chunkSize, int dataLength, long readOffset, int readCount)
        {
            var input = GetTestBytes(dataLength);
            var providerData = AddToProviderViaChunks(fileId, input, chunkSize);
            Assert.IsNotNull(providerData);

            var provider = new MongoDbBlobProvider {ChunkSize = chunkSize};
            var context = new BlobStorageContext(provider, BlobStorageContext.SerializeBlobProviderData(providerData))
            {
                FileId = fileId,
                Length = input.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };

            var stream = provider.GetStreamForRead(context);
            stream.Seek(readOffset, SeekOrigin.Begin);
            var buffer = new byte[readCount];
            var readedByteCount = stream.Read(buffer, 0, readCount);

            var realLength = Math.Min(readCount, dataLength - readOffset).ToInt();
            Assert.AreEqual(realLength, readedByteCount);

            var output = new byte[realLength];
            Array.ConstrainedCopy(buffer, 0, output, 0, realLength);

            var expected = BitConverter.ToString(input, readOffset.ToInt(), realLength);
            Assert.AreEqual(expected, BitConverter.ToString(output));
        }

        /*-------------------------------------------------------------------------------------------------*/

        [TestMethod]
        public void Write()
        {
            Write_TheTest(++_fileId, ChunkSize, 0);
            Write_TheTest(++_fileId, ChunkSize, 5);
            Write_TheTest(++_fileId, ChunkSize, 8);
            Write_TheTest(++_fileId, ChunkSize, 10);
            Write_TheTest(++_fileId, ChunkSize, 12);
            Write_TheTest(++_fileId, ChunkSize, 30);
            Write_TheTest(++_fileId, ChunkSize, 42);
        }
        private static void Write_TheTest(int fileId, int chunkSize, int dataLength)
        {
            var input = GetTestBytes(dataLength);
            var provider = new MongoDbBlobProvider {ChunkSize = chunkSize};
            var context = new BlobStorageContext(provider)
            {
                FileId = fileId,
                Length = input.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };

            provider.Allocate(context);

            using (var streamToWrite = provider.GetStreamForWrite(context))
            {
                using (var streamToRead = new MemoryStream(input))
                {
                    streamToRead.CopyTo(streamToWrite);
                }
            }

            var bytesLoaded = new byte[dataLength];

            using (var stream = provider.GetStreamForRead(context))
            {
                var bytesRead = stream.Read(bytesLoaded, 0, dataLength);
                Assert.AreEqual(dataLength, bytesRead);
            }

            var expected = BitConverter.ToString(input, 0);
            var actual = BitConverter.ToString(bytesLoaded, 0);
            Assert.AreEqual(expected, actual);
        }

        /*-------------------------------------------------------------------------------------------------*/

        [TestMethod]
        public void Delete()
        {
            Delete_TheTest(++_fileId, ChunkSize, 9);
            Delete_TheTest(++_fileId, ChunkSize, 10);
            Delete_TheTest(++_fileId, ChunkSize, 11);
            Delete_TheTest(++_fileId, ChunkSize, 29);
            Delete_TheTest(++_fileId, ChunkSize, 30);
            Delete_TheTest(++_fileId, ChunkSize, 42);
        }
        private static void Delete_TheTest(int fileId, int chunkSize, int length)
        {
            var input = GetTestBytes(length);
            var providerData = AddToProviderViaChunks(fileId, input, chunkSize);

            var provider = new MongoDbBlobProviderAccessor { ChunkSize = chunkSize };
            var context = new BlobStorageContext(provider, BlobStorageContext.SerializeBlobProviderData(providerData))
            {
                FileId = fileId,
                Length = input.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };

            provider.Delete(context);

            var chunkCount = providerData.BlobSize / providerData.ChunkSize + 1;
            for (var i = 0; i < chunkCount; i++)
                Assert.IsNull(provider.ReadChunk($"{fileId}-asdf", i));
        }

        /*-------------------------------------------------------------------------------------------------*/

        [TestMethod]
        public void ReadByte()
        {
            var fileId = ++_fileId;
            var input = GetTestBytes(42);
            var providerData = AddToProviderViaChunks(fileId, input, ChunkSize);
            var provider = new MongoDbBlobProvider { ChunkSize = ChunkSize };
            var context = new BlobStorageContext(provider, BlobStorageContext.SerializeBlobProviderData(providerData))
            {
                FileId = fileId,
                Length = input.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };

            var readsAtStart = MongoDbBlobProvider.ReadCount;
            using (var stream = provider.GetStreamForRead(context))
                while (stream.ReadByte() >= 0) { }

            Assert.AreEqual(5, MongoDbBlobProvider.ReadCount - readsAtStart);
        }


        [TestMethod]
        public void WriteByte()
        {
            var fileId = ++_fileId;
            var input = GetTestBytes(42);
            var provider = new MongoDbBlobProvider { ChunkSize = ChunkSize };
            var context = new BlobStorageContext(provider)
            {
                FileId = fileId,
                Length = input.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };

            provider.Allocate(context);

            var writesAtStart = MongoDbBlobProvider.WriteCount;
            using (var streamToWrite = provider.GetStreamForWrite(context))
            {
                foreach (var @byte in input)
                    streamToWrite.WriteByte(@byte);
            }

            Assert.AreEqual(5, MongoDbBlobProvider.WriteCount - writesAtStart);
        }

        /*-------------------------------------------------------------------------------------------------*/

        [TestMethod]
        public void Write_Random()
        {
            // create initial file content
            var fileId = ++_fileId;
            var dataLength = 64;
            var originalFile = GetTestBytes(dataLength);
            //var providerData = AddToProviderViaStream(fileId, originalFile, _chunkSize);

            var provider = new MongoDbBlobProvider { ChunkSize = ChunkSize };

            // modify the file content and reallocate it
            var modifiedFile = originalFile.Reverse().ToList().Concat(new byte[] { 0xFD, 0xFE, 0xFF }).ToArray();
            dataLength = modifiedFile.Length;
            var context = new BlobStorageContext(provider)
            {
                FileId = fileId,
                Length = modifiedFile.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };
            provider.Allocate(context);

            // slice the modified file content into pieces that have valid size
            const int clientChunkSize = ChunkSize * 2;
            var chunks = new List<Tuple<long, byte[]>>();
            for (long offset = 0; offset < modifiedFile.Length; offset += clientChunkSize)
            {
                var currentChunkSize = Math.Min(clientChunkSize, modifiedFile.LongLength - offset);
                var bytes = new byte[currentChunkSize];
                Array.ConstrainedCopy(modifiedFile, offset.ToInt(), bytes, 0, bytes.Length);
                chunks.Add(new Tuple<long, byte[]>(offset, bytes));
            }

            // write chunks in reverse order that simulates the random write
            for (var i = chunks.Count - 1; i >= 0; i--)
            {
                var chunk = chunks[i];
                var offset = chunk.Item1;
                var bytes = chunk.Item2;
                provider.Write(context, offset, bytes);
            }

            // read back the whole file
            byte[] bytesLoaded;
            using (var stream = provider.GetStreamForRead(context))
            {
                bytesLoaded = new byte[stream.Length];
                stream.Read(bytesLoaded, 0, dataLength);
            }

            // check
            var expected = BitConverter.ToString(modifiedFile, 0);
            var actual = BitConverter.ToString(bytesLoaded, 0);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        public void Write_Random_OffsetErrors()
        {
            Write_Random_OffsetErrors_TheTest(23, 10, 1, "Invalid offset: 1. Chunk size: 10, fileSize: 23");
            Write_Random_OffsetErrors_TheTest(23, 10, 9, "Invalid offset: 9. Chunk size: 10, fileSize: 23");
            Write_Random_OffsetErrors_TheTest(23, 10, 11, "Invalid offset: 11. Chunk size: 10, fileSize: 23");
            Write_Random_OffsetErrors_TheTest(23, 10, 19, "Invalid offset: 19. Chunk size: 10, fileSize: 23");
            Write_Random_OffsetErrors_TheTest(23, 10, 21, "Invalid offset: 21. Chunk size: 10, fileSize: 23");
            Write_Random_OffsetErrors_TheTest(23, 10, 30, "Invalid offset: 30. Chunk size: 10, fileSize: 23");

            Write_Random_OffsetErrors_TheTest(23, 9, 0, "Invalid chunk size: 9. Expected: 10. FileSize: 23, offset: 0");
            Write_Random_OffsetErrors_TheTest(23, 9, 10, "Invalid chunk size: 9. Expected: 10. FileSize: 23, offset: 10");
            Write_Random_OffsetErrors_TheTest(23, 11, 0, "Invalid chunk size: 1. Expected: 10. FileSize: 23, offset: 10");
            Write_Random_OffsetErrors_TheTest(23, 11, 10, "Invalid last chunk size: 1. Expected: 3. FileSize: 23, offset: 20");

            Write_Random_OffsetErrors_TheTest(23, 9, 20, "Invalid last chunk size: 9. Expected: 3. FileSize: 23, offset: 20");
            Write_Random_OffsetErrors_TheTest(23, 2, 20, "Invalid last chunk size: 2. Expected: 3. FileSize: 23, offset: 20");
        }
        private void Write_Random_OffsetErrors_TheTest(long fileLength, int chunkLength, long offset, string expectedMessagePart)
        {
            var provider = new MongoDbBlobProvider { ChunkSize = ChunkSize };

            var context = new BlobStorageContext(provider)
            {
                FileId = ++_fileId,
                Length = fileLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };
            provider.Allocate(context);

            try
            {
                provider.Write(context, offset, GetTestBytes(chunkLength));
                Assert.Fail($"Offset:{offset}, length:{chunkLength}: Exception was not thrown.");
            }
            catch (MongoDbBlobProviderException ex)
            {
                Assert.IsTrue(ex.Message.ToLowerInvariant().Contains(expectedMessagePart.ToLowerInvariant()),
                    $"Offset:{offset}, length:{chunkLength}: Message does not contain '{expectedMessagePart}'. Message: " + ex.Message);
                System.Diagnostics.Trace.WriteLine(ex.Message);
            }
            catch (Exception e)
            {
                Assert.Fail($"Offset:{offset}, length:{chunkLength}. Expected exception is MongoDbBlobProviderException was not thrown #1. Current is {e.GetType().FullName}, message: '{e.Message}'");
            }
        }

        [TestMethod]
        public void BUG_Write_Random_LastUploadChunkBiggerThanProviderChunk()
        {
            // prerequisit: last upload chunk be bigger than provider's chunk.
            //   file length: 74, upload chunk: 20, provider chunk: 10, last uploaded: 14.
            // create initial file content
            var fileId = ++_fileId;
            var dataLength = 74;
            var originalFile = GetTestBytes(dataLength);
            //var providerData = AddToProviderViaStream(fileId, originalFile, _chunkSize);

            var provider = new MongoDbBlobProvider { ChunkSize = ChunkSize };

            // modify the file content and reallocate it
            var modifiedFile = originalFile.Reverse().ToList().Concat(new byte[] { 0xFD, 0xFE, 0xFF }).ToArray();
            dataLength = modifiedFile.Length;
            var context = new BlobStorageContext(provider)
            {
                FileId = fileId,
                Length = modifiedFile.LongLength,
                VersionId = 9999,
                PropertyTypeId = 1
            };
            provider.Allocate(context);

            // slice the modified file content into pieces that have valid size
            const int clientChunkSize = ChunkSize * 2;
            var chunks = new List<Tuple<long, byte[]>>();
            for (long offset = 0; offset < modifiedFile.Length; offset += clientChunkSize)
            {
                var currentChunkSize = Math.Min(clientChunkSize, modifiedFile.LongLength - offset);
                var bytes = new byte[currentChunkSize];
                Array.ConstrainedCopy(modifiedFile, offset.ToInt(), bytes, 0, bytes.Length);
                chunks.Add(new Tuple<long, byte[]>(offset, bytes));
            }

            // write chunks in reverse order that simulates the random write
            for (var i = chunks.Count - 1; i >= 0; i--)
            {
                var chunk = chunks[i];
                var offset = chunk.Item1;
                var bytes = chunk.Item2;
                provider.Write(context, offset, bytes);
            }

            // read back the whole file
            byte[] bytesLoaded;
            using (var stream = provider.GetStreamForRead(context))
            {
                bytesLoaded = new byte[stream.Length];
                stream.Read(bytesLoaded, 0, dataLength);
            }

            // check
            var expected = BitConverter.ToString(modifiedFile, 0);
            var actual = BitConverter.ToString(bytesLoaded, 0);
            Assert.AreEqual(expected, actual);
        }





        /*=================================================================================================*/

        private static MongoDbBlobProviderData AddToProviderViaChunks(int fileId, byte[] bytes, int chunkSize)
        {
            var provider = new MongoDbBlobProviderAccessor { ChunkSize = chunkSize };
            var context = new BlobStorageContext(provider) { VersionId = 42, PropertyTypeId = 1, FileId = fileId, Length = bytes.LongLength };

            provider.Allocate(context);
            var chunkIndex = 0;
            for (var chunkStart = 0; chunkStart < bytes.Length; chunkStart += chunkSize)
            {
                var buffer = new byte[Math.Min(chunkSize, bytes.Length - chunkStart)];
                Array.ConstrainedCopy(bytes, chunkStart, buffer, 0, buffer.Length);
                var fileIdentifier = ((MongoDbBlobProviderData) context.BlobProviderData).FileIdentifier;
                provider.WriteChunk(context, fileIdentifier, chunkIndex++, buffer);
            }

            return (MongoDbBlobProviderData)context.BlobProviderData;
        }

        private static Stream GetStreamFromProvider(int fileId, MongoDbBlobProviderData providerData)
        {
            var provider = new MongoDbBlobProvider { ChunkSize = ChunkSize };
            var context = new BlobStorageContext(provider, BlobStorageContext.SerializeBlobProviderData(providerData))
            {
                FileId = fileId,
                Length = providerData.BlobSize
            };

            return provider.GetStreamForRead(context);
        }


        private static byte[] GetTestBytes(int length)
        {
            return Enumerable.Range(0, length).Select(Convert.ToByte).ToArray();
        }

    }
}
