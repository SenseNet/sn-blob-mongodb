using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SenseNet.Diagnostics;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{

    public class MongoDbBlobProvider : IBlobProvider
    {
        private static readonly MongoDbConnection Db = new MongoDbConnection();

        public int ChunkSize { get; set; } = Configuration.ChunkSize;

        private static long _readCount;
        public static long ReadCount => Interlocked.Read(ref _readCount);

        private static long _writeCount;
        public static long WriteCount => Interlocked.Read(ref _writeCount);

        private static IMongoCollection<BsonDocument> GetBlobCollection()
        {
            return Db.GetBlobCollection();
        }


        internal virtual byte[] LoadChunk(string fileIdentifier, int chunkIndex, IMongoCollection<BsonDocument> collection)
        {
            using (var op = SnTrace.Database.StartOperation("STATIC MongoDbBlobProvider.LoadChunk: fileIdentifier:{0}, chunkIndex:{1}", fileIdentifier, chunkIndex))
            {
                //var chunkId = GetChunkId(fileIdentifier, chunkIndex);
                var builder = Builders<BsonDocument>.Filter;
                var filter = builder.And(
                    builder.Eq(Field.FileIdentifier, fileIdentifier),
                    builder.Eq(Field.ChunkIndex, chunkIndex)) ;

                var result = collection.Find(filter);

                var list = result.ToList();
                var document = list.FirstOrDefault();
                if (document == null)
                    return null;

                var bytes = document[Field.Blob].AsByteArray;
                Interlocked.Increment(ref _readCount);

                op.Successful = true;
                return bytes;
            }
        }

        internal static void WriteChunk(string fileIdentifier, int chunkIndex, TraceInfo traceInfo, byte[] bytes, IMongoCollection<BsonDocument> collection)
        {
            using (var op = SnTrace.Database.StartOperation("STATIC MongoDbBlobProvider.WriteChunk: fileId:{0}, chunkIndex:{1}", fileIdentifier, chunkIndex))
            {
                var document = CreateBsonDocument(fileIdentifier, chunkIndex, traceInfo, bytes);

                collection.InsertOne(document);
                Interlocked.Increment(ref _writeCount);
                op.Successful = true;
            }
        }

        internal static async Task WriteChunkAsync(string fileIdentifier, int chunkIndex, TraceInfo traceInfo, byte[] bytes, IMongoCollection<BsonDocument> collection)
        {
            using (var op = SnTrace.Database.StartOperation("STATIC MongoDbBlobProvider.WriteChunkAsync: fileIdentifier:{0}, chunkIndex:{1}", fileIdentifier, chunkIndex))
            {
                var document = CreateBsonDocument(fileIdentifier, chunkIndex, traceInfo, bytes);

                await collection.InsertOneAsync(document);
                Interlocked.Increment(ref _writeCount);
                op.Successful = true;
            }
        }

        private static BsonDocument CreateBsonDocument(string fileIdentifier, int chunkIndex, TraceInfo traceInfo, byte[] bytes)
        {
            //var chunkId = GetChunkId(fileIdentifier, chunkIndex);
            var document = new BsonDocument
            {
                //{Field.VersionId, traceInfo.VersionId},
                //{Field.PropertyTypeId, traceInfo.PropertyTypeId},
                //{Field.FileSize, traceInfo.FileSize},

                //{Field.ChunkId, chunkId},
                {Field.ChunkIndex, chunkIndex},
                {Field.FileIdentifier, fileIdentifier},
                {Field.CreatedAt, DateTime.UtcNow},
                {Field.Blob, bytes}
            };
            return document;
        }

        //internal static string GetChunkId(string fileIdentifier, int chunkIndex)
        //{
        //    return $"{fileIdentifier}_{chunkIndex}";
        //}

        //================================================================================= Test support

        /// <summary>DO NOT DELETE. Used in tests.</summary>
        protected static void _Cleanup()
        {
            Db.DropDatabase();
            Db.CreateDatabase();
        }

        /// <summary>DO NOT DELETE. Used in tests.</summary>
        protected byte[] _ReadChunk(string fileIdentifier, int chunkIndex)
        {
            return LoadChunk(fileIdentifier, chunkIndex, GetBlobCollection());
        }

        /// <summary>DO NOT DELETE. Used in tests.</summary>
        protected void _WriteChunk(BlobStorageContext context, string fileIdentifier, int chunkIndex, byte[] bytes)
        {
            WriteChunk(fileIdentifier, chunkIndex, context.GetTraceInfo(), bytes, GetBlobCollection());
        }

        //=================================================================================

        public void Allocate(BlobStorageContext context)
        {
            SnTrace.Database.Write("MongoDbBlobProvider.Allocate: {0}", context.BlobProviderData);
            context.BlobProviderData =
                MongoDbBlobProviderData.Create(ChunkSize, context.Length);
        }

        public Stream CloneStream(BlobStorageContext context, Stream stream)
        {
            if (stream is IMongoDbBlobStorageStream mongoDbStream)
                return mongoDbStream.CloneStream(context, stream);

            throw new NotSupportedException(
                typeof(MongoDbBlobProvider).Name + " cannot clone a stream that is " + stream.GetType().FullName);
        }

        public void Delete(BlobStorageContext context)
        {
            using (var op = SnTrace.Database.StartOperation("MongoDbBlobProvider.Delete: {0}", context.BlobProviderData))
            {
                var providerData = (MongoDbBlobProviderData) context.BlobProviderData;
                var collection = GetBlobCollection();
                var filter = Builders<BsonDocument>.Filter.Eq(Field.FileIdentifier, providerData.FileIdentifier);
                collection.DeleteMany(filter);
                op.Successful = true;
            }
        }

        public Stream GetStreamForRead(BlobStorageContext context)
        {
            SnTrace.Database.Write("MongoDbBlobProvider.GetStreamForRead: {0}", context.BlobProviderData);
            return new ChunkReaderStream((MongoDbBlobProviderData)context.BlobProviderData, GetBlobCollection(), this);
        }

        public Stream GetStreamForWrite(BlobStorageContext context)
        {
            SnTrace.Database.Write("MongoDbBlobProvider.GetStreamForWrite: {0}", context.BlobProviderData);
            return new ChunkWriterStream((MongoDbBlobProviderData)context.BlobProviderData, context.GetTraceInfo(), GetBlobCollection());
        }

        public object ParseData(string providerData)
        {
            SnTrace.Database.Write("MongoDbBlobProvider.ParseData: {0}", providerData);
            return BlobStorageContext.DeserializeBlobProviderData<MongoDbBlobProviderData>(providerData);
        }


        public void Write(BlobStorageContext context, long offset, byte[] buffer)
        {
            using (var op = SnTrace.Database.StartOperation("MongoDbBlobProvider.Write: offset: {0}, buffer length: {1}, {2}", offset, buffer?.Length, context.BlobProviderData))
            {
                var providerData = (MongoDbBlobProviderData) context.BlobProviderData;
                var fileIdentifier = providerData.FileIdentifier;
                var originalChunkSize = providerData.ChunkSize;

                AssertValidOffset(context.Length, originalChunkSize, offset);

                var traceInfo = context.GetTraceInfo();
                var length = buffer.Length;
                var sourceOffset = 0;
                while (length > 0)
                {
                    var chunkIndex = (offset/originalChunkSize).ToInt();
                    var currentChunkLength = Math.Min(originalChunkSize, length);
                    var bytes = new byte[currentChunkLength];
                    Array.ConstrainedCopy(buffer, sourceOffset, bytes, 0, currentChunkLength);

                    AssertValidChunk(context.Length, originalChunkSize, offset, bytes.Length);
                    WriteChunk(fileIdentifier, chunkIndex, traceInfo, bytes, GetBlobCollection());

                    length -= bytes.Length;
                    offset += originalChunkSize;
                    sourceOffset += originalChunkSize;
                }
                op.Successful = true;
            }
        }
        private static void AssertValidOffset(long fileSize, int chunkSize, long offset)
        {
            if (chunkSize < 0)
                throw new ArgumentOutOfRangeException(nameof(chunkSize));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (offset % chunkSize > 0)
                throw new MongoDbBlobProviderException($"Invalid offset: {offset}. Chunk size: {chunkSize}, fileSize: {fileSize}");
            if (offset >= fileSize)
                throw new MongoDbBlobProviderException($"Invalid offset: {offset}. Chunk size: {chunkSize}, fileSize: {fileSize}");
        }
        private static void AssertValidChunk(long fileSize, int chunkSize, long offset, int bufferSize)
        {
            var expectedLastChunkLength = fileSize % chunkSize;
            var isLastChunk = fileSize - expectedLastChunkLength <= offset;

            if (isLastChunk && expectedLastChunkLength != bufferSize)
                throw new MongoDbBlobProviderException($"Invalid last chunk size: {bufferSize}. Expected: {expectedLastChunkLength}. FileSize: {fileSize}, offset: {offset}");
            if (!isLastChunk && bufferSize % chunkSize != 0)
                throw new MongoDbBlobProviderException($"Invalid chunk size: {bufferSize}. Expected: {chunkSize}. FileSize: {fileSize}, offset: {offset}");
        }

        protected void DeleteChunks(string fileIdentifier, int chunkSize, long originalLength, long offset, int length,
            IMongoCollection<BsonDocument> collection)
        {
            using (var op = 
                SnTrace.Database.StartOperation("MongoDbBlobProvider.DeleteChunks: fileIdentifier:{0}, chunkSize:{1}, originalLength:{2}, offset:{3}, length:{4}",
                fileIdentifier, chunkSize, originalLength, offset, length))
            {
                var deleteLength = Math.Min(originalLength, offset + length);
                for (var offs = offset; offs < deleteLength; offs += chunkSize)
                {
                    var chunkIndex = (offset/chunkSize).ToInt();
                    //var filter = Builders<BsonDocument>.Filter.Eq(Field.ChunkId, GetChunkId(fileIdentifier, chunkIndex));
                    var builder = Builders<BsonDocument>.Filter;
                    var filter = builder.And(
                        builder.Eq(Field.FileIdentifier, fileIdentifier),
                        builder.Eq(Field.ChunkIndex, chunkIndex));

                    var result = collection.DeleteOne(filter);
                    if (result.IsAcknowledged)
                        SnTrace.Database.Write("MongoDbBlobProvider.DeleteChunks: DeletedCount:{0}", result.DeletedCount);
                    else
                        SnTrace.Database.Write("MongoDbBlobProvider.DeleteChunks: IsAcknowledged:false");
                }
                op.Successful = true;
            }
        }

        public async Task WriteAsync(BlobStorageContext context, long offset, byte[] buffer)
        {
            using (var op = SnTrace.Database.StartOperation("MongoDbBlobProvider.WriteAsync: offset:{0}, context:{1}.", offset, context))
            {
                var providerData = (MongoDbBlobProviderData) context.BlobProviderData;
                var fileIdentifier = providerData.FileIdentifier;
                var originalChunkSize = providerData.ChunkSize;

                AssertValidOffset(context.Length, originalChunkSize, offset);

                var traceInfo = context.GetTraceInfo();
                var length = buffer.Length;
                var sourceOffset = 0;
                while (length > 0)
                {
                    var chunkIndex = (offset/originalChunkSize).ToInt();
                    var currentChunkLength = Math.Min(originalChunkSize, length);
                    var bytes = new byte[currentChunkLength];
                    Array.ConstrainedCopy(buffer, sourceOffset, bytes, 0, currentChunkLength);

                    AssertValidChunk(context.Length, originalChunkSize, offset, bytes.Length);
                    await WriteChunkAsync(fileIdentifier, chunkIndex, traceInfo, bytes, GetBlobCollection());

                    length -= bytes.Length;
                    offset += originalChunkSize;
                    sourceOffset += originalChunkSize;
                }
                op.Successful = true;
            }
        }

    }
}
