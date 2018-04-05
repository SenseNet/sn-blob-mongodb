using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.IO;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    internal class ChunkReaderStream : Stream, IMongoDbBlobStorageStream
    {
        private readonly string _fileIdentifier;
        private readonly int _chunkSize;
        private readonly IMongoCollection<BsonDocument> _databaseCollection;
        private readonly MongoDbBlobProvider _provider;

        private int _currentChunkIndex;
        private int _loadedChunkIndex = -1;
        private byte[] _loadedBytes;

        public ChunkReaderStream(MongoDbBlobProviderData providerData, IMongoCollection<BsonDocument> databaseCollection, MongoDbBlobProvider provider)
        {
            _fileIdentifier = providerData.FileIdentifier;
            Length = providerData.BlobSize;
            _chunkSize = providerData.ChunkSize;
            _databaseCollection = databaseCollection;
            _provider = provider;
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length { get; }

        /// <summary>DO NOT USE DIRECTLY.</summary>
        private long _position;
        public override long Position
        {
            get { return _position; }
            set
            {
                _currentChunkIndex = (value / _chunkSize).ToInt();
                _position = value;
            }
        }

        public override void Flush()
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (Position >= Length)
                return 0;

            var totalCount = 0;
            while (count > 0)
            {
                var chunkOffset = (long)_chunkSize * _currentChunkIndex;

                if (_currentChunkIndex != _loadedChunkIndex)
                {
                    _loadedBytes = _provider.LoadChunk(_fileIdentifier, _currentChunkIndex, _databaseCollection);
                    if (_loadedBytes == null)
                        throw new MongoDbBlobProviderException($"Chunk not found. FileIdentifier:{_fileIdentifier}, chunkIndex:{_currentChunkIndex}");

                    _loadedChunkIndex = _currentChunkIndex;
                }
                var copiedCount = CopyBytes(_loadedBytes, (Position - chunkOffset).ToInt(), buffer, offset, count);

                Position += copiedCount;
                offset += copiedCount;
                count -= copiedCount;
                totalCount += copiedCount;
                if (Position >= Length)
                    break;
            }
            return totalCount;
        }


        private static int CopyBytes(byte[] source, int sourceOffset, byte[] target, int targetOffset, int expectedCount)
        {
            var availableSourceCount = source.Length - sourceOffset;
            var availableTargetCount = target.Length - targetOffset;
            var availableCount = Math.Min(Math.Min(availableSourceCount, availableTargetCount), expectedCount);

            Array.ConstrainedCopy(source, sourceOffset, target, targetOffset, availableCount);

            return availableCount;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var position = Position;
            switch (origin)
            {
                case SeekOrigin.Begin:
                    position = offset;
                    break;
                case SeekOrigin.Current:
                    position += offset;
                    break;
                case SeekOrigin.End:
                    position = Length - offset;
                    break;
            }
            if (position < 0)
                throw new MongoDbBlobProviderException($"Invalid offset. Expected max:{Length}, requested:{offset}");
            Position = position;
            return position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        //=======================================================================

        public Stream CloneStream(BlobStorageContext context, Stream stream)
        {
            var providerData = (MongoDbBlobProviderData) context.BlobProviderData;
            var dabaseCollection = _databaseCollection;
            var provider = _provider;
            return new ChunkReaderStream(providerData, dabaseCollection, provider);
        }
    }
}
