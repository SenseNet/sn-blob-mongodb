using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.IO;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    internal class ChunkWriterStream : Stream, IMongoDbBlobStorageStream
    {
        private readonly string _fileIdentifier;
        private readonly int _chunkSize;
        private readonly TraceInfo _traceInfo;

        private int _currentChunkIndex;
        private int _currentChunkPosition;
        private byte[] _buffer;
        private bool _flushIsNecessary;

        private readonly IMongoCollection<BsonDocument> _databaseCollection;

        public ChunkWriterStream(MongoDbBlobProviderData providerData, TraceInfo traceInfo, IMongoCollection<BsonDocument> databaseCollection)
        {
            _fileIdentifier = providerData.FileIdentifier;
            Length = providerData.BlobSize;
            _chunkSize = providerData.ChunkSize;
            _databaseCollection = databaseCollection;

            _traceInfo = traceInfo;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length { get; }

        /// <summary>DO NOT USE DIRECTLY.</summary>
        private long _position;
        public override long Position
        {
            get { return _position; }
            set
            {
                // set Position value only through the private SetPosition method
                throw new NotSupportedException();
            }
        }
        private void SetPosition(long position)
        {
            _currentChunkIndex = (position / _chunkSize).ToInt();
            _currentChunkPosition = (position % _chunkSize).ToInt();
            _position = position;
        }

        public override void Flush()
        {
            // nothing to write to the db
            if (!_flushIsNecessary)
                return;
            
            var bytesToWrite = _currentChunkPosition;
            var chunkIndex = _currentChunkIndex;

            // If the current chunk position is 0, that means we are at the beginning of the
            // next chunk, so we have to write all bytes (chunk size) from the buffer using
            // the previous chunk index.
            if (_currentChunkPosition == 0)
            {
                bytesToWrite = _chunkSize;
                chunkIndex = _currentChunkIndex - 1;
            }

            byte[] bytes;
            if (bytesToWrite == _buffer.Length)
            {
                bytes = _buffer;
            }
            else
            {
                bytes = new byte[bytesToWrite];
                Array.ConstrainedCopy(_buffer, 0, bytes, 0, bytesToWrite);
            }

            MongoDbBlobProvider.WriteChunk(_fileIdentifier, chunkIndex, _traceInfo, bytes, _databaseCollection);

            _flushIsNecessary = false;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0 || count < 0 || buffer.Length < offset + count)
                throw new ArgumentException(
                    $"Invalid write parameters: buffer length {buffer.Length}, offset {offset}, count {count}.");
            if (Position >= Length)
                throw new InvalidOperationException("Stream length exceeded.");

            // nothing to write
            if (count == 0)
                return;

            // Initialize buffer here and not in the constructor 
            // to allocate memory only when it is needed.
            if (_buffer == null)
                _buffer = new byte[_chunkSize];

            var bytesToWrite = count;
            while (bytesToWrite > 0)
            {
                // if the inner buffer is already full, write it to the db
                if (_currentChunkPosition >= _chunkSize || _currentChunkPosition == 0 && _flushIsNecessary)
                {
                    Flush();

                    if (_currentChunkPosition >= _chunkSize)
                    {
                        // reset inner buffer position and move to the next chunk index
                        _currentChunkPosition = 0;
                        _currentChunkIndex++;
                    }
                }

                // we can only write so much bytes in one round as many slots are left in the inner buffer
                var maxBytesToWrite = Math.Min(bytesToWrite, _chunkSize - _currentChunkPosition);

                Array.ConstrainedCopy(buffer, offset, _buffer, _currentChunkPosition, maxBytesToWrite);
                
                bytesToWrite -= maxBytesToWrite;
                offset += maxBytesToWrite;
                _currentChunkPosition += maxBytesToWrite;
                _flushIsNecessary = true;
            }

            SetPosition(Position + count);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                Flush();
            }
            finally
            {
                base.Dispose(disposing);
            }
        }

        //=======================================================================

        public Stream CloneStream(BlobStorageContext context, Stream stream)
        {
            var providerData = (MongoDbBlobProviderData)context.BlobProviderData;
            var dabaseCollection = _databaseCollection;
            var traceInfo = context.GetTraceInfo();
            return new ChunkWriterStream(providerData, traceInfo, dabaseCollection);
        }
    }
}
