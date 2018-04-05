using System;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    public class MongoDbBlobProviderData
    {
        public string FileIdentifier { get; set; }
        public int ChunkSize { get; set; }
        public long BlobSize { get; set; }

        public static MongoDbBlobProviderData Create(int chunkSize, long blobSize)
        {
            if (chunkSize < 1)
                throw new ArgumentException("The chunkSize cannot less than 1.");
            if (blobSize < 1L)
                throw new ArgumentException("The blobSize cannot less than 1.");

            return new MongoDbBlobProviderData
            {
                FileIdentifier = Guid.NewGuid().ToString(),
                ChunkSize = chunkSize,
                BlobSize = blobSize
            };
        }

        public override string ToString()
        {
            return $"MongoDbBlobData: FileIdentifier:{FileIdentifier}, ChunkSize:{ChunkSize}, BlobSize:{BlobSize}.";
        }
    }
}
