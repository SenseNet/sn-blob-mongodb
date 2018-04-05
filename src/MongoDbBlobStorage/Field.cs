using System;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    internal static class Field
    {
        public static readonly string ChunkIndex = "ChunkIndex";
        public static readonly string FileIdentifier = "FileIdentifier";
        public static readonly string CreatedAt = "CreatedAt";
        public static readonly string Blob = "Blob";
    }

    internal class ChunkModel
    {
        public int ChunkIndex;
        public string FileIdentifier;
        public DateTime CreatedAt;
        public byte[] Blob;
    }
}
