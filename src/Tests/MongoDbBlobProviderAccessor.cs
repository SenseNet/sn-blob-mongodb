using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;

namespace MongoDbBlobStorage.Tests
{
    internal class MongoDbBlobProviderAccessor : MongoDbBlobProvider
    {
        public static void Cleanup()
        {
            _Cleanup();
        }

        public byte[] ReadChunk(string fileIdentifier, int chunkIndex)
        {
            return _ReadChunk(fileIdentifier, chunkIndex);
        }

        public void WriteChunk(BlobStorageContext context, string fileIdentifier, int chunkIndex, byte[] bytes)
        {
            _WriteChunk(context, fileIdentifier, chunkIndex, bytes);
        }
    }
}
