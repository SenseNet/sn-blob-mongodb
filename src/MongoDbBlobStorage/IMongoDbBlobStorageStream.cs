using System.IO;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    internal interface IMongoDbBlobStorageStream
    {
        Stream CloneStream(BlobStorageContext context, Stream stream);
    }
}
