using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage;
using SenseNet.ContentRepository.Storage.Data;

namespace MongoDbBlobStorage.Tests
{
    internal class TestBlobStorageMetaDataProvider : IBlobStorageMetaDataProvider
    {
        private static readonly Dictionary<int, Tuple<IBlobProvider, string>> Storage =
            new Dictionary<int, Tuple<IBlobProvider, string>>();

        public bool IsFilestreamEnabled()
        {
            throw new NotImplementedException();
        }

        public BlobStorageContext GetBlobStorageContext(int fileId, bool clearStream, int versionId, int propertyTypeId)
        {
            var record = Storage[fileId];
            return new BlobStorageContext(record.Item1, record.Item2) {VersionId = versionId, PropertyTypeId = propertyTypeId};
        }

        public Task<BlobStorageContext> GetBlobStorageContextAsync(int fileId, bool clearStream, int versionId, int propertyTypeId)
        {
            throw new NotImplementedException();
        }

        public void InsertBinaryProperty(IBlobProvider blobProvider, BinaryDataValue value, int versionId, int propertyTypeId,
            bool isNewNode)
        {
            throw new NotImplementedException();
        }

        public void InsertBinaryPropertyWithFileId(BinaryDataValue value, int versionId, int propertyTypeId, bool isNewNode)
        {
            throw new NotImplementedException();
        }

        public void UpdateBinaryProperty(IBlobProvider blobProvider, BinaryDataValue value)
        {
            throw new NotImplementedException();
        }

        public void DeleteBinaryProperty(int versionId, int propertyTypeId)
        {
            throw new NotImplementedException();
        }

        public BinaryCacheEntity LoadBinaryCacheEntity(int versionId, int propertyTypeId)
        {
            throw new NotImplementedException();
        }

        public string StartChunk(IBlobProvider blobProvider, int versionId, int propertyTypeId, long fullSize)
        {
            var ctx = new BlobStorageContext(blobProvider)
            {
                VersionId = versionId,
                PropertyTypeId = propertyTypeId,
                FileId = 0,
                Length = fullSize
            };

            if (blobProvider != new PrivateType(typeof(BlobStorageBase)).GetStaticFieldOrProperty("BuiltInProvider"))
                blobProvider.Allocate(ctx);
            else
                throw new NotSupportedException();

            var binaryPropertyId = versionId*10 + propertyTypeId;
            var fileId = binaryPropertyId*2;

            ctx.FileId = fileId;

            var blobProviderData = BlobStorageContext.SerializeBlobProviderData(ctx.BlobProviderData);
            Storage[fileId] = new Tuple<IBlobProvider, string>(blobProvider, blobProviderData);

            return
                (string)
                new PrivateObject(new ChunkToken
                {
                    VersionId = versionId,
                    PropertyTypeId = propertyTypeId,
                    BinaryPropertyId = binaryPropertyId,
                    FileId = fileId
                }).Invoke("GetToken");
        }

        public void CommitChunk(int versionId, int propertyTypeId, int fileId, long fullSize, BinaryDataValue source)
        {
            throw new NotImplementedException();
        }

        public void CleanupFilesSetDeleteFlag()
        {
            throw new NotImplementedException();
        }

        public bool CleanupFiles()
        {
            throw new NotImplementedException();
        }
    }
}
