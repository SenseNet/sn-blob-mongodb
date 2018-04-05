using System;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    public static class Extensions
    {
        public static int ToInt(this long int64)
        {
            return Convert.ToInt32(int64);
        }

        internal static TraceInfo GetTraceInfo(this BlobStorageContext context)
        {
            return new TraceInfo
            {
                VersionId = context.VersionId,
                PropertyTypeId = context.PropertyTypeId,
                FileSize = context.Length
            };
        }
    }
}
