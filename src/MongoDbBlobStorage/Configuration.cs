using System.Configuration;
using SenseNet.Diagnostics;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    internal static class Configuration
    {
        internal const string DefaultConnectionString = "mongodb://localhost/SenseNetBlobStorage";
        private static string _connectionString;
        public static string ConnectionString
        {
            get => _connectionString ??
                   (_connectionString =
                       ConfigurationManager.ConnectionStrings["SenseNet.MongoDbBlobDatabase"]?.ToString()
                       ?? DefaultConnectionString);
            internal set => _connectionString = value;
        }

        private static int? _chunkSize;
        public static int ChunkSize
        {
            get
            {
                if (_chunkSize == null)
                {
                    _chunkSize= GetIntegerConfigValue("MongoDbBlobDatabaseChunkSize", 1024 * 8);
                    SnTrace.Database.Write("Configuration: MongoDbBlobDatabaseChunkSize: " + ChunkSize);
                }
                return _chunkSize.Value;
            }
        }

        private static int GetIntegerConfigValue(string key, int defaultValue)
        {
            var result = defaultValue;

            var configString = ConfigurationManager.AppSettings[key];
            if (string.IsNullOrEmpty(configString))
                return result;

            int configVal;
            if (int.TryParse(configString, out configVal))
                result = configVal;

            return result;
        }
    }
}
