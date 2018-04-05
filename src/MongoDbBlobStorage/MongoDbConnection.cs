using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Web;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage
{
    internal class MongoDbConnection
    {
        public MongoDbConnection() : this(Configuration.ConnectionString) { }
        public MongoDbConnection(string connectionString)
        {
            const string mongdbPrefix = "mongodb://";

            if (!connectionString.StartsWith(mongdbPrefix, StringComparison.InvariantCultureIgnoreCase))
                throw new ConfigurationErrorsException("Missing scheme. MongoDB connection string must start with 'mongodb://' scheme.");

            var segments = connectionString.Substring(mongdbPrefix.Length).Split('?')[0].Split('/');

            if (segments.Length < 2)
                throw new ConfigurationErrorsException("Missing database name. MongoDB connection string must contain a database name in this form: 'mongodb://host/<databaseName>'.");

            DatabaseName = HttpUtility.UrlDecode(segments.Last());

            if(string.IsNullOrEmpty(DatabaseName))
                throw new ConfigurationErrorsException("Missing database name. MongoDB connection string must contain a database name in this form: 'mongodb://host/<databaseName>'.");
        }

        public string DatabaseName { get; }
        public static readonly string CollectionName = "Blobs";

        private IMongoClient _client;
        internal IMongoClient GetClient()
        {
            return _client ?? (_client = new MongoClient(Configuration.ConnectionString));
        }

        private IMongoDatabase _database;
        internal IMongoDatabase GetDatabase()
        {
            return _database ?? (_database = GetClient().GetDatabase(DatabaseName));
        }

        private IMongoCollection<BsonDocument> _collection;
        internal virtual IMongoCollection<BsonDocument> GetBlobCollection()
        {
            return _collection ?? (_collection = GetDatabase().GetCollection<BsonDocument>("Blobs"));
        }


        internal void DropDatabase()
        {
            GetClient().DropDatabase(DatabaseName);
        }


        internal void CreateDatabase()
        {
            var db = GetClient().GetDatabase(DatabaseName);
            var blobs = db.GetCollection<BsonDocument>("Blobs");

            var key1 = new BsonDocument(new Dictionary<string, object> { { "FileIdentifier", 1 }, { "ChunkIndex", 1 } });
            var indexModel1 = new CreateIndexModel<BsonDocument>(key1,
                new CreateIndexOptions {Name = "FileIdentifier_ChunkIndex" /*, Unique = true */});

            blobs.Indexes.CreateMany(new[] { indexModel1 });
        }
    }
}
