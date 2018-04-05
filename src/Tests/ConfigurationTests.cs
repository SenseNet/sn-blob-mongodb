using System;
using System.CodeDom;
using System.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;
using Configuration = SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage.Configuration;

namespace MongoDbBlobStorage.Tests
{
    [TestClass]
    public class ConfigurationTests : TestBase
    {
        public override TestContext TestContext { get; set; }

        [TestMethod]
        public void Config_ConnectionString_Current()
        {
            Assert.AreEqual(UsedConnectionString, Configuration.ConnectionString);
        }
        [TestMethod]
        public void Config_ConnectionString_FromAppConfig()
        {
            using (Swindle(typeof(Configuration), "ConnectionString", null))
                Assert.AreEqual("mongodb://MongoDbBlobDatabase_TestValue/MongoDbBlobDatabase_Test_42",
                    Configuration.ConnectionString);
        }
        [TestMethod]
        public void Config_ConnectionString()
        {
            var testValue = "ConnectionString-42";
            using (Swindle(typeof(Configuration), "ConnectionString", testValue))
                Assert.AreEqual(testValue, Configuration.ConnectionString);
        }
        [TestMethod]

        public void Config_ConnectionStringError_NoScheme()
        {
            try
            {
                var x = new MongoDbConnection("NoScheme-ConnectionString");
                Assert.Fail("ConfigurationErrorsException exception was not thrown.");
            }
            catch (System.Configuration.ConfigurationErrorsException e)
            {
                Assert.IsTrue(e.Message.ToLowerInvariant().Contains("missing scheme"));
            }
        }
        [TestMethod]
        public void Config_ConnectionStringError_NoDatabase()
        {
            try
            {
                var cn = new MongoDbConnection("mongodb://NoDatabase-ConnectionString");
                Assert.Fail("ConfigurationErrorsException exception was not thrown.");
            }
            catch (System.Configuration.ConfigurationErrorsException e)
            {
                Assert.IsTrue(e.Message.ToLowerInvariant().Contains("missing database name"));
            }
        }
        [TestMethod]
        public void Config_ConnectionStringError_MoreSegments_NoDatabase()
        {
            try
            {
                var cn = new MongoDbConnection("mongodb://host/segment1/segment2/segment3/");
                Assert.Fail("ConfigurationErrorsException exception was not thrown.");
            }
            catch (System.Configuration.ConfigurationErrorsException e)
            {
                Assert.IsTrue(e.Message.ToLowerInvariant().Contains("missing database name"));
            }

        }
        [TestMethod]
        public void Config_ConnectionStringError_MoreSegmentsAndOptions_NoDatabase()
        {
            try
            {
                var cn = new MongoDbConnection("mongodb://host/segment1/segment2/segment3/?replicaSet=test&otheroption=impossible/value");
                Assert.Fail("ConfigurationErrorsException exception was not thrown.");
            }
            catch (System.Configuration.ConfigurationErrorsException e)
            {
                Assert.IsTrue(e.Message.ToLowerInvariant().Contains("missing database name"));
            }
        }

        [TestMethod]
        public void Config_DatabaseName_FromAppConfig()
        {
            using (Swindle(typeof(Configuration), "ConnectionString", null))
                Assert.AreEqual("MongoDbBlobDatabase_Test_42", new MongoDbConnection().DatabaseName);
        }
        [TestMethod]
        public void Config_DatabaseName()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://localhost/{testValue}";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }


        [TestMethod]
        public void Config_CollectionName()
        {
            Assert.AreEqual("Blobs", MongoDbConnection.CollectionName);
        }

        [TestMethod]
        public void Config_DatabaseName_MoreHost()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://admin:P%40ssw0rd1@10.10.107.24:27017,10.10.107.26:27017,10.10.107.23:27017/{testValue}";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }

        [TestMethod]
        public void Config_DatabaseName_MoreHostAndOptions()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://db1.example.net:27017,db2.example.net:2500/{testValue}?replicaSet=test";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }

        [TestMethod]
        [ExpectedException(typeof(ConfigurationErrorsException))]
        public void Config_MissingDatabaseName_MoreHostAndOptions()
        {
            var cnstr = $"mongodb://db1.example.net:27017,db2.example.net:2500/?replicaSet=test";
            Assert.AreEqual("", new MongoDbConnection(cnstr).DatabaseName);
        }

        [TestMethod]
        public void Config_DatabaseName_MoreSegments()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://host/segment1/segment2/segment3/{testValue}";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }

        [TestMethod]
        public void Config_DatabaseName_SlashInThePassword()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://admin:P%40ss/w0rd1@10.10.107.24:27017,10.10.107.26:27017,10.10.107.23:27017/{testValue}?replicaSet=test";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }
        [TestMethod]
        public void Config_DatabaseName_SlashInTheOptions()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://admin:P%40ssw0rd1@10.10.107.24:27017,10.10.107.26:27017,10.10.107.23:27017/{testValue}?replicaSet=test&otheroption=impossible/value";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }
        [TestMethod]
        public void Config_DatabaseName_SlashInThePasswordAndOptions()
        {
            var testValue = "DatabaseName-42";
            var cnstr = $"mongodb://admin:P%40ss/w0rd1@10.10.107.24:27017,10.10.107.26:27017,10.10.107.23:27017/{testValue}?replicaSet=test&otheroption=impossible/value";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }
        [TestMethod]
        public void Config_DatabaseName_Space()
        {
            var testValue = "Database Name";
            var testValueEncoded = "Database%20Name";
            var cnstr = $"mongodb://admin:P%40ss/w0rd1@10.10.107.24:27017,10.10.107.26:27017,10.10.107.23:27017/{testValueEncoded}?replicaSet=test&otheroption=impossible/value";
            Assert.AreEqual(testValue, new MongoDbConnection(cnstr).DatabaseName);
        }

    }
}
