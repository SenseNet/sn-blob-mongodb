using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SenseNet.ContentRepository.Storage.Data.MongoDbBlobStorage;
using SenseNet.Diagnostics;

namespace MongoDbBlobStorage.Tests
{
    [TestClass]
    public abstract class TestBase
    {
        public abstract TestContext TestContext { get; set; }

        private SnTrace.Operation _currentOperation;

        internal static string UsedConnectionString => Configuration.DefaultConnectionString + "_ForTests";

        [AssemblyInitialize]
        public static void InitializeAllTests(TestContext context)
        {
            Configuration.ConnectionString = UsedConnectionString;

            MongoDbBlobProviderAccessor.Cleanup();

            new PrivateType("SenseNet.BlobStorage", "SenseNet.ContentRepository.Storage.BlobStorageComponents")
                .SetStaticProperty("DataProvider", new TestBlobStorageMetaDataProvider());

            new PrivateType("SenseNet.BlobStorage", "SenseNet.ContentRepository.Storage.Data.SqlClient.BuiltInBlobProviderSelector")
                .SetStaticFieldOrProperty("ExternalBlobProvider", new MongoDbBlobProvider { ChunkSize = 10 });

            SnTrace.EnableAll();
        }

        [AssemblyCleanup]
        public static void FinalizeAllTests()
        {
            SnTrace.Flush();
        }

        [TestInitialize]
        public void CheckBeforeTest()
        {
            _currentOperation = SnTrace.Test.StartOperation("TESTMETHOD: {0}", TestContext.TestName);
        }
        [TestCleanup]
        public void CheckAfterTest()
        {
            if (_currentOperation == null)
                return;
            _currentOperation.Successful = true;
            _currentOperation.Dispose();
        }


        public IDisposable Swindle(Type @class, string memberName, object cheat)
        {
            return new Swindler(@class, memberName, cheat);
        }

        private class Swindler : IDisposable
        {
            private PrivateType _accessor;
            private string _memberName;
            private object _originalValue;

            public Swindler(Type @class, string memberName, object cheat)
            {
                _accessor = new PrivateType(@class);
                _memberName = memberName;
                _originalValue = _accessor.GetStaticFieldOrProperty(memberName);
                _accessor.SetStaticFieldOrProperty(_memberName, cheat);
            }
            public void Dispose()
            {
                _accessor.SetStaticFieldOrProperty(_memberName, _originalValue);
            }
        }

    }
}
