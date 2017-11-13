using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;


namespace TableStoragePrototypeing.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
private void TestMethod1()
        {
            var table = TableStorageConnector<DynamicTableEntity>();
        }
    }
}
