using System.Data.SqlClient;
using Tense.Rql.SqlServer.UnitTests.Models;

namespace Tense.Rql.SqlServer.UnitTests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            var node = RqlNode.Parse("");
            var generator = new RqlSqlGenerator(100);

            var sqlStatement = generator.GenerateResourceCollectionStatement<EAuthor>(node, out List<SqlParameter> parameters);
        }

        [TestMethod]
        public void TestMethod2()
        {
            var node = RqlNode.Parse("SORT(LastName,FirstName)");
            var generator = new RqlSqlGenerator(100);

            var sqlStatement = generator.GenerateResourceCollectionStatement<EAuthor>(node, out List<SqlParameter> parameters);
        }

        [TestMethod]
        public void TestMethod3()
        {
            var node = RqlNode.Parse("SORT(LastName,FirstName)");
            var generator = new RqlSqlGenerator(100);

            var sqlStatement = generator.GenerateResourceCollectionStatement<EAuthor>(node, out List<SqlParameter> parameters, true);
        }

        [TestMethod]
        public void TestMethod4()
        {
            try
            {
                var node = RqlNode.Parse("Mean(Score1)");
                var generator = new RqlSqlGenerator(100);

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod5()
        {
            try
            {
                var node = RqlNode.Parse("Aggregate(Category,Mean(Score1))");
                var generator = new RqlSqlGenerator(100);

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }
    }
}