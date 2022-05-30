using System.Data;
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
            try
            {
                var node = RqlNode.Parse("");
                var generator = new RqlSqlGenerator(100);

                var sqlStatement = generator.GenerateResourceCollectionStatement<EAuthor>(node, out List<SqlParameter> parameters);

                var expectedSql = @"SELECT [dbo].[Authors].[AuthorId], [dbo].[Authors].[FirstName], [dbo].[Authors].[LastName], [dbo].[Authors].[Website]
  FROM [dbo].[Authors] WITH(NOLOCK)
 ORDER BY AuthorId
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 0);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod2()
        {
            try
            {
                var node = RqlNode.Parse("SORT(LastName,FirstName)");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT [dbo].[Authors].[AuthorId], [dbo].[Authors].[FirstName], [dbo].[Authors].[LastName], [dbo].[Authors].[Website]
  FROM [dbo].[Authors] WITH(NOLOCK)
 ORDER BY [dbo].[Authors].[LastName], [dbo].[Authors].[FirstName]
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                var sqlStatement = generator.GenerateResourceCollectionStatement<EAuthor>(node, out List<SqlParameter> parameters);

                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 0);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }


[TestMethod]
        public void TestMethod3()
        {
            try
            {
                var node = RqlNode.Parse("Mean(Score1)");
                var generator = new RqlSqlGenerator(100);

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);

                var expectedSql = @"SELECT avg([dbo].[Customers].[Score1]) as [Score1]
  FROM [dbo].[Customers] WITH(NOLOCK)";

                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 0);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod4()
        {
            try
            {
                var node = RqlNode.Parse("Aggregate(Category,Mean(Score1))");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT [dbo].[Customers].[Category], avg([dbo].[Customers].[Score1]) as [Score1]
  FROM [dbo].[Customers] WITH(NOLOCK)
 GROUP BY [dbo].[Customers].[Category]
 ORDER BY [dbo].[Customers].[Category]
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);
                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 0);
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
                var node = RqlNode.Parse("values(Category)");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT DISTINCT [dbo].[Customers].[Category]
  FROM [dbo].[Customers] WITH(NOLOCK)
 ORDER BY [dbo].[Customers].[Category]";

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters, true);
                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 0);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod6()
        {
            try
            {
                var node = RqlNode.Parse("AuthorId=8&sort(AuthorId,BookId)");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT [dbo].[BooksByAuthor].[AuthorId], [dbo].[BooksByAuthor].[BookId], [dbo].[BooksByAuthor].[Title], [dbo].[BooksByAuthor].[PublishDate], [dbo].[BooksByAuthor].[CategoryId], [dbo].[BooksByAuthor].[Synopsis]
  FROM [dbo].[BooksByAuthor] WITH(NOLOCK)
 WHERE [dbo].[BooksByAuthor].[AuthorId] = @P0
 ORDER BY [dbo].[BooksByAuthor].[AuthorId], [dbo].[BooksByAuthor].[BookId]
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                var sqlStatement = generator.GenerateResourceCollectionStatement<EBooksByAuthor>(node, out List<SqlParameter> parameters);
                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 1);
                Assert.AreEqual(parameters[0].ParameterName, "@P0");
                Assert.AreEqual(parameters[0].SqlDbType, SqlDbType.Int);
                Assert.AreEqual(parameters[0].Value, 8);
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod7()
        {
            try
            {
                var node = RqlNode.Parse("Like(LastName,T*,J*)");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT [dbo].[Customers].[Id], [dbo].[Customers].[FirstName], [dbo].[Customers].[LastName], [dbo].[Customers].[Category], [dbo].[Customers].[Age], [dbo].[Customers].[Score1], [dbo].[Customers].[Score2]
  FROM [dbo].[Customers] WITH(NOLOCK)
 WHERE ([dbo].[Customers].[LastName] LIKE (@P0) OR [dbo].[Customers].[LastName] LIKE (@P1))
 ORDER BY Id
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);
                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 2);
                Assert.AreEqual(parameters[0].ParameterName, "@P0");
                Assert.AreEqual(parameters[0].SqlDbType, SqlDbType.VarChar);
                Assert.AreEqual(parameters[0].Value, "T%");
                Assert.AreEqual(parameters[1].ParameterName, "@P1");
                Assert.AreEqual(parameters[1].SqlDbType, SqlDbType.VarChar);
                Assert.AreEqual(parameters[1].Value, "J%");
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod8()
        {
            try
            {
                var node = RqlNode.Parse("contains(LastName,T*,J*)");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT [dbo].[Customers].[Id], [dbo].[Customers].[FirstName], [dbo].[Customers].[LastName], [dbo].[Customers].[Category], [dbo].[Customers].[Age], [dbo].[Customers].[Score1], [dbo].[Customers].[Score2]
  FROM [dbo].[Customers] WITH(NOLOCK)
 WHERE ([dbo].[Customers].[LastName] LIKE (@P0) OR [dbo].[Customers].[LastName] LIKE (@P1))
 ORDER BY Id
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);
                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 2);
                Assert.AreEqual(parameters[0].ParameterName, "@P0");
                Assert.AreEqual(parameters[0].SqlDbType, SqlDbType.VarChar);
                Assert.AreEqual(parameters[0].Value, "T%");
                Assert.AreEqual(parameters[1].ParameterName, "@P1");
                Assert.AreEqual(parameters[1].SqlDbType, SqlDbType.VarChar);
                Assert.AreEqual(parameters[1].Value, "J%");
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }

        [TestMethod]
        public void TestMethod9()
        {
            try
            {
                var node = RqlNode.Parse("excludes(LastName,T*,J*)");
                var generator = new RqlSqlGenerator(100);

                var expectedSql = @"SELECT [dbo].[Customers].[Id], [dbo].[Customers].[FirstName], [dbo].[Customers].[LastName], [dbo].[Customers].[Category], [dbo].[Customers].[Age], [dbo].[Customers].[Score1], [dbo].[Customers].[Score2]
  FROM [dbo].[Customers] WITH(NOLOCK)
 WHERE ([dbo].[Customers].[LastName] NOT LIKE (@P0) AND [dbo].[Customers].[LastName] NOT LIKE (@P1))
 ORDER BY Id
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";

                var sqlStatement = generator.GenerateResourceCollectionStatement<ECustomer>(node, out List<SqlParameter> parameters);
                Assert.AreEqual(sqlStatement, expectedSql);
                Assert.AreEqual(parameters.Count, 2);
                Assert.AreEqual(parameters[0].ParameterName, "@P0");
                Assert.AreEqual(parameters[0].SqlDbType, SqlDbType.VarChar);
                Assert.AreEqual(parameters[0].Value, "T%");
                Assert.AreEqual(parameters[1].ParameterName, "@P1");
                Assert.AreEqual(parameters[1].SqlDbType, SqlDbType.VarChar);
                Assert.AreEqual(parameters[1].Value, "J%");
            }
            catch (Exception error)
            {
                Assert.Fail(error.Message);
            }
        }
    }
}