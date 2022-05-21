# Tense.Rql.SqlServer #
Adds SQL Sever support for RQL. The **Tense.Rql.SqlServer* library is responsible for translating an RQL Statement into its equivalent SQL Statement for SQL Server. The main class that accomplishes this feature is the RqlSqlGenerator.

## RqlSqlGenerator ##
**RqlSqlGenerator**(int *batchlimit*)

Instantiates an **RqlSqlGenerator** instance of the generator. 

- *batchlimit* - the maximum number of records that can be returned in a single paged collection.

## GenerateSelectSingle ##
*string* **GenerateSelectSingle**<T>(**RqlNode** *node*, out **List/<SqlParameter/>** *parameters*) where T : class

The **GenerateSelectSingle** produces a SQL Statement that returns a entity resource of type T, conforming to the specifications of the **RqlNode**. 

- *node* - the **RqlNode** that specifies the requirements of the returned entity object.
- *parameters* - the set of Sql Parameters needed to run the SQL Statement

**Returns**
The SQL Statement needed to obtain the entity.