# Tense.Rql.SqlServer #
Adds SQL Sever support for RQL. The **Tense.Rql.SqlServer** library is responsible for translating an RQL Statement into its equivalent SQL Statement for SQL Server. The main class that accomplishes this feature is the **RqlSqlGenerator**.

## RqlSqlGenerator ##
**RqlSqlGenerator**(int *batchlimit*)

Instantiates an **RqlSqlGenerator** instance of the generator. 

- *batchlimit* - the maximum number of records that can be returned in a single paged collection.

## GenerateSelectSingle ##
*string* **GenerateSelectSingle**<T>(**RqlNode** *node*, out **List\<SqlParameter\>** *parameters*) where T : class

The **GenerateSelectSingle** produces a SQL Statement that returns a entity resource of type T, conforming to the specifications of the **RqlNode**. 

- *node* - the **RqlNode** that specifies the requirements of the returned entity object.
- *parameters* - the set of Sql Parameters needed to run the SQL Statement

**Returns**
The SQL Statement needed to obtain the entity.

<h2>Change History</h2>
<table>
    <tr>
        <th>Date</th>
        <th>Description</th>
        <th>Version</th>
    </tr>
    <tr>
        <td>05/23/2022</td>
        <td>Fixed DateTime parsing bugs.</td>
        <td>0.0.8-alpha</td>
    </tr>  
    <tr>
        <td>05/26/2022</td>
        <td>Fixed Numeric parsing bugs.</td>
        <td>0.0.10-alpha</td>
    </tr>
    <tr>
        <td>05/28/2022</td>
        <td>Fixed aggregate paging.</td>
        <td>0.0.11-alpha</td>
    </tr>
    <tr>
        <td>05/28/2022</td>
        <td>Fixed collection paging.</td>
        <td>0.0.12-alpha</td>
    </tr>
    <tr>
        <td>05/29/2022</td>
        <td>Fixed Like, Contains and Exclude multi-value operations.</td>
        <td>0.0.13-alpha</td>
    </tr>
</table>