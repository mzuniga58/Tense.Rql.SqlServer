using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;

namespace Tense.Rql.SqlServer
{
	/// <summary>
	/// Generates SQL from an RqlNode specification
	/// </summary>
    public class RqlSqlGenerator
    {
		private const string OB = "[";
		private const string CB = "]";
		private readonly int _batchLimit;

		/// <summary>
		/// Emitter
		/// </summary>
		/// <param name="batchLimit">The maximum number of records that can be returned from a single query.</param>
		public RqlSqlGenerator(int batchLimit)
		{
			_batchLimit = batchLimit;	
		}

		/// <summary>
		/// Builds the SQL Query to update items in the datastore
		/// </summary>
		/// <param name="entityType">The type of object to update.</param>
		/// <param name="item">The item to update</param>
		/// <param name="parameters">The list of <see cref="SqlParameter"/>s used in the query.</param>
		/// <param name="node">The <see cref="RqlNode"/> that further constrains the update.</param>
		/// <returns>The SQL query used to update items in the datastore</returns>
		public string GenerateUpdateStatement(Type entityType, object item, RqlNode node, out List<SqlParameter> parameters) 
		{
			parameters = new List<SqlParameter>();
			var tableAttribute = entityType.GetCustomAttribute<Table>();
			var properties = entityType.GetProperties();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {item.GetType().Name} is not an entity model.");

			var sql = new StringBuilder();

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.AppendLine($"UPDATE {OB}{tableAttribute.Name}{CB}");
			else
				sql.AppendLine($"UPDATE {OB}{tableAttribute.Schema}{CB}.{OB}{tableAttribute.Name}{CB}");

			bool first = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					if (memberAttribute.SkipUpdate)
						continue;

					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					if (string.Equals(tableName, tableAttribute.Name, StringComparison.InvariantCulture))
					{
						if (!memberAttribute.IsPrimaryKey && !memberAttribute.AutoField)
						{
							var parameterName = $"@P{parameters.Count}";
							var propValue = property.GetValue(item);
							parameters.Add(BuildSqlParameter(parameterName, property, propValue));

							if (first)
							{
								sql.Append($" SET {OB}{columnName}{CB} = {parameterName}");
								first = false;
							}
							else
							{
								sql.AppendLine(",");
								sql.Append($"     {OB}{columnName}{CB} = {parameterName}");
							}
						}
					}
				}
			}

			AppendWhereClause(sql, whereClause);
			return sql.ToString();
		}

		/// <summary>
		/// Builds a query to delete an object from the datastore using the specfied keys
		/// </summary>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <param name="node">The <see cref="RqlNode"/> that further constrains the delete.</param>
		/// <returns></returns>
		public string GenerateDeleteStatement<T>(RqlNode node, out List<SqlParameter> parameters)
		{
			parameters = new List<SqlParameter>();
			var tableAttribute = typeof(T).GetCustomAttribute<Table>();

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {typeof(T).Name} is not an entity model.");

			var properties = typeof(T).GetProperties();
			var sql = new StringBuilder();

			string whereClause = ParseWhereClause<T>(node, null, tableAttribute, properties, parameters);

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.AppendLine($"DELETE FROM {OB}{tableAttribute.Name}{CB}");
			else
				sql.AppendLine($"DELETE FROM {OB}{tableAttribute.Schema}{CB}.{OB}{tableAttribute.Name}{CB}");

			AppendWhereClause(sql, whereClause);

			return sql.ToString();
		}

		/// <summary>
		/// Builds the SQL query to add an entity to the datastore
		/// </summary>
		/// <param name="entityType">The type of entity to add</param>
		/// <param name="item">The entity to be added</param>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <param name="identityProperty"></param>
		/// <returns></returns>
		public string GenerateInsertStatement(Type entityType, object item, out List<SqlParameter> parameters, out PropertyInfo? identityProperty) 
		{
			parameters = new List<SqlParameter>();	
			var tableAttribute = entityType.GetCustomAttribute<Table>();

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var sql = new StringBuilder();
			var properties = entityType.GetProperties();
			var containsIdentity = false;

			identityProperty = null;

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.Append($"INSERT INTO {OB}{tableAttribute.Name}{CB}\r\n (");
			else
				sql.Append($"INSERT INTO {OB}{tableAttribute.Schema}{CB}.{OB}{tableAttribute.Name}{CB}\r\n (");

			bool firstField = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					bool includeField = true;

					//	If the member is an identity column, then SQL will assign the value,
					//	do not include it in the list of columns to insert
					if (memberAttribute.IsIdentity)
					{
						containsIdentity = true;
						includeField = false;
						identityProperty = property;
					}

					if (memberAttribute.AutoField)
						includeField = false;

					//	Only columns that belong to the main table are inserted
					if (!string.Equals(tableName, tableAttribute.Name, StringComparison.InvariantCulture))
						includeField = false;

					if (includeField)
					{
						if (firstField)
						{
							sql.Append($"{OB}{columnName}{CB}");
							firstField = false;
						}
						else
						{
							sql.Append($", {OB}{columnName}{CB}");
						}
					}
				}
			}

			sql.AppendLine(")");
			if (containsIdentity)
			{
				foreach (var property in properties)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
						var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

						if (string.Compare(tableName, tableAttribute.Name, true) == 0)
							if (memberAttribute.IsIdentity)
								sql.AppendLine($" OUTPUT inserted.{OB}{columnName}{CB}");
					}
				}
			}

			sql.Append(" VALUES (");
			firstField = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

					bool includeField = true;

					//	Don't include identity columns
					if (memberAttribute.IsIdentity)
						includeField = false;

					//	Don't include auto fields
					if (memberAttribute.AutoField)
						includeField = false;

					//	Don't include foreign columns
					if (string.Compare(tableName, tableAttribute.Name, true) != 0)
						includeField = false;

					if (includeField)
					{
						var parameterName = $"@P{parameters.Count}";
						parameters.Add(BuildSqlParameter(parameterName, property, property.GetValue(item)));

						if (firstField)
						{
							sql.Append($"{parameterName}");
							firstField = false;
						}
						else
						{
							sql.Append($", {parameterName}");
						}
					}
				}
			}

			sql.Append(')');

			return sql.ToString();
		}

		/// <summary>
		/// Builds the SQL query for a single result
		/// </summary>
		/// <param name="node"></param>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <returns></returns>
		public string GenerateSelectSingle<T>(RqlNode node, out List<SqlParameter> parameters)
        {
			return GenerateSelectSingle(typeof(T), node, out parameters);
        }

		/// <summary>
		/// Builds the SQL query for a single result
		/// </summary>
		/// <param name="entityType">The type of object to update.</param>
		/// <param name="node"></param>
		/// <param name="parameters">The list of SQL parameters that must be bound to execute the SQL statement</param>
		/// <returns></returns>
		public string GenerateSelectSingle(Type entityType, RqlNode node, out List<SqlParameter> parameters)
		{
			parameters = new List<SqlParameter>();
			var tableAttribute = entityType.GetCustomAttribute<Table>();

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var orderByClause = ParseOrderByClause(entityType, node);
			var selectFields = node?.Find(RqlOperation.SELECT);
			bool firstField = true;

			sql.Append("SELECT ");

			if (node?.Find(RqlOperation.DISTINCT) != null)
			{
				sql.Append("DISTINCT ");
			}

			if (node?.Find(RqlOperation.FIRST) != null)
			{
				sql.Append("TOP 1 ");
			}

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField);
				}
			}

			AppendFromClause(sql, tableAttribute);
			AppendWhereClause(sql, whereClause);
			AppendOrderByClause(sql, orderByClause);

			return sql.ToString();
		}

		/// <summary>
		/// Builds the count query for the collection
		/// </summary>
		/// <param name="node">The <see cref="RqlNode"/> that filters the result set.</param>
		/// <param name="parameters">The list of <see cref="SqlParameter"/>s used in the SQL Command.</param>
		/// <param name="entityType">The type of entities to retrieve</param>
		/// <returns></returns>
		public string GenerateCollectionCountStatement(Type entityType, RqlNode node, out List<SqlParameter> parameters)
		{
			parameters = new List<SqlParameter>();	
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);

			if (node.Find(RqlOperation.AGGREGATE) is RqlNode aggregateNode)
			{
				sql.AppendLine("SELECT COUNT(*) as [RecordCount]");
				sql.Append("  FROM ( SELECT ");

				bool firstMember = true;

				foreach (RqlNode childNode in aggregateNode)
				{
					if (childNode.Operation == RqlOperation.PROPERTY)
					{
						var property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						AppendPropertyForRead(sql, tableAttribute, property, ref firstMember);
					}
				}

				AppendFromClause(sql, tableAttribute);
				AppendWhereClause(sql, whereClause);
				AppendGroupByClause(sql, aggregateNode, tableAttribute, properties);

				sql.Append(") as T0");
			}
			else if (node.Find(RqlOperation.MAX) != null ||
					 node.Find(RqlOperation.MIN) != null ||
					 node.Find(RqlOperation.MEAN) != null ||
					 node.Find(RqlOperation.SUM) != null)
			{
				sql.Append("SELECT 1 AS [RecordCount]");
			}
			if (node.Find(RqlOperation.VALUES) is RqlNode valuesNode)
			{
				sql.Append("SELECT COUNT(*) as [RecordCount] FROM (SELECT DISTINCT ");
				bool firstMember = true;

				var propertyNode = valuesNode.NonNullValue<RqlNode>(0);
				var property = properties.FirstOrDefault(p => string.Equals(propertyNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
				AppendProperty(sql, tableAttribute, property, ref firstMember);
				AppendFromClause(sql, tableAttribute);
				AppendWhereClause(sql, whereClause);
				sql.Append(") AS T0");
			}
			else if (node.Find(RqlOperation.FIRST) is not null)
			{
				sql.Append("SELECT 1 AS [RecordCount]");
			}
			else if (node.Find(RqlOperation.DISTINCT) != null)
			{
				var selectFields = node?.Find(RqlOperation.SELECT);

				sql.Append("SELECT COUNT(*) AS [RecordCount] FROM (SELECT DISTINCT ");

				bool firstField = true;

				foreach (var property in properties)
				{
					if (RqlUtilities.CheckForInclusion(property, selectFields, false))
					{
						AppendPropertyForRead(sql, tableAttribute, property, ref firstField);
					}
				}

				AppendFromClause(sql, tableAttribute);
				AppendWhereClause(sql, whereClause);
				sql.AppendLine(") AS T0");
			}
			else
			{
				sql.Append("SELECT COUNT(*) AS [RecordCount]");

				AppendFromClause(sql, tableAttribute);
				AppendWhereClause(sql, whereClause);
			}

			return sql.ToString();
		}

		/// <summary>
		/// Builds the SQL query for the collection
		/// </summary>
		/// <param name="node">The compiled RQL query</param>
		/// <param name="count">The number of records in the collection</param>
		/// <param name="batchLimit">The maximum number of items that can be included in a collection batch</param>
		/// <param name="parameters">The parameters for the SQL query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		public string GenerateResourceCollectionStatement<T>(RqlNode node, int count, int batchLimit, out List<SqlParameter> parameters, bool NoPaging = false)
        {
			return GenerateResourceCollectionStatement(typeof(T), node, count, batchLimit, out parameters, NoPaging);
		}

		/// <summary>
		/// Builds the SQL query for the collection
		/// </summary>
		/// <param name="entityType">The type of entity to query.</param>
		/// <param name="node">The compiled RQL query</param>
		/// <param name="count">The number of records in the collection</param>
		/// <param name="batchLimit">The maximum number of items that can be included in a collection batch</param>
		/// <param name="parameters">The parameters for the SQL query</param>
		/// <param name="NoPaging">Do not page results even if the result set exceeds the system defined limit. Default value = false.</param>
		/// <returns></returns>
		public string GenerateResourceCollectionStatement(Type entityType, RqlNode node, int count, int batchLimit, out List<SqlParameter> parameters, bool NoPaging = false)
		{
			parameters = new List<SqlParameter>();

			if (node.Operation == RqlOperation.NOOP)
				return BuildStandardPagedCollection(entityType, node, count, parameters);

			RqlNode? pageFilter = node.ExtractLimitClause();

			if (!node.Contains(RqlOperation.AGGREGATE) && (node.Contains(RqlOperation.MAX) ||
															node.Contains(RqlOperation.MIN) ||
															node.Contains(RqlOperation.MEAN) ||
															node.Contains(RqlOperation.COUNT) ||
															node.Contains(RqlOperation.SUM)))
			{
				//	Column Aggregates are never paged
				return BuildColumnAggregateCollection(entityType, node, parameters);
			}
			else if (node.Contains(RqlOperation.VALUES))
			{
				//	Values collections are never paged
				return BuildValuesCollection(entityType, node, count, batchLimit, parameters, NoPaging);
			}
			else if (NoPaging || (count < batchLimit && pageFilter == null))
			{
				//	We need to build a non-paged collection

				if (node.Contains(RqlOperation.AGGREGATE))
					return BuildAggregateNonPagedCollection(entityType, node, parameters);
				else if (node.Contains(RqlOperation.DISTINCT))
					return BuildDistinctNonPagedCollection(entityType, node, parameters);
				else
					return BuidStandardNonPagedCollection(entityType, node, parameters);
			}
			else
			{
				//	We need to build a paged collection

				if (node.Contains(RqlOperation.AGGREGATE))
					return BuildAggregatePagedCollection(entityType, node, count, pageFilter, parameters);
				else if (node.Contains(RqlOperation.DISTINCT))
					return BuildDistinctPagedCollection(entityType, node, count, pageFilter, parameters);
				else
					return BuildStandardPagedCollection(entityType, node, count, parameters);
			}
		}

		private string BuildStandardPagedCollection(Type entityType, RqlNode node, int count, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var orderByClause = ParseOrderByClause(entityType, node);
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node?.Find(RqlOperation.SELECT);

			bool firstField = true;

			sql.Append("SELECT ");

			if (node?.Find(RqlOperation.DISTINCT) != null)
			{
				sql.Append("DISTINCT ");
			}

			if (node?.Find(RqlOperation.FIRST) != null)
			{
				sql.Append("TOP 1 ");
			}

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField, "T0");
				}
			}

			sql.AppendLine();
			sql.AppendLine(" FROM (");
			sql.AppendLine(" SELECT ROW_NUMBER() OVER ( ORDER BY ");

			if (string.IsNullOrWhiteSpace(orderByClause))
			{
				firstField = true;
				foreach (var property in properties)
				{
					var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

					if (memberAttribute != null)
					{
						if (memberAttribute.IsPrimaryKey)
						{
							AppendPropertyForOrderBy(sql, property, ref firstField);
						}
					}
				}
			}
			else
			{
				sql.Append(orderByClause);
			}

			sql.AppendLine(") as [ROW_NUMBER],");
			firstField = true;

			foreach (var property in properties)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					if (RqlUtilities.CheckForInclusion(property, selectFields))
					{
						AppendPropertyForRead(sql, tableAttribute, property, ref firstField);
					}
				}
			}

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.Append($"\r\n           FROM [{tableAttribute.Name}] WITH(NOLOCK)");
			else
				sql.Append($"\r\n           FROM [{tableAttribute.Schema}].[{tableAttribute.Name}] WITH(NOLOCK) ");

			AppendWhereClause(sql, whereClause);

			int start = 1;

			RqlNode? pageFilter = node?.ExtractLimitClause();

			if (pageFilter != null)
			{
				start = pageFilter.NonNullValue<int>(0);
				count = pageFilter.NonNullValue<int>(1);
			}

			if (count > _batchLimit)
				count = _batchLimit;

			sql.AppendLine($") as [t0]");
			sql.AppendLine($" WHERE [t0].[ROW_NUMBER] BETWEEN {start} AND {start + count - 1}");
			sql.AppendLine($" ORDER BY [t0].[ROW_NUMBER]");

			return sql.ToString();
		}

		private string BuidStandardNonPagedCollection(Type entityType, RqlNode node, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var orderByClause = ParseOrderByClause(entityType, node);
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node?.Find(RqlOperation.SELECT);

			sql.Append("SELECT ");

			if (node?.Find(RqlOperation.FIRST) != null)
			{
				sql.Append("TOP 1 ");
			}

			bool firstField = true;
			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField);
				}
			}

			AppendFromClause(sql, tableAttribute);
			AppendWhereClause(sql, whereClause);
			AppendOrderByClause(sql, orderByClause);

			return sql.ToString();
		}

		private string BuildDistinctPagedCollection(Type entityType, RqlNode node, int count, RqlNode? pageFilter, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node.Find(RqlOperation.SELECT);

			bool firstField = true;

			sql.Append("SELECT ");

			if (node.Find(RqlOperation.FIRST) != null)
			{
				sql.Append("TOP 1 ");
			}

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields, false))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField, "T0");
				}
			}

			var orderByClause = ParseOrderByClause(entityType, node, "T1");

			sql.AppendLine();
			sql.Append(" FROM ( SELECT ROW_NUMBER() OVER ( ORDER BY ");

			if (string.IsNullOrWhiteSpace(orderByClause))
			{
				firstField = true;
				foreach (var property in properties)
				{
					if (RqlUtilities.CheckForInclusion(property, selectFields, false))
					{
						AppendPropertyForRead(sql, tableAttribute, property, ref firstField, "T1");
					}
				}
			}
			else
			{
				sql.Append(orderByClause);
			}

			sql.AppendLine(") as [ROW_NUMBER],");
			firstField = true;

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields, false))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField, "T1");
				}
			}

			sql.Append(" FROM ( SELECT DISTINCT ");
			firstField = true;

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields, false))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField);
				}
			}

			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.Append($" FROM [{tableAttribute.Name}] WITH(NOLOCK)");
			else
				sql.Append($" FROM [{tableAttribute.Schema}].[{tableAttribute.Name}] WITH(NOLOCK) ");

			AppendWhereClause(sql, whereClause);

			sql.Append(") as [T1]");

			int start = 1;

			if (pageFilter != null)
			{
				start = pageFilter.NonNullValue<int>(0);
				count = pageFilter.NonNullValue<int>(1);
			}

			if (count > _batchLimit)
				count = _batchLimit;

			sql.AppendLine($") as [T0]");
			sql.AppendLine($" WHERE [T0].[ROW_NUMBER] BETWEEN {start} AND {start + count - 1}");
			sql.AppendLine($" ORDER BY [T0].[ROW_NUMBER]");

			return sql.ToString();
		}

		private string BuildDistinctNonPagedCollection(Type entityType, RqlNode node, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node.Find(RqlOperation.SELECT);

			var orderByClause = ParseOrderByClause(entityType, node);
			bool firstField = true;

			sql.Append("SELECT ");

			if (node?.Find(RqlOperation.FIRST) != null)
			{
				sql.Append("TOP 1 ");
			}

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields, false))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField, "T0");
				}
			}

			sql.Append(" FROM (SELECT DISTINCT ");
			firstField = true;

			foreach (var property in properties)
			{
				if (RqlUtilities.CheckForInclusion(property, selectFields, false))
				{
					AppendPropertyForRead(sql, tableAttribute, property, ref firstField);
				}
			}

			AppendFromClause(sql, tableAttribute);
			AppendWhereClause(sql, whereClause);
			AppendOrderByClause(sql, orderByClause);

			sql.Append(") AS T0");

			return sql.ToString();
		}

		private string BuildValuesCollection(Type entityType, RqlNode node, int count, int batchLimit, List<SqlParameter> parameters, bool NoPaging)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			if (node.Find(RqlOperation.FIRST) != null)
				throw new InvalidCastException($"A FIRST clause is not compatible with a VALUES clause.");

			RqlNode? pageFilter = node.ExtractLimitClause();

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node.Find(RqlOperation.SELECT);

			var orderByClause = ParseOrderByClause(entityType, node);
			var valuesNode = node.Find(RqlOperation.VALUES);

			if (valuesNode != null)
			{
				if (NoPaging || (count < batchLimit && pageFilter == null))
				{
					sql.Append("select distinct ");
					bool firstMember = true;

					var propertyNode = valuesNode.NonNullValue<RqlNode>(0);
					var property = properties.FirstOrDefault(p => string.Equals(propertyNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
					AppendProperty(sql, tableAttribute, property, ref firstMember);

					AppendFromClause(sql, tableAttribute);
					AppendWhereClause(sql, whereClause);
					AppendOrderByClause(sql, orderByClause);
				}
				else
				{
					sql.Append("SELECT ");

					PropertyInfo property;

					var childNode = valuesNode.NonNullValue<RqlNode>(0);

					if (childNode.Operation == RqlOperation.PROPERTY)
					{
						property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
					}
					else
					{
						var propertyNode = childNode.NonNullValue<RqlNode>(0);
						property = properties.FirstOrDefault(p => string.Equals(propertyNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
					}

					sql.Append($"[t0].[{property.Name}]");

					sql.AppendLine();
					sql.AppendLine("  FROM (");
					sql.AppendLine("         SELECT ROW_NUMBER() OVER ( ORDER BY ");

					if (string.IsNullOrWhiteSpace(orderByClause))
					{
						var firstField = true;
						if (RqlUtilities.CheckForInclusion(property, selectFields, false))
						{
							AppendPropertyForRead(sql, tableAttribute, property, ref firstField, "T1");
						}
					}
					else
					{
						sql.Append(orderByClause);
					}

					sql.AppendLine(") as [ROW_NUMBER],");

					bool firstMember = true;
					property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
					AppendProperty(sql, tableAttribute, property, ref firstMember);

					AppendFromClause(sql, tableAttribute);
					AppendWhereClause(sql, whereClause);

					int start = 1;

					if (pageFilter != null)
					{
						start = pageFilter.NonNullValue<int>(0);
						count = pageFilter.NonNullValue<int>(1);
					}

					if (count > _batchLimit)
						count = _batchLimit;

					sql.AppendLine($") as [t0]");
					sql.AppendLine($" WHERE [t0].[ROW_NUMBER] BETWEEN {start} AND {start + count - 1}");
					sql.AppendLine($" ORDER BY [t0].[ROW_NUMBER]");
				}
			}

			return sql.ToString();
		}

		private string BuildColumnAggregateCollection(Type entityType, RqlNode node, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node?.Find(RqlOperation.SELECT);

			if (selectFields != null)
			{
				foreach (var property in properties)
				{
					if (RqlUtilities.CheckForInclusion(property, selectFields, false))
					{
						if (!IncludedInAggregationList(node, property.Name))
							throw new InvalidCastException($"{property.Name} is not included in an aggregation function");
					}
				}
			}

			sql.Append("SELECT ");
			bool first = true;
			BuildSimpleAggregate(entityType, node, sql, properties, tableAttribute, ref first);

			AppendFromClause(sql, tableAttribute);
			AppendWhereClause(sql, whereClause);

			return sql.ToString();
		}

		private string BuildAggregatePagedCollection(Type entityType, RqlNode node, int count, RqlNode? pageFilter, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var properties = entityType.GetProperties();
			var sql = new StringBuilder();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node.Find(RqlOperation.SELECT);
			var orderByClause = ParseOrderByClause(entityType, node);

			sql.Append("SELECT ");

			bool firstMember = true;

			if (node?.Find(RqlOperation.AGGREGATE) is RqlNode aggregateNode)
			{
				foreach (RqlNode childNode in aggregateNode)
				{
					PropertyInfo property;
					MemberAttribute memberAttribute;

					if (childNode.Operation == RqlOperation.PROPERTY)
					{
						property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						memberAttribute = property.GetCustomAttribute<MemberAttribute>();
					}
					else
					{
						var propertyNode = childNode.NonNullValue<RqlNode>(0);
						property = properties.FirstOrDefault(p => string.Equals(propertyNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						memberAttribute = property.GetCustomAttribute<MemberAttribute>();
					}

					if (memberAttribute != null)
					{
						if (firstMember)
							firstMember = false;
						else
							sql.Append(", ");

						sql.Append($"[t0].[{property.Name}]");
					}
				}

				sql.AppendLine();
				sql.AppendLine("  FROM (");
				sql.AppendLine("         SELECT ROW_NUMBER() OVER (");

				if (string.IsNullOrWhiteSpace(orderByClause))
				{
					var primaryKeyProperties = properties.Where(x => x.GetCustomAttribute<MemberAttribute>() != null && x.GetCustomAttribute<MemberAttribute>().IsPrimaryKey);

					if (primaryKeyProperties.Count() > 1)
					{
						sql.Append(" ORDER BY ");

						bool firstComponent = true;
						foreach (var composite in primaryKeyProperties)
						{
							var tableName = tableAttribute.Name;
							var schema = tableAttribute.Schema;
							var memberAttribute = composite.GetCustomAttribute<MemberAttribute>();

							if (memberAttribute != null)
							{
								var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? composite.Name : memberAttribute.ColumnName;

								if (firstComponent)
								{
									if (string.IsNullOrWhiteSpace(schema))
										sql.Append($"MAX([{tableName}].[{columnName}])");
									else
										sql.Append($"MAX([{schema}].[{tableName}].[{columnName}])");
									firstComponent = false;
								}
								else
								{
									if (string.IsNullOrWhiteSpace(schema))
										sql.Append($", MAX([{tableName}].[{columnName}])");
									else
										sql.Append($", MAX([{schema}].[{tableName}].[{columnName}])");
								}
							}
						}
					}
					else if (primaryKeyProperties.Any())
					{
						var primaryKeyProperty = properties.FirstOrDefault(x => x.GetCustomAttribute<MemberAttribute>() != null && x.GetCustomAttribute<MemberAttribute>().IsPrimaryKey);

						if (primaryKeyProperty != null)
						{
							var tableName =
								string.IsNullOrWhiteSpace(primaryKeyProperty.GetCustomAttribute<MemberAttribute>().TableName) ? tableAttribute.Name : primaryKeyProperty.GetCustomAttribute<MemberAttribute>().TableName;

							var schema =
								string.IsNullOrWhiteSpace(primaryKeyProperty.GetCustomAttribute<MemberAttribute>().Schema) ? tableAttribute.Schema : primaryKeyProperty.GetCustomAttribute<MemberAttribute>().Schema;

							var columnName = string.IsNullOrWhiteSpace(primaryKeyProperty.GetCustomAttribute<MemberAttribute>().ColumnName) ? primaryKeyProperty.Name : primaryKeyProperty.GetCustomAttribute<MemberAttribute>().ColumnName;

							if (string.IsNullOrWhiteSpace(schema))
								sql.Append($" ORDER BY MAX([{tableName}].[{columnName}]) asc ");
							else
								sql.Append($" ORDER BY MAX([{schema}].[{tableName}].[{columnName}]) asc ");
						}
					}
				}
				else
				{
					sql.Append(" ORDER BY ");
					sql.Append(orderByClause);
				}

				sql.AppendLine(") as [ROW_NUMBER],");
				firstMember = true;

				foreach (RqlNode childNode in aggregateNode)
				{
					if (childNode.Operation == RqlOperation.PROPERTY)
					{
						var property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						AppendProperty(sql, tableAttribute, property, ref firstMember);
					}
					else
					{
						var operation = string.Empty;
						switch (childNode.Operation)
						{
							case RqlOperation.MAX:
								operation = "max";
								break;

							case RqlOperation.MIN:
								operation = "min";
								break;

							case RqlOperation.MEAN:
								operation = "avg";
								break;

							case RqlOperation.SUM:
								operation = "sum";
								break;

							case RqlOperation.COUNT:
								operation = "count";
								break;
						}
						if (firstMember)
							firstMember = false;
						else
							sql.Append(", ");

						var propertyNode = (RqlNode)childNode[0];
						var property = properties.FirstOrDefault(p => string.Equals(propertyNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

						if (memberAttribute != null)
						{
							var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
							var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
							var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

							if (string.IsNullOrWhiteSpace(schema))
							{
								sql.Append($"{operation}([{tableName}].[{columnName}]) as {propertyNode.Value<string>(0)}");
							}
							else
							{
								sql.Append($"{operation}([{schema}].[{tableName}].[{columnName}]) as {propertyNode.Value<string>(0)}");
							}
						}
					}
				}

				AppendFromClause(sql, tableAttribute);
				AppendWhereClause(sql, whereClause);
				AppendGroupByClause(sql, aggregateNode, tableAttribute, properties);

				int start = 1;

				if (pageFilter != null)
				{
					start = pageFilter.NonNullValue<int>(0);
					count = pageFilter.NonNullValue<int>(1);
				}

				if (count > _batchLimit)
					count = _batchLimit;

				sql.AppendLine($") as [t0]");
				sql.AppendLine($" WHERE [t0].[ROW_NUMBER] BETWEEN {start} AND {start + count - 1}");
				sql.AppendLine($" ORDER BY [t0].[ROW_NUMBER]");
			}

			return sql.ToString();
		}

		private string BuildAggregateNonPagedCollection(Type entityType, RqlNode node, List<SqlParameter> parameters)
		{
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var sql = new StringBuilder();
			var properties = entityType.GetProperties();
			var whereClause = ParseWhereClause(entityType, node, null, tableAttribute, properties, parameters);
			var selectFields = node.Find(RqlOperation.SELECT);
			var orderByClause = ParseOrderByClause(entityType, node);

			sql.Append("select ");
			bool firstMember = true;

			if (node?.Find(RqlOperation.AGGREGATE) is RqlNode aggregateNode)
			{
				foreach (RqlNode childNode in aggregateNode)
				{
					if (childNode.Operation == RqlOperation.PROPERTY)
					{
						var property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						AppendPropertyForRead(sql, tableAttribute, property, ref firstMember);
					}
					else
					{
						var operation = string.Empty;

						operation = childNode.Operation switch
						{
							RqlOperation.MAX => "MAX",
							RqlOperation.MIN => "MIN",
							RqlOperation.MEAN => "AVG",
							RqlOperation.COUNT => "COUNT",
							RqlOperation.SUM => "SUM",
							_ => throw new InvalidCastException($"Invalid operation {childNode.Operation}"),
						};
						if (firstMember)
							firstMember = false;
						else
							sql.Append(", ");

						var propertyNode = (RqlNode)childNode[0];
						var property = properties.FirstOrDefault(p => string.Equals(propertyNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
						var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

						if (memberAttribute != null)
						{
							var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
							var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
							var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

							if (string.IsNullOrWhiteSpace(schema))
							{
								sql.Append($"{operation}([{tableName}].[{columnName}]) as {property.Name}");
							}
							else
							{
								sql.Append($"{operation}([{schema}].[{tableName}].[{columnName}]) as {property.Name}");
							}
						}
					}
				}

				AppendFromClause(sql, tableAttribute);
				AppendWhereClause(sql, whereClause);
				AppendGroupByClause(sql, aggregateNode, tableAttribute, properties);
				AppendOrderByClause(sql, orderByClause);
			}

			return sql.ToString();
		}

		#region Helper Functions
		/// <summary>
		/// Returns a formatted WHERE clause representation of the filters in the query string
		/// </summary>
		/// <param name="node">The RQL node representation of the query string</param>
		/// <param name="op"></param>
		/// <param name="tableAttribute">The table attributes associated with type T</param>
		/// <param name="properties">The list of properties of type T</param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		private string ParseWhereClause<T>(RqlNode node, string? op, Table tableAttribute, IEnumerable<PropertyInfo> properties, List<SqlParameter> parameters)
        {
			return ParseWhereClause(typeof(T), node, op, tableAttribute, properties, parameters);
        }

		/// <summary>
		/// Returns a formatted WHERE clause representation of the filters in the query string
		/// </summary>
		/// <param name="entityType">The type of entity</param>
		/// <param name="node">The RQL node representation of the query string</param>
		/// <param name="op"></param>
		/// <param name="tableAttribute">The table attributes associated with type T</param>
		/// <param name="properties">The list of properties of type T</param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		private string ParseWhereClause(Type entityType, RqlNode node, string? op, Table tableAttribute, IEnumerable<PropertyInfo> properties, List<SqlParameter> parameters)
		{
			if (node.Operation == RqlOperation.NOOP)
				return string.Empty;

			var whereClause = new StringBuilder();

			switch (node.Operation)
			{
				case RqlOperation.AND:
					{
						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append('(');

						bool first = true;

						foreach (RqlNode argument in node)
						{
							var subClause = ParseWhereClause(entityType, argument, "AND", tableAttribute, properties, parameters);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (first)
									first = false;
								else
									whereClause.Append(" AND ");

								whereClause.Append(subClause);
							}
						}

						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append(')');
					}
					break;

				case RqlOperation.OR:
					{
						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append('(');

						bool first = true;

						foreach (RqlNode argument in node)
						{
							var subClause = ParseWhereClause(entityType, argument, "OR", tableAttribute, properties, parameters);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (first)
									first = false;
								else
									whereClause.Append(" OR ");

								whereClause.Append(subClause);
							}
						}

						if (!string.IsNullOrWhiteSpace(op))
							whereClause.Append(')');
					}
					break;

				case RqlOperation.GE:
					whereClause.Append(ConstructComparrisonOperator(entityType, ">=", node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<object>(1), parameters));
					break;

				case RqlOperation.GT:
					whereClause.Append(ConstructComparrisonOperator(entityType, ">", node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<object>(1), parameters));
					break;

				case RqlOperation.LE:
					whereClause.Append(ConstructComparrisonOperator(entityType, "<=", node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<object>(1), parameters));
					break;

				case RqlOperation.LT:
					whereClause.Append(ConstructComparrisonOperator(entityType, "<", node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<object>(1), parameters));
					break;

				case RqlOperation.EQ:
					whereClause.Append(ConstructComparrisonOperator(entityType, "=", node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<object>(1), parameters));
					break;

				case RqlOperation.NE:
					whereClause.Append(ConstructComparrisonOperator(entityType, "<>", node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<object>(1), parameters));
					break;

				case RqlOperation.IN:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.NonNullValue<RqlNode>(0).Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"{OB}{tableName}{CB}.{OB}{property.Name}{CB} IN(");
								else
									whereClause.Append($"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} IN(");

								for (int i = 1; i < node.Count; i++)
								{
									if (i > 1)
										whereClause.Append(',');

									var parameterName = $"@P{parameters.Count}";
									parameters.Add(BuildSqlParameter(parameterName, property, node.Value<object>(i)));
									whereClause.Append(parameterName);
								}

								whereClause.Append(')');
							}
							else
							{
								throw new InvalidCastException($"{property.Name} is not a member of {entityType.Name}");
							}
						}
						else
						{
							throw new InvalidCastException($"{property?.Name} is not a member of {entityType.Name}");
						}
					}
					break;

				case RqlOperation.OUT:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.NonNullValue<RqlNode>(0).Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"{OB}{tableName}{CB}.{OB}{property.Name}{CB} NOT IN(");
								else
									whereClause.Append($"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} NOT IN(");

								for (int i = 1; i < node.Count; i++)
								{
									if (i > 1)
										whereClause.Append(',');

									var parameterName = $"@P{parameters.Count}";
									parameters.Add(BuildSqlParameter(parameterName, property, node.Value<object>(i)));
									whereClause.Append(parameterName);
								}

								whereClause.Append(')');

							}
							else
							{
								throw new InvalidCastException($"{property.Name} is not a member of {entityType.Name}");
							}
						}
						else
						{
							throw new InvalidCastException($"{property?.Name} is not a member of {entityType.Name}");
						}
					}
					break;

				case RqlOperation.CONTAINS:
				case RqlOperation.LIKE:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.NonNullValue<RqlNode>(0).Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"{OB}{tableName}{CB}.{OB}{property.Name}{CB} LIKE (");
								else
									whereClause.Append($"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} LIKE (");

								var filter = node.NonNullValue<string>(1);
								filter = filter.Replace("*", "%").Replace("?", "_");

								var parameterName = $"@P{parameters.Count}";
								parameters.Add(BuildSqlParameter(parameterName, property, filter));
								whereClause.Append(parameterName);

								whereClause.Append(')');
							}
							else
							{
								throw new InvalidCastException($"{property.Name} is not a member of {entityType.Name}");
							}
						}
						else
						{
							throw new InvalidCastException($"{property?.Name} is not a member of {entityType.Name}");
						}
					}
					break;

				case RqlOperation.EXCLUDES:
					{
						var property = properties.FirstOrDefault(x => string.Equals(x.Name, node.NonNullValue<RqlNode>(0).Value<string>(0), StringComparison.OrdinalIgnoreCase));

						if (property != null)
						{
							var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

							if (memberAttribute != null)
							{
								var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
								var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;

								if (string.IsNullOrWhiteSpace(schema))
									whereClause.Append($"{OB}{tableName}{CB}.{OB}{property.Name}{CB} NOT LIKE (");
								else
									whereClause.Append($"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} NOT LIKE (");

								var filter = node.NonNullValue<string>(1);
								filter = filter.Replace("*", "%").Replace("?", "_");

								var parameterName = $"@P{parameters.Count}";
								parameters.Add(BuildSqlParameter(parameterName, property, filter));
								whereClause.Append(parameterName);

								whereClause.Append(')');
							}
							else
							{
								throw new InvalidCastException($"{property.Name} is not a member of {entityType.Name}");
							}
						}
						else
						{
							throw new InvalidCastException($"{property?.Name} is not a member of {entityType.Name}");
						}
					}
					break;
			}

			return whereClause.ToString();
		}

		private string ConstructComparrisonOperator(Type nodeType, string operation, string attributeName, object attributeValue, List<SqlParameter> parameters)
		{
			var tableAttribute = nodeType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {nodeType.Name} is not an entity model.");

			var property = nodeType.GetProperties().FirstOrDefault(x => x.Name.ToLower() == attributeName.ToLower());

			if (property != null)
			{
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

				if (memberAttribute != null)
				{
					var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
					var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
					var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

					try
					{
						if (attributeValue == null)
						{
							if (string.Compare(operation, "<>", true) == 0)
							{
								if (string.IsNullOrWhiteSpace(schema))
									return $"{OB}{tableName}{CB}.{OB}{columnName}{CB} is not null";
								else
									return $"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{columnName}{CB} is not null";
							}
							else
							{
								if (string.IsNullOrWhiteSpace(schema))
									return $"{OB}{tableName}{CB}.{OB}{property.Name}{CB} is null";
								else
									return $"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} is null";
							}
						}
						else
						{
							var parameterName = $"@P{parameters.Count}";
							if (string.Equals(memberAttribute.NativeDataType, "hierarchyid", StringComparison.OrdinalIgnoreCase))
							{
								var theValue = attributeValue.ToString().Replace("-", "/");
								parameters.Add(BuildSqlParameter(parameterName, property, theValue));
							}
							else
							{
								parameters.Add(BuildSqlParameter(parameterName, property, attributeValue));
							}

							if (string.IsNullOrWhiteSpace(schema))
								return $"{OB}{tableName}{CB}.{OB}{property.Name}{CB} {operation} {parameterName}";
							else
								return $"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} {operation} {parameterName}";
						}
					}
					catch (FormatException error)
					{
						throw new InvalidCastException("Unknown exception", error);
					}
				}
				else
				{
					throw new InvalidCastException($"{property.Name} is not a member of {nodeType.Name}");
				}
			}
			else
			{
				throw new InvalidCastException($"Malformed RQL query: {attributeName} is not a member of {nodeType.Name}.");
			}
		}

		/// <summary>
		/// Returns a formatted ORDER BY clause representation of the sort operation in the query string
		/// </summary>
		/// <param name="entityType">The type of object</param>
		/// <param name="node">The RQL node representation of the query string</param>
		/// <param name="domain">Replace schema.tablename with this value of present</param>
		/// <returns></returns>
		private string ParseOrderByClause(Type entityType, RqlNode node, string domain = "")
		{
			if (node.Operation == RqlOperation.NOOP)
				return string.Empty;
			
			var tableAttribute = entityType.GetCustomAttribute<Table>(false);

			if (tableAttribute == null)
				throw new InvalidCastException($"The class {entityType.Name} is not an entity model.");

			var orderByClause = new StringBuilder();

			switch (node.Operation)
			{
				case RqlOperation.AND:
					{
						foreach (RqlNode argument in node)
						{
							var subClause = ParseOrderByClause(entityType, argument, domain);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (orderByClause.Length > 0)
									orderByClause.Append(", ");

								orderByClause.Append(subClause);
							}
						}
					}
					break;

				case RqlOperation.OR:
					{
						foreach (RqlNode argument in node)
						{
							var subClause = ParseOrderByClause(entityType, argument, domain);

							if (!string.IsNullOrWhiteSpace(subClause))
							{
								if (orderByClause.Length > 0)
									orderByClause.Append(", ");

								orderByClause.Append(subClause);
							}
						}
					}
					break;

				case RqlOperation.SORT:
					{
						foreach (RqlNode argument in node)
						{
							var fieldName = argument.NonNullValue<RqlNode>(1).Value<string>(0);
							var property = entityType.GetProperties().FirstOrDefault(p => string.Equals(p.Name, fieldName, StringComparison.OrdinalIgnoreCase));

							if (property != null)
							{
								var memberAttribute = property.GetCustomAttribute<MemberAttribute>(false);

								if (memberAttribute != null)
								{
									var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
									var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
									var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

									if (orderByClause.Length > 0)
										orderByClause.Append(", ");

									if (argument.NonNullValue<RqlSortOrder>(0) == RqlSortOrder.Descending)
									{
										if (string.IsNullOrWhiteSpace(domain))
										{
											if (string.IsNullOrWhiteSpace(schema))
												orderByClause.Append($"{OB}{tableName}{CB}.{OB}{property.Name}{CB} desc");
											else
												orderByClause.Append($"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB} desc");
										}
										else
										{
											orderByClause.Append($"{OB}{domain}{CB}.{OB}{property.Name}{CB} desc");
										}
									}
									else
									{
										if (string.IsNullOrWhiteSpace(domain))
										{
											if (string.IsNullOrWhiteSpace(schema))
												orderByClause.Append($"{OB}{tableName}{CB}.{OB}{property.Name}{CB}");
											else
												orderByClause.Append($"{OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{property.Name}{CB}");
										}
										else
											orderByClause.Append($"{OB}{domain}{CB}.{OB}{property.Name}{CB}");
									}
								}
								else
									throw new InvalidCastException($"{property.Name} is not a member of {entityType.Name}");
							}
							else
								throw new InvalidCastException($"{property?.Name} is not a member of {entityType.Name}");
						}
					}
					break;
			}

			return orderByClause.ToString();
		}

		internal static SqlParameter BuildSqlParameter(string parameterName, PropertyInfo property, object? value)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

			if (string.Equals(memberAttribute.NativeDataType, "bigint", StringComparison.OrdinalIgnoreCase))
			{
				return BuildLongParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "smallint", StringComparison.OrdinalIgnoreCase))
			{
				return BuildShortParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "bit", StringComparison.OrdinalIgnoreCase))
			{
				return BuildBoolParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "char", StringComparison.OrdinalIgnoreCase))
			{
				return BuildCharParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "decimal", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDecimalParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "money", StringComparison.OrdinalIgnoreCase))
			{
				return BuildMoneyParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "smallmoney", StringComparison.OrdinalIgnoreCase))
			{
				return BuildSmallMoneyParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "numeric", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDecimalParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "float", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDoubleParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "real", StringComparison.OrdinalIgnoreCase))
			{
				return BuildSingleParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "hierarchyid", StringComparison.OrdinalIgnoreCase))
			{
				return BuildHierarchyIdParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "int", StringComparison.OrdinalIgnoreCase))
			{
				return BuildIntParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "nchar", StringComparison.OrdinalIgnoreCase))
			{
				return BuildNCharParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "ntext", StringComparison.OrdinalIgnoreCase))
			{
				return BuildNTextParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "nvarchar", StringComparison.OrdinalIgnoreCase))
			{
				return BuildNVarcharParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "text", StringComparison.OrdinalIgnoreCase))
			{
				return BuildTextParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "tinyint", StringComparison.OrdinalIgnoreCase))
			{
				return BuildTinyParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "varchar", StringComparison.OrdinalIgnoreCase))
			{
				return BuildVarcharParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "date", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDateParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "datetime", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDateTimeParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "datetime2", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDateTime2Parameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "smalldatetime", StringComparison.OrdinalIgnoreCase))
			{
				return BuildSmallDateTimeParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "datetimeoffset", StringComparison.OrdinalIgnoreCase))
			{
				return BuildDateTimeOffsetParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "uniqueidentifier", StringComparison.OrdinalIgnoreCase))
			{
				return BuildGuidParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "binary", StringComparison.OrdinalIgnoreCase))
			{
				return BuildBinaryParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "varbinary", StringComparison.OrdinalIgnoreCase))
			{
				return BuildVarBinaryParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "image", StringComparison.OrdinalIgnoreCase))
			{
				return BuildImageParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "time", StringComparison.OrdinalIgnoreCase))
			{
				return BuildTimeParameter(parameterName, memberAttribute, value);
			}
			else if (string.Equals(memberAttribute.NativeDataType, "xml", StringComparison.OrdinalIgnoreCase))
			{
				return BuildXmlParameter(parameterName, memberAttribute, value);
			}
			else
			{
				throw new InvalidCastException($"Unsupported data type {memberAttribute.NativeDataType}");
			}
		}

		internal static SqlParameter BuildXmlParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			var nLength = memberAttribute.Length > 0 ? memberAttribute.Length : -1;

			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Xml, nLength) { Value = DBNull.Value };
				else
				{
					var document = new XmlDocument();

					var decl = document.CreateXmlDeclaration("1.0", "utf-8", "yes");
					string s;

					XmlElement root = document.DocumentElement;
					document.InsertBefore(decl, root);

					using (var stringWriter = new StringWriter())
					using (var xmlTextWriter = XmlWriter.Create(stringWriter))
					{
						document.WriteTo(xmlTextWriter);
						xmlTextWriter.Flush();
						s = stringWriter.GetStringBuilder().ToString();
					}
					return new SqlParameter(parameterName, SqlDbType.Xml, nLength) { Value = s };
				}
			}
			else if (value.GetType() == typeof(XmlDocument))
			{
				return new SqlParameter(parameterName, SqlDbType.Xml, nLength) { Value = ((XmlDocument)value).ToString() };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.Xml, nLength) { Value = (string)value };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to Xml");
			}
		}

		internal static SqlParameter BuildTimeParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			var nLength = memberAttribute.Length > 0 ? memberAttribute.Length : -1;

			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Time, nLength) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Time, nLength) { Value = TimeSpan.FromSeconds(0) };
			}
			else if (value.GetType() == typeof(TimeSpan) || value.GetType() == typeof(TimeSpan?))
			{
				return new SqlParameter(parameterName, SqlDbType.Time, nLength) { Value = value };
			}
			else if (value.GetType() == typeof(string))
			{
				var bval = TimeSpan.Parse((string)value);
				return new SqlParameter(parameterName, SqlDbType.Time, nLength) { Value = bval };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to TimeSpan");
			}
		}

		internal static SqlParameter BuildImageParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			var nLength = memberAttribute.Length > 0 ? memberAttribute.Length : -1;

			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.VarBinary, nLength) { Value = DBNull.Value };
				else
					throw new InvalidCastException("Cannot set non-nullable member to null");
			}
			else if (value.GetType() == typeof(byte[]))
			{
				var bval = (byte[])value;
				return new SqlParameter(parameterName, SqlDbType.VarBinary, bval.Length) { Value = bval };
			}
			else if (value.GetType() == typeof(string))
			{
				var bval = Convert.FromBase64String((string)value);
				return new SqlParameter(parameterName, SqlDbType.VarBinary, bval.Length) { Value = bval };
			}
#if windows
			else if (value.GetType() == typeof(Image))
			{
				byte[] bval = ((Image)value).GetBytes();
				return new SqlParameter(parameterName, SqlDbType.VarBinary, bval.Length) { Value = bval };
			}
			else if (value.GetType() == typeof(Bitmap))
			{
				byte[] bval = ((Image)value).GetBytes();
				return new SqlParameter(parameterName, SqlDbType.VarBinary, bval.Length) { Value = bval };
			}
#endif
			else
			{
				throw new InvalidCastException("Cannot cast value to Image");
			}
		}

		internal static SqlParameter BuildVarBinaryParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			var nLength = memberAttribute.Length > 0 ? memberAttribute.Length : -1;

			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.VarBinary, nLength) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.VarBinary, 0) { Value = new List<byte>().ToArray() };
			}
			else if (value.GetType() == typeof(byte[]))
			{
				var bValue = (byte[])value;

				if (bValue.Length == 0)
					if (memberAttribute.IsNullable)
						return new SqlParameter(parameterName, SqlDbType.Binary, nLength) { Value = DBNull.Value };

				return new SqlParameter(parameterName, SqlDbType.Binary, bValue.Length) { Value = value };
			}
			else if (value.GetType() == typeof(string))
			{
				if (string.IsNullOrWhiteSpace(value.ToString()))
				{
					if (memberAttribute.IsNullable)
						return new SqlParameter(parameterName, SqlDbType.Binary, nLength) { Value = DBNull.Value };
					else
						return new SqlParameter(parameterName, SqlDbType.Binary, 0) { Value = new List<byte>().ToArray() };
				}
				else
				{
					var bval = Convert.FromBase64String((string)value);
					return new SqlParameter(parameterName, SqlDbType.Binary, bval.Length) { Value = bval };
				}
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to bytes");
			}
		}

		internal static SqlParameter BuildBinaryParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Binary, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Binary, 0) { Value = new List<byte>().ToArray() };
			}
			else if (value.GetType() == typeof(byte[]))
			{
				var bValue = (byte[])value;

				if (bValue.Length == 0)
					if (memberAttribute.IsNullable)
						return new SqlParameter(parameterName, SqlDbType.Binary, memberAttribute.Length) { Value = DBNull.Value };

				return new SqlParameter(parameterName, SqlDbType.Binary, bValue.Length) { Value = value };
			}
			else if (value.GetType() == typeof(string))
			{
				if (string.IsNullOrWhiteSpace(value.ToString()))
				{
					if (memberAttribute.IsNullable)
						return new SqlParameter(parameterName, SqlDbType.Binary, memberAttribute.Length) { Value = DBNull.Value };
					else
						return new SqlParameter(parameterName, SqlDbType.Binary, 0) { Value = new List<byte>().ToArray() };
				}
				else
				{
					var bval = Convert.FromBase64String((string)value);
					return new SqlParameter(parameterName, SqlDbType.Binary, bval.Length) { Value = bval };
				}
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to bytes");
			}
		}

		internal static SqlParameter BuildDateParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Date, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Date, memberAttribute.Length) { Value = DateTime.MinValue.Date };
			}
			else if (value.GetType() == typeof(DateTime) || value.GetType() == typeof(DateTime?))
			{
				return new SqlParameter(parameterName, SqlDbType.Date, memberAttribute.Length) { Value = ((DateTime)value).Date };
			}
			else if (value.GetType() == typeof(DateTimeOffset) || value.GetType() == typeof(DateTimeOffset?))
			{
				return new SqlParameter(parameterName, SqlDbType.Date, memberAttribute.Length) { Value = ((DateTimeOffset)value).Date };
			}
			else if (value.GetType() == typeof(string))
			{
				var dateValue = DateTimeOffset.Parse(value.ToString());
				return new SqlParameter(parameterName, SqlDbType.Date, memberAttribute.Length) { Value = dateValue.Date };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to date");
			}
		}

		internal static SqlParameter BuildDateTimeParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.DateTime, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.DateTime, memberAttribute.Length) { Value = DateTime.MinValue };
			}
			else if (value.GetType() == typeof(DateTime) || value.GetType() == typeof(DateTime?))
			{
				return new SqlParameter(parameterName, SqlDbType.DateTime, memberAttribute.Length) { Value = (DateTime)value };
			}
			else if (value.GetType() == typeof(DateTimeOffset) || value.GetType() == typeof(DateTimeOffset?))
			{
				return new SqlParameter(parameterName, SqlDbType.DateTime, memberAttribute.Length) { Value = (DateTimeOffset)value };
			}
			else if (value.GetType() == typeof(string))
			{
				var dateValue = DateTime.Parse(value.ToString());
				return new SqlParameter(parameterName, SqlDbType.DateTime, memberAttribute.Length) { Value = dateValue };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to DateTime");
			}
		}

		internal static SqlParameter BuildDateTimeOffsetParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.DateTimeOffset, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.DateTimeOffset, memberAttribute.Length) { Value = DateTimeOffset.MinValue };
			}
			else if (value.GetType() == typeof(DateTime) || value.GetType() == typeof(DateTime?))
			{
				return new SqlParameter(parameterName, SqlDbType.DateTimeOffset, memberAttribute.Length) { Value = (DateTime)value };
			}
			else if (value.GetType() == typeof(DateTimeOffset) || value.GetType() == typeof(DateTimeOffset?))
			{
				return new SqlParameter(parameterName, SqlDbType.DateTimeOffset, memberAttribute.Length) { Value = (DateTimeOffset)value };
			}
			else if (value.GetType() == typeof(string))
			{
				var dateValue = DateTime.Parse(value.ToString());
				return new SqlParameter(parameterName, SqlDbType.DateTimeOffset, memberAttribute.Length) { Value = dateValue };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to DateTime");
			}
		}

		internal static SqlParameter BuildDateTime2Parameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.DateTime2, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.DateTime2, memberAttribute.Length) { Value = DateTime.MinValue };
			}
			else if (value.GetType() == typeof(DateTime) || value.GetType() == typeof(DateTime?))
			{
				return new SqlParameter(parameterName, SqlDbType.DateTime2, memberAttribute.Length) { Value = (DateTime)value };
			}
			else if (value.GetType() == typeof(DateTimeOffset) || value.GetType() == typeof(DateTimeOffset?))
			{
				return new SqlParameter(parameterName, SqlDbType.DateTime2, memberAttribute.Length) { Value = (DateTimeOffset)value };
			}
			else if (value.GetType() == typeof(string))
			{
				var dateValue = DateTime.Parse(value.ToString());
				return new SqlParameter(parameterName, SqlDbType.DateTime2, memberAttribute.Length) { Value = dateValue };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to DateTime");
			}
		}

		internal static SqlParameter BuildSmallDateTimeParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.SmallDateTime, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.SmallDateTime, memberAttribute.Length) { Value = DateTime.MinValue };
			}
			else if (value.GetType() == typeof(DateTime) || value.GetType() == typeof(DateTime?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallDateTime, memberAttribute.Length) { Value = (DateTime)value };
			}
			else if (value.GetType() == typeof(DateTimeOffset) || value.GetType() == typeof(DateTimeOffset?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallDateTime, memberAttribute.Length) { Value = (DateTimeOffset)value };
			}
			else if (value.GetType() == typeof(string))
			{
				var dateValue = DateTime.Parse(value.ToString());
				return new SqlParameter(parameterName, SqlDbType.SmallDateTime, memberAttribute.Length) { Value = dateValue };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to DateTime");
			}
		}

		internal static SqlParameter BuildBoolParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(bool) || value.GetType() == typeof(bool?))
			{
				return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = ((bool)value) ? 1 : 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = ((sbyte)value) == 0 ? 0 : 1 };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = ((short)value) == 0 ? 0 : 1 };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = ((int)value) == 0 ? 0 : 1 };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.Bit, memberAttribute.Length) { Value = ((long)value) == 0 ? 0 : 1 };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to boolean");
			}
		}

		internal static SqlParameter BuildShortParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.SmallInt, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.SmallInt, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallInt, memberAttribute.Length) { Value = Convert.ToInt16(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallInt, memberAttribute.Length) { Value = Convert.ToInt16(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallInt, memberAttribute.Length) { Value = Convert.ToInt16(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallInt, memberAttribute.Length) { Value = Convert.ToInt16(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to short");
			}
		}

		internal static SqlParameter BuildTinyParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.TinyInt, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.TinyInt, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.TinyInt, memberAttribute.Length) { Value = Convert.ToByte(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.TinyInt, memberAttribute.Length) { Value = Convert.ToByte(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.TinyInt, memberAttribute.Length) { Value = Convert.ToByte(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.TinyInt, memberAttribute.Length) { Value = Convert.ToByte(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to short");
			}
		}

		internal static SqlParameter BuildDecimalParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = DBNull.Value, Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
				else
					return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = 0, Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(float) || value.GetType() == typeof(float?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(double) || value.GetType() == typeof(double?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else if (value.GetType() == typeof(decimal) || value.GetType() == typeof(decimal?))
			{
				return new SqlParameter(parameterName, SqlDbType.Decimal, memberAttribute.Length) { Value = Convert.ToDecimal(value), Precision = (byte)memberAttribute.Precision, Scale = (byte)memberAttribute.Scale };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to decimal");
			}
		}

		internal static SqlParameter BuildMoneyParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(float) || value.GetType() == typeof(float?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(double) || value.GetType() == typeof(double?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(decimal) || value.GetType() == typeof(decimal?))
			{
				return new SqlParameter(parameterName, SqlDbType.Money, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to short");
			}
		}

		internal static SqlParameter BuildSmallMoneyParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(float) || value.GetType() == typeof(float?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(double) || value.GetType() == typeof(double?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else if (value.GetType() == typeof(decimal) || value.GetType() == typeof(decimal?))
			{
				return new SqlParameter(parameterName, SqlDbType.SmallMoney, memberAttribute.Length) { Value = Convert.ToDecimal(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to short");
			}
		}

		internal static SqlParameter BuildDoubleParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else if (value.GetType() == typeof(float) || value.GetType() == typeof(float?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else if (value.GetType() == typeof(double) || value.GetType() == typeof(double?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else if (value.GetType() == typeof(decimal) || value.GetType() == typeof(decimal?))
			{
				return new SqlParameter(parameterName, SqlDbType.Float, memberAttribute.Length) { Value = Convert.ToDouble(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to short");
			}
		}

		internal static SqlParameter BuildSingleParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else if (value.GetType() == typeof(float) || value.GetType() == typeof(float?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else if (value.GetType() == typeof(double) || value.GetType() == typeof(double?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else if (value.GetType() == typeof(decimal) || value.GetType() == typeof(decimal?))
			{
				return new SqlParameter(parameterName, SqlDbType.Real, memberAttribute.Length) { Value = Convert.ToSingle(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to short");
			}
		}

		internal static SqlParameter BuildIntParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Int, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Int, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.Int, memberAttribute.Length) { Value = Convert.ToInt32(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.Int, memberAttribute.Length) { Value = Convert.ToInt32(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.Int, memberAttribute.Length) { Value = Convert.ToInt32(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.Int, memberAttribute.Length) { Value = Convert.ToInt32(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to int");
			}
		}

		internal static SqlParameter BuildLongParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.BigInt, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.BigInt, memberAttribute.Length) { Value = 0 };
			}
			else if (value.GetType() == typeof(sbyte) || value.GetType() == typeof(byte) || value.GetType() == typeof(sbyte?) || value.GetType() == typeof(byte?))
			{
				return new SqlParameter(parameterName, SqlDbType.BigInt, memberAttribute.Length) { Value = Convert.ToInt64(value) };
			}
			else if (value.GetType() == typeof(short) || value.GetType() == typeof(ushort) || value.GetType() == typeof(short?) || value.GetType() == typeof(ushort?))
			{
				return new SqlParameter(parameterName, SqlDbType.BigInt, memberAttribute.Length) { Value = Convert.ToInt64(value) };
			}
			else if (value.GetType() == typeof(int) || value.GetType() == typeof(uint) || value.GetType() == typeof(int?) || value.GetType() == typeof(uint?))
			{
				return new SqlParameter(parameterName, SqlDbType.BigInt, memberAttribute.Length) { Value = Convert.ToInt64(value) };
			}
			else if (value.GetType() == typeof(long) || value.GetType() == typeof(ulong) || value.GetType() == typeof(long?) || value.GetType() == typeof(ulong?))
			{
				return new SqlParameter(parameterName, SqlDbType.BigInt, memberAttribute.Length) { Value = Convert.ToInt64(value) };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to int");
			}
		}

		internal static SqlParameter BuildCharParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Char, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Char, memberAttribute.Length) { Value = "" };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.Char, memberAttribute.Length) { Value = value.ToString() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.Char, memberAttribute.Length) { Value = ((Bstr)value).Decode() };
			}
			else if (value.GetType() == typeof(char))
			{
				return new SqlParameter(parameterName, SqlDbType.Char, memberAttribute.Length) { Value = value };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to char");
			}
		}

		internal static SqlParameter BuildNCharParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.NChar, memberAttribute.Length) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.NChar, memberAttribute.Length) { Value = "" };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.NChar, memberAttribute.Length) { Value = value.ToString() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.NChar, memberAttribute.Length) { Value = ((Bstr)value).Decode() };
			}
			else if (value.GetType() == typeof(char))
			{
				return new SqlParameter(parameterName, SqlDbType.NChar, memberAttribute.Length) { Value = value };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to char");
			}
		}

		internal static SqlParameter BuildHierarchyIdParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				return new SqlParameter(parameterName, SqlDbType.NVarChar, memberAttribute.Length) { Value = DBNull.Value };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.NVarChar, memberAttribute.Length) { Value = value.ToString().Replace("-", "/") };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.NVarChar) { Value = ((Bstr)value).Decode().Replace("-", "/") };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to string");
			}
		}

		internal static SqlParameter BuildVarcharParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			var nLength = memberAttribute.Length > 0 ? memberAttribute.Length : -1;

			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.VarChar, nLength) { Value = DBNull.Value };
				else

					return new SqlParameter(parameterName, SqlDbType.VarChar, nLength) { Value = "" };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.VarChar, nLength) { Value = value.ToString() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.VarChar, nLength) { Value = ((Bstr)value).Decode() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.VarChar, nLength) { Value = ((Bstr)value).Value };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to string");
			}
		}

		internal static SqlParameter BuildNVarcharParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			var nLength = memberAttribute.Length > 0 ? memberAttribute.Length : -1;

			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.NVarChar, nLength) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.NVarChar, nLength) { Value = "" };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.NVarChar, nLength) { Value = value.ToString() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.NVarChar, nLength) { Value = ((Bstr)value).Decode() };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to string");
			}
		}

		internal static SqlParameter BuildTextParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.Text) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.Text) { Value = "" };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.Text) { Value = value.ToString() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.Text) { Value = ((Bstr)value).Decode() };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to string");
			}
		}

		internal static SqlParameter BuildNTextParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.NText) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.NText) { Value = "" };
			}
			else if (value.GetType() == typeof(string))
			{
				return new SqlParameter(parameterName, SqlDbType.NText) { Value = value.ToString() };
			}
			else if (value.GetType() == typeof(Bstr))
			{
				return new SqlParameter(parameterName, SqlDbType.NText) { Value = ((Bstr)value).Decode() };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to string");
			}
		}

		internal static SqlParameter BuildGuidParameter(string parameterName, MemberAttribute memberAttribute, object? value)
		{
			if (value == null)
			{
				if (memberAttribute.IsNullable)
					return new SqlParameter(parameterName, SqlDbType.UniqueIdentifier) { Value = DBNull.Value };
				else
					return new SqlParameter(parameterName, SqlDbType.UniqueIdentifier) { Value = Guid.Empty };
			}
			else if (value.GetType() == typeof(Guid))
			{
				return new SqlParameter(parameterName, SqlDbType.UniqueIdentifier) { Value = value };
			}
			else
			{
				throw new InvalidCastException("Cannot cast value to string");
			}
		}


		internal void BuildSimpleAggregate(Type entityType, RqlNode? node, StringBuilder sql, PropertyInfo[] properties, Table tableAttribute, ref bool first)
		{
			if (node is not null)
			{
				switch (node.Operation)
				{
					case RqlOperation.AND:
						foreach (RqlNode childNode in node)
							BuildSimpleAggregate(entityType, childNode, sql, properties, tableAttribute, ref first);
						break;

					case RqlOperation.OR:
						foreach (RqlNode childNode in node)
							BuildSimpleAggregate(entityType, childNode, sql, properties, tableAttribute, ref first);
						break;

					case RqlOperation.COUNT:
					case RqlOperation.MAX:
					case RqlOperation.MIN:
					case RqlOperation.MEAN:
					case RqlOperation.SUM:
						{
							var property = properties.FirstOrDefault(p => string.Equals(p.Name, node.NonNullValue<RqlNode>(0).Value<string>(0), StringComparison.OrdinalIgnoreCase));

							if (property != null)
							{
								var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

								if (memberAttribute != null)
								{
									var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
									var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
									var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

									if (first)
										first = false;
									else
										sql.Append(", ");

									string operation = "";

									switch (node.Operation)
									{
										case RqlOperation.MAX:
											operation = "max(cast";
											break;

										case RqlOperation.MIN:
											operation = "min(cast";
											break;

										case RqlOperation.MEAN:
											operation = "avg(cast";
											break;

										case RqlOperation.SUM:
											operation = "sum(cast";
											break;

										case RqlOperation.COUNT:
											operation = "count(cast";
											break;
									}

									if (string.IsNullOrWhiteSpace(schema))
									{
										sql.Append($"{operation}({OB}{tableName}{CB}.{OB}{columnName}{CB} as decimal)) as {OB}{property.Name}{CB}");
									}
									else
									{
										sql.Append($"{operation}({OB}{schema}{CB}.{OB}{tableName}{CB}.{OB}{columnName}{CB} as decimal)) as {OB}{property.Name}{CB}");
									}
								}
								else
								{
									throw new InvalidCastException($"{node.NonNullValue<RqlNode>(0).Value<string>(0)} is not a member of {entityType.Name}");
								}
							}
							else
							{
								throw new InvalidCastException($"{node.NonNullValue<RqlNode>(0).Value<string>(0)} is not a member of {entityType.Name}");
							}
						}
						break;
				}
			}
		}

		internal bool IncludedInAggregationList(RqlNode? node, string propertyName)
		{
			bool result = false;

			if (node is not null)
			{
				switch (node.Operation)
				{
					case RqlOperation.AND:
					case RqlOperation.OR:
						foreach (RqlNode childNode in node)
							result |= IncludedInAggregationList(childNode, propertyName);
						break;

					case RqlOperation.MAX:
					case RqlOperation.MIN:
					case RqlOperation.MEAN:
					case RqlOperation.SUM:
						var propertyNode = node.NonNullValue<RqlNode>(0);
						result = string.Equals(propertyNode.Value<string>(0), propertyName, StringComparison.OrdinalIgnoreCase);
						break;

					default:
						break;
				}
			}

			return result;
		}

		private static void AppendFromClause(StringBuilder sql, Table tableAttribute)
		{
			sql.AppendLine();
			if (string.IsNullOrWhiteSpace(tableAttribute.Schema))
				sql.AppendLine($"  FROM [{tableAttribute.Name}] WITH(NOLOCK)");
			else
				sql.AppendLine($"  FROM [{tableAttribute.Schema}].[{tableAttribute.Name}] WITH(NOLOCK)");

			sql.Append(' ');
		}

		private static void AppendOrderByClause(StringBuilder sql, string orderByClause)
		{
			if (!string.IsNullOrWhiteSpace(orderByClause))
			{
				sql.Append("\r\n ORDER BY ");
				sql.Append(orderByClause);
				sql.Append(' ');
			}
		}

		private static void AppendWhereClause(StringBuilder sql, string whereClause)
		{
			if (!string.IsNullOrWhiteSpace(whereClause))
			{
				sql.Append(" WHERE ");
				sql.Append(whereClause);
				sql.Append(' ');
			}
		}

		private static void AppendGroupByClause(StringBuilder sql, RqlNode aggregateNode, Table tableAttribute, PropertyInfo[] properties)
		{
			bool firstMember = true;
			sql.Append("GROUP BY ");

			foreach (RqlNode childNode in aggregateNode)
			{
				if (childNode.Operation == RqlOperation.PROPERTY)
				{
					var property = properties.FirstOrDefault(p => string.Equals(childNode.Value<string>(0), p.Name, StringComparison.OrdinalIgnoreCase));
					AppendProperty(sql, tableAttribute, property, ref firstMember);
				}
			}

			sql.Append(' ');
		}

		private static void AppendProperty(StringBuilder sql, Table tableAttribute, PropertyInfo property, ref bool firstMember)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
			var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
			var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
			var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

			if (firstMember)
				firstMember = false;
			else
				sql.Append(", ");

			if (string.IsNullOrWhiteSpace(schema))
			{
				if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
					sql.Append($"[{tableName}].[{columnName}]");
				else
					sql.Append($"[{tableName}].[{columnName}] as [{property.Name}]");
			}
			else
			{
				if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
					sql.Append($"[{schema}].[{tableName}].[{columnName}]");
				else
					sql.Append($"[{schema}].[{tableName}].[{columnName}] as [{property.Name}]");
			}
		}
		private static void AppendPropertyForOrderBy(StringBuilder sql, PropertyInfo property, ref bool firstMember)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
			var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

			if (firstMember)
				firstMember = false;
			else
				sql.Append(", ");

			if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
				sql.Append($"[{columnName}]");
			else
				sql.Append($"[{property.Name}]");
		}

		private static void AppendPropertyForRead(StringBuilder sql, Table tableAttribute, PropertyInfo property, ref bool firstMember, string overrideTable = "")
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();
			var tableName = string.IsNullOrWhiteSpace(memberAttribute.TableName) ? tableAttribute.Name : memberAttribute.TableName;
			var schema = string.IsNullOrWhiteSpace(memberAttribute.Schema) ? tableAttribute.Schema : memberAttribute.Schema;
			var columnName = string.IsNullOrWhiteSpace(memberAttribute.ColumnName) ? property.Name : memberAttribute.ColumnName;

			if (firstMember)
				firstMember = false;
			else
				sql.Append(", ");

			if (string.Equals(memberAttribute.NativeDataType, "hierarchyid", StringComparison.OrdinalIgnoreCase))
			{
				if (string.IsNullOrWhiteSpace(schema))
				{
					if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"CAST([{tableName}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as {columnName}");
						else
							sql.Append($"CAST([{overrideTable}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as {columnName})");
					}
					else
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"CAST([{tableName}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as [{property.Name}]");
						else
							sql.Append($"[{overrideTable}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as [{property.Name}]");
					}
				}
				else
				{
					if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"CAST([{schema}].[{tableName}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as {columnName}");
						else
							sql.Append($"CAST([{overrideTable}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as {columnName}");
					}
					else
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"[{schema}].[{tableName}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as {property.Name}");
						else
							sql.Append($"[{overrideTable}].[{columnName}] as NVARCHAR({memberAttribute.Length})) as {property.Name}");
					}
				}
			}
			else
			{
				if (string.IsNullOrWhiteSpace(schema))
				{
					if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"[{tableName}].[{columnName}]");
						else
							sql.Append($"[{overrideTable}].[{columnName}]");
					}
					else
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"[{tableName}].[{columnName}] as [{property.Name}]");
						else
							sql.Append($"[{overrideTable}].[{columnName}] as [{property.Name}]");
					}
				}
				else
				{
					if (string.Equals(columnName, property.Name, StringComparison.OrdinalIgnoreCase))
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"[{schema}].[{tableName}].[{columnName}]");
						else
							sql.Append($"[{overrideTable}].[{columnName}]");
					}
					else
					{
						if (string.IsNullOrEmpty(overrideTable))
							sql.Append($"[{schema}].[{tableName}].[{property.Name}]");
						else
							sql.Append($"[{overrideTable}].[{property.Name}]");
					}
				}
			}
		}
		#endregion
	}
}
