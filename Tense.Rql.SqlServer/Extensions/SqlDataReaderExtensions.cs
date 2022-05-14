using Tense.Rql;
using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Tense.Rql.SqlServer
{
	/// <summary>
	/// Extensions to SqlDataReader
	/// </summary>
    public static class SqlDataReaderExtensions
	{
		/// <summary>
		/// Read the property
		/// </summary>
		/// <param name="reader">The <see cref="SqlDataReader"/>.</param>
		/// <param name="property">The <see cref="PropertyInfo"/> for the property being read.</param>
		/// <param name="memberAttribute">The <see cref="MemberAttribute"/> associated with the property.</param>
		public static object ReadProperty(this SqlDataReader reader, PropertyInfo property, MemberAttribute memberAttribute)
		{
			var propertyType = property.PropertyType;

			if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
			{
				propertyType = Nullable.GetUnderlyingType(property.PropertyType);
			}

			if (propertyType == reader.GetFieldType(reader.GetOrdinal(property.Name)))
			{
				if (string.Equals(memberAttribute.NativeDataType, "hierarchyid", StringComparison.OrdinalIgnoreCase))
				{
					var theValue = (string)reader.GetFieldValue<object>(reader.GetOrdinal(property.Name));
					theValue = theValue.Replace("/", "-");
					return theValue;
				}
				else
				{
					return reader.GetFieldValue<object>(reader.GetOrdinal(property.Name));
				}
			}
			else if (propertyType == typeof(char) && reader.GetFieldType(reader.GetOrdinal(property.Name)) == typeof(string))
			{
				var str = reader.GetFieldValue<string>(reader.GetOrdinal(property.Name));
				return str[0];
			}
#if windows
			else if (propertyType == typeof(Image) && reader.GetFieldType(reader.GetOrdinal(property.Name)) == typeof(byte[]))
			{
				var theBytes = reader.GetFieldValue<byte[]>(reader.GetOrdinal(property.Name));
				return ImageEx.Parse(theBytes);
			}
#endif
			else
			{
				var dbValue = reader.GetFieldValue<object>(reader.GetOrdinal(property.Name));
				var propValue = Convert.ChangeType(dbValue, propertyType);
				return propValue;
			}

			throw new InvalidCastException($"Unrecognized data type {propertyType}");
		}

		/// <summary>
		/// Read the property asynchronously
		/// </summary>
		/// <param name="reader">The <see cref="SqlDataReader"/>.</param>
		/// <param name="property">The <see cref="PropertyInfo"/> for the property being read.</param>
		/// <param name="memberAttribute">The <see cref="MemberAttribute"/> associated with the property.</param>
		/// <param name="token">The Cancellation token</param>
		public static async Task<object> ReadPropertyAsync(this SqlDataReader reader, PropertyInfo property, MemberAttribute memberAttribute, CancellationToken token)
		{
			var propertyType = property.PropertyType;

			if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
			{
				propertyType = Nullable.GetUnderlyingType(property.PropertyType);
			}

			if (propertyType == reader.GetFieldType(reader.GetOrdinal(property.Name)))
			{
				if (string.Equals(memberAttribute.NativeDataType, "hierarchyid", StringComparison.OrdinalIgnoreCase))
				{
					var theValue = (string)await reader.GetFieldValueAsync<object>(reader.GetOrdinal(property.Name), token).ConfigureAwait(false);
					theValue = theValue.Replace("/", "-");
					return theValue;
				}
				else if (string.Equals(memberAttribute.NativeDataType, "date", StringComparison.OrdinalIgnoreCase) ||
						 string.Equals(memberAttribute.NativeDataType, "datetime", StringComparison.OrdinalIgnoreCase) ||
						 string.Equals(memberAttribute.NativeDataType, "smalldatetime", StringComparison.OrdinalIgnoreCase) ||
						 string.Equals(memberAttribute.NativeDataType, "time", StringComparison.OrdinalIgnoreCase) ||
						 string.Equals(memberAttribute.NativeDataType, "datetime2", StringComparison.OrdinalIgnoreCase))
				{
					var val = reader.GetDateTime(reader.GetOrdinal(property.Name));

					if (val.Kind == DateTimeKind.Unspecified)
						val = new DateTime(val.Ticks, DateTimeKind.Utc);

					if (val.Kind == DateTimeKind.Local)
						val = val.ToUniversalTime();

					return val;
				}
				else if (string.Equals(memberAttribute.NativeDataType, "datetimeoffset", StringComparison.OrdinalIgnoreCase) )
				{
					var val = reader.GetDateTimeOffset(reader.GetOrdinal(property.Name));
					return val;
				}
				else
				{
					return await reader.GetFieldValueAsync<object>(reader.GetOrdinal(property.Name), token).ConfigureAwait(false);
				}
			}
			else if (propertyType == typeof(char) && reader.GetFieldType(reader.GetOrdinal(property.Name)) == typeof(string))
			{
				var str = await reader.GetFieldValueAsync<string>(reader.GetOrdinal(property.Name), token).ConfigureAwait(false);
				return str[0];
			}
#if windows
			else if (propertyType == typeof(Image) && reader.GetFieldType(reader.GetOrdinal(property.Name)) == typeof(byte[]))
            {
				var theBytes = await reader.GetFieldValueAsync<byte[]>(reader.GetOrdinal(property.Name), token).ConfigureAwait(false);
				return ImageEx.Parse(theBytes);
            }
#endif
			else
			{
				var dbValue = await reader.GetFieldValueAsync<object>(reader.GetOrdinal(property.Name), token).ConfigureAwait(false);
				var propValue = Convert.ChangeType(dbValue, propertyType);
				return propValue;
			}

			throw new InvalidCastException($"Unrecognized data type {propertyType}");
		}

		/// <summary>
		/// Creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <typeparam name="T">The type of object to read.</typeparam>
		/// <param name="reader">The <see cref="SqlDataReader"/>.</param>
		/// <param name="node">The <see cref="RqlNode"/> used to filter the object members.</param>
		/// <returns></returns>
		public static object GetObject<T>(this SqlDataReader reader, RqlNode node)
        {
			return reader.GetObject(node, typeof(T));
        }

		/// <summary>
		/// Creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <param name="reader">The SqlDataReader used to populate the object</param>
		/// <param name="node">The <see cref="RqlNode"/> used to filter the object members.</param>
		/// <param name="T">The type of object to create and populate.</param>
		/// <returns></returns>
		public static object GetObject(this SqlDataReader reader, RqlNode node, Type T)
		{
			var model = Activator.CreateInstance(T);
			PropertyInfo[] properties = T.GetProperties();
			RqlNode? selectClause = node?.ExtractSelectClause();
			bool hasAggregates = node != null && node.HasAggregates();
			bool hasValues = node != null && node.Contains(RqlOperation.VALUES);

			foreach (var property in properties)
			{
				//	Decide if we want to include this field in the result set. It must be a member attribute to be read from the database
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					//	If the select list is empty, include all fields.
					//	If the select list is not empty, and the field is the primary key, include it.
					//	If the select list is not empty, and the field is not the primary key, only include it if it is in the list.
					var includeProperty = true;

					var propertyNode = new RqlNode(RqlOperation.PROPERTY);
					propertyNode.Add(property.Name);

					if (selectClause != null &&                 //	we have a list 
						!selectClause.Contains(propertyNode))   //  and this field is not in the list.
					{
						if (!hasAggregates && !hasValues)
						{
							includeProperty = property.GetCustomAttribute<MemberAttribute>().IsPrimaryKey;
						}
						else
							includeProperty = false;
					}

					if (includeProperty)
					{
						if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
						{
							if (reader.IsDBNull(reader.GetOrdinal(property.Name)))
							{
								property.SetValue(model, null);
							}
							else
							{
								property.SetValue(model, reader.ReadProperty(property, memberAttribute));
							}
						}
						else
						{
							property.SetValue(model, reader.ReadProperty(property, memberAttribute));
						}
					}
				}
			}

			return model;
		}

		/// <summary>
		/// Asynchronously creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <typeparam name="T">The type of object to read.</typeparam>
		/// <param name="reader">The <see cref="SqlDataReader"/>.</param>
		/// <param name="node">The <see cref="RqlNode"/> used to filter the object members.</param>
		/// <param name="token">The <see cref="CancellationToken"/> used to cancel the request.</param>
		/// <returns></returns>
		public static async Task<object?> GetObjectAsync<T>(this SqlDataReader reader, RqlNode node, CancellationToken token)
        {
			return await reader.GetObjectAsync(typeof(T), node, token);
        }

		/// <summary>
		/// Asynchronously creates and populates an object of type T from the data in the database.
		/// </summary>
		/// <param name="reader">The SqlDataReader used to populate the object</param>
		/// <param name="node">The <see cref="RqlNode"/> used to filter the object members.</param>
		/// <param name="T">The type of object to create and populate.</param>
		/// <param name="token">The <see cref="CancellationToken"/> used to cancel the request.</param>
		/// <returns></returns>
		public static async Task<object?> GetObjectAsync(this SqlDataReader reader, Type T, RqlNode? node, CancellationToken token)
		{
			//	First, create an instance of the object of type T
			var model = Activator.CreateInstance(T);

			//	Get the list of properties of the object of type T
			PropertyInfo[] properties = T.GetProperties();

			//	Obtain the list of properties to include (these will be the properties 
			//	included in the SELECT clause, if any. If no SELECT clause is found
			//	then it is simply the list of all properties.
			RqlNode? selectClause = node?.ExtractSelectClause();
			bool hasDistinct = node != null && node.Find(RqlOperation.DISTINCT) != null;
			bool hasAggregates = node != null && node.HasAggregates();
			bool hasValues = node != null && node.Contains(RqlOperation.VALUES);

			foreach (var property in properties)
			{
				//	Decide if we want to include this field in the result set. It must be a member attribute to be read from the database
				var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

				if (memberAttribute != null)
				{
					//	If we have a SELECT clause, and this property is not in that list, then don't 
					//	include it in the output, unless, that property is an identity property
					//	and the RQL statement does not include any aggregate functions or a VALUES 
					//	clause. 
					//
					//	Note: Aggregate functions include the AGGREGATE, MAX, MIN, MEAN, SUM and
					//		  COUNT clauses.

					var propertyNode = new RqlNode(RqlOperation.PROPERTY);
					propertyNode.Add(property.Name);

					if (RqlUtilities.CheckForInclusion(property, selectClause, !(hasDistinct || hasAggregates || hasValues)))
					{
						//	The property value may be null. If so, account for that here.
						if (reader.IsDBNull(reader.GetOrdinal(property.Name)))
						{
							property.SetValue(model, null);
						}
						else
						{
							property.SetValue(model, await reader.ReadPropertyAsync(property, memberAttribute, token).ConfigureAwait(false));
						}
					}
				}
			}

			return model;
		}
	}
}
