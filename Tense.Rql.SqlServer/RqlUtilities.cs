using Tense.Rql;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Tense.Rql.SqlServer
{
	/// <summary>
	/// A collection of RQL utilities
	/// </summary>
	public static class RqlUtilities
	{
        /// <summary>
        /// Extracts the list of properties used by any aggregate functions
        /// </summary>
        /// <returns></returns>
        public static RqlNode ExtractAggregateFields(this RqlNode node)
		{
			return node.ExtractAggregateFields(node);
		}

        /// <summary>
        /// Extracts the list of properties used by any aggregate functions
        /// </summary>
        /// <param name="_"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        public static RqlNode ExtractAggregateFields(this RqlNode _, RqlNode node)
		{
			var resultNode = new RqlNode(RqlOperation.SELECT);

			switch (node.Operation)
			{
				case RqlOperation.AND:
				case RqlOperation.OR:
					foreach (RqlNode? childnode in node)
					{
						if (childnode != null)
						{
							var agg = ExtractAggregateFields(childnode);

							foreach (var child in agg)
								resultNode.Add(child);
						}
					}
					break;

				case RqlOperation.MAX:
				case RqlOperation.MIN:
				case RqlOperation.SUM:
				case RqlOperation.MEAN:
					resultNode.Add((RqlNode?)node[0]);
					break;

				case RqlOperation.COUNT:
					if (node.Count > 0)
						resultNode.Add((RqlNode?)node[0]);
					break;

				case RqlOperation.AGGREGATE:
					foreach (RqlNode? childNode in node)
					{
						if (childNode?.Operation == RqlOperation.PROPERTY)
						{
							resultNode.Add(childNode);
						}
						else
						{
							resultNode.Add((RqlNode?)childNode?[0]);
						}
					}
					break;
			}

			return resultNode;
		}

		/// <summary>
		/// Extracts the list of items used in a where clause
		/// </summary>
		/// <returns></returns>
		public static List<KeyValuePair<string, object>> ExtractKeyList(this RqlNode node)
		{
			var theList = new List<KeyValuePair<string, object>>();

			if (node.Operation == RqlOperation.AND)
			{
				foreach (RqlNode? child in node)
				{
					theList.AddRange(child?.ExtractKeyList());
				}
			}
			else if (node.Operation == RqlOperation.OR)
			{
				foreach (RqlNode? child in node)
				{
					theList.AddRange(child?.ExtractKeyList());
				}
			}
			else if (node.Operation == RqlOperation.EQ)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.NE)
			{
				theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
			}
			else if (node.Operation == RqlOperation.LT)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.GT)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.LE)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.GE)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.DISTINCT)
			{
			}
			else if (node.Operation == RqlOperation.LIMIT)
			{
			}
			else if (node.Operation == RqlOperation.SELECT)
			{
			}
			else if (node.Operation == RqlOperation.SORT)
			{
			}
			else if (node.Operation == RqlOperation.IN)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.OUT)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.SUM)
			{
			}
			else if (node.Operation == RqlOperation.MAX)
			{
			}
			else if (node.Operation == RqlOperation.MIN)
			{
			}
			else if (node.Operation == RqlOperation.MEAN)
			{
			}
			else if (node.Operation == RqlOperation.VALUES)
			{
			}
			else if (node.Operation == RqlOperation.COUNT)
			{
			}
			else if (node.Operation == RqlOperation.FIRST)
			{
			}
			else if (node.Operation == RqlOperation.ONE)
			{
			}
			else if (node.Operation == RqlOperation.AGGREGATE)
			{
			}
			else if (node.Operation == RqlOperation.CONTAINS || node.Operation == RqlOperation.LIKE)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }
            else if (node.Operation == RqlOperation.EXCLUDES)
			{
                theList.Add(new KeyValuePair<string, object>(node.NonNullValue<RqlNode>(0).NonNullValue<string>(0), node.NonNullValue<RqlNode>(0).NonNullValue<string>(1)));
            }

            return theList;
		}

		/// <summary>
		/// Checks to see if the property should be included in the SELECT Statement
		/// </summary>
		/// <param name="property"></param>
		/// <param name="selectClause"></param>
		/// <param name="includeKey"></param>
		/// <returns></returns>
		public static bool CheckForInclusion(PropertyInfo property, RqlNode? selectClause, bool includeKey = true)
		{
			var memberAttribute = property.GetCustomAttribute<MemberAttribute>();

			if (memberAttribute == null)
				return false;

			if (memberAttribute.IsPrimaryKey && includeKey)
				return true;

			if (selectClause == null)                   //	do we have a list?
				return true;

			var propertyNode = new RqlNode(RqlOperation.PROPERTY);
			propertyNode.Add(property.Name);

			if (selectClause.Contains(propertyNode))    //	Is this property in the list
				return true;

			return false;
		}
	}
}
