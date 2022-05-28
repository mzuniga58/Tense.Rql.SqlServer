using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tense.Rql.SqlServer.UnitTests.Models
{
	///	<summary>
	///	ECustomer
	///	</summary>
	[Table("Customers", Schema = "dbo", DBType = "SQLSERVER")]
	public class ECustomer
	{
		///	<summary>
		///	Id
		///	</summary>
		[Member(IsPrimaryKey = true, IsIdentity = true, AutoField = true, IsIndexed = true, IsNullable = false, NativeDataType = "int")]
		public int Id { get; set; }

		///	<summary>
		///	FirstName
		///	</summary>
		[Member(IsNullable = false, Length = 50, IsFixed = false, NativeDataType = "varchar")]
		public string FirstName { get; set; } = string.Empty;

		///	<summary>
		///	LastName
		///	</summary>
		[Member(IsNullable = false, Length = 50, IsFixed = false, NativeDataType = "varchar")]
		public string LastName { get; set; } = string.Empty;

		///	<summary>
		///	Category
		///	</summary>
		[Member(IsNullable = false, NativeDataType = "int")]
		public int Category { get; set; }

		///	<summary>
		///	Age
		///	</summary>
		[Member(IsNullable = false, NativeDataType = "int")]
		public int Age { get; set; }

		///	<summary>
		///	Score1
		///	</summary>
		[Member(IsNullable = false, Precision = 18, Scale = 4, NativeDataType = "decimal")]
		public decimal Score1 { get; set; }

		///	<summary>
		///	Score2
		///	</summary>
		[Member(IsNullable = false, Precision = 18, Scale = 4, NativeDataType = "decimal")]
		public decimal Score2 { get; set; }
	}
}
