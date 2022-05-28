using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tense.Rql.SqlServer.UnitTests.Models
{
	///	<summary>
	///	EAuthor
	///	</summary>
	[Table("Authors", Schema = "dbo", DBType = "SQLSERVER")]
	public class EAuthor
	{
		///	<summary>
		///	AuthorId
		///	</summary>
		[Member(IsPrimaryKey = true, IsIdentity = true, AutoField = true, IsIndexed = true, IsNullable = false, NativeDataType = "int")]
		public int AuthorId { get; set; }

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
		///	Website
		///	</summary>
		[Member(IsNullable = true, IsFixed = false, NativeDataType = "varchar")]
		public string? Website { get; set; }
	}
}
