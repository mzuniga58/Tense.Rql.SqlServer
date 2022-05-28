using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tense.Rql.SqlServer.UnitTests.Models
{
	///	<summary>
	///	EBooksByAuthor
	///	</summary>
	[Table("BooksByAuthor", Schema = "dbo", DBType = "SQLSERVER")]
	public class EBooksByAuthor
	{
		///	<summary>
		///	AuthorId
		///	</summary>
		[Member(IsNullable = false, NativeDataType = "int")]
		public int AuthorId { get; set; }

		///	<summary>
		///	BookId
		///	</summary>
		[Member(IsNullable = false, NativeDataType = "int")]
		public int BookId { get; set; }

		///	<summary>
		///	Title
		///	</summary>
		[Member(IsNullable = false, Length = 50, IsFixed = false, NativeDataType = "varchar")]
		public string Title { get; set; } = string.Empty;

		///	<summary>
		///	PublishDate
		///	</summary>
		[Member(IsNullable = false, NativeDataType = "datetime")]
		public DateTime PublishDate { get; set; } = DateTime.UtcNow;

		///	<summary>
		///	CategoryId
		///	</summary>
		[Member(IsNullable = false, NativeDataType = "int")]
		public int CategoryId { get; set; }

		///	<summary>
		///	Synopsis
		///	</summary>
		[Member(IsNullable = true, IsFixed = false, NativeDataType = "varchar")]
		public string? Synopsis { get; set; }
	}
}
