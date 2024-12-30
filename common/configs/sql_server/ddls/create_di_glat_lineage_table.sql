create table [dbo].[DI_GLAT_LINEAGE](
id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
type nvarchar(max),
sub_type nvarchar(max),
parent_node nvarchar(max),
lineage nvarchar(max)
);
