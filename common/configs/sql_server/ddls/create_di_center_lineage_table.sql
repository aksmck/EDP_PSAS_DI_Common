create table [dbo].[DI_CENTER_LINEAGE](
id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
segment nvarchar(max),
sub_segment nvarchar(max),
parent_node nvarchar(max),
lineage nvarchar(max)
);
