namespace NyBikesAnalitics.Migrations.NyBikesDb
{
    using FluentMigrator.Runner.VersionTableInfo;

    [VersionTableMetaData]
    public class VersionTable : IVersionTableMetaData
    {
        public object ApplicationContext { get; set; }

        public string TableName => "version_info";

        public string ColumnName => "version";

        public string AppliedOnColumnName => "applied_on";

        public string DescriptionColumnName => "description";

        public string SchemaName => string.Empty;

        public string UniqueIndexName => "uc_version";

        public bool OwnsSchema => true;
    }
}
