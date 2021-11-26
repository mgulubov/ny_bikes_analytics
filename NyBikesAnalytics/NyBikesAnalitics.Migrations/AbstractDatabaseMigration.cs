namespace NyBikesAnalitics.Migrations
{
    using FluentMigrator;

    using Interfaces;
    using NyBikesAnalitics.Migrations.Readers;

    public abstract class AbstractDatabaseMigration : Migration, IDatabaseMigration
    {
        private readonly string scriptName;
        private readonly EmbeddedResourceReader reader;

        protected AbstractDatabaseMigration(string scriptName)
        {
            this.reader = new EmbeddedResourceReader(typeof(AbstractDatabaseMigration).Assembly);
            this.scriptName = scriptName;
        }

        public abstract string BaseDirectory { get; }

        public abstract override void Up();

        public abstract override void Down();

        protected virtual string GetEmbeddedUpScriptPath()
        {
            return $"{this.BaseDirectory}/UpScripts/{this.scriptName}";
        }

        protected virtual string GetEmbeddedDownScriptPath()
        {
            return $"{this.BaseDirectory}/DownScripts/{this.scriptName}";
        }

        protected void ExecuteSqlStatementFromFile(string filePath)
        {
            string sqlStatement = this.reader.GetEmbeddedResource(filePath);
            this.Execute.Sql(sqlStatement);
        }
    }
}
