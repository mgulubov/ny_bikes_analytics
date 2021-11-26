namespace NyBikesAnalitics.Migrations.NyBikesDb
{
    using FluentMigrator;

    [Migration(2, "Create and fill the weather_data table.")]
    public class CreateWeatherDataTable : NyBikesDatabaseMigration
    {

        private const string ScriptName = "0000002_CreateWeatherDataTable.sql";

        public CreateWeatherDataTable()
            : base(ScriptName)
        {
        }

        public override void Up()
        {
            string filePath = this.GetEmbeddedUpScriptPath();
            this.ExecuteSqlStatementFromFile(filePath);
        }

        public override void Down()
        {
            string filePath = this.GetEmbeddedDownScriptPath();
            this.ExecuteSqlStatementFromFile(filePath);
        }
    }
}
