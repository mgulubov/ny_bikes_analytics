namespace NyBikesAnalitics.Migrations.NyBikesDb
{
    using FluentMigrator;

    [Migration(1, "Create and fill the bike_trip table")]
    public class CreateBikeTripTable : NyBikesDatabaseMigration
    {
        private const string ScriptName = "0000001_CreateBikeTripTable.sql";

        public CreateBikeTripTable()
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
        }
    }
}
