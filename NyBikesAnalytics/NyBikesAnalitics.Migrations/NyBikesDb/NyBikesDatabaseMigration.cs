namespace NyBikesAnalitics.Migrations.NyBikesDb
{
    using FluentMigrator;

    [Tags("NyBikes")]
    public abstract class NyBikesDatabaseMigration : AbstractDatabaseMigration
    {
        private readonly string baseDirectory = "NyBikesDb";

        public NyBikesDatabaseMigration(string scriptName)
           : base(scriptName)
        {
        }

        public override string BaseDirectory
        {
            get { return this.baseDirectory; }
        }
    }
}
