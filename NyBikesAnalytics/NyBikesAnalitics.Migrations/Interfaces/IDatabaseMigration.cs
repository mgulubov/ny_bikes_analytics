namespace NyBikesAnalitics.Migrations.Interfaces
{
    public interface IDatabaseMigration
    {
        string BaseDirectory { get; }

        void Up();

        void Down();
    }
}
