using System.Data;
using Dapper;
using MySql.Data.MySqlClient;

namespace TestProject1;

public class MySQLCancellationTests
{
#pragma warning disable NUnit1032 // An IDisposable field/property should be Disposed in a TearDown method
    private IDbConnection conn = default!;
#pragma warning restore NUnit1032 // An IDisposable field/property should be Disposed in a TearDown method

    [OneTimeSetUp]
    public void Setup()
    {
        conn = new MySqlConnection(SetUpDbContainer.MySqlContainer!.GetConnectionString());
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (conn is not null)
        {
            await ((MySqlConnection)conn).DisposeAsync();
        }
    }

    [Test]
    [Ignore("To check MySQL.Data versions")]
    public async Task ExecuteAsync_CancellationTest()
    {
        // await conn.ExecuteAsync("drop table Persons");
        await conn.ExecuteAsync("""
            create table Persons
            (
                Id char(36) not null,
                FirstName nvarchar(255) not null,
                LastName longtext not null,
                Position int not null,
                DateOfBirth datetime null
            );
            
            create unique index Persons_Id_uindex
                on Persons (Id);
            """);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        AsyncTestDelegate action = async () =>
        {
            var sql = """
                insert into Persons (Id, FirstName, LastName, Position)
                values (@Id, @FirstName, @LastName, @Position)
                """;

            var cd = new CommandDefinition(sql, new { Id = Guid.NewGuid(), FirstName = "F", LastName = "L", Position = 1}, cancellationToken: cts.Token);
            await conn.ExecuteAsync(cd);
        };

        Assert.ThrowsAsync<TaskCanceledException>(action);
    }
}