using Testcontainers.MySql;

namespace TestProject1;

[SetUpFixture]
public class SetUpDbContainer
{
    public static MySqlContainer? MySqlContainer { get; private set; }

    [OneTimeSetUp]
    public async Task CreateMySqlContainer ()
    {
        var mySqlContainer = await UpMySqlContainer();
        SetUpDbContainer.MySqlContainer = mySqlContainer;
    }

    [OneTimeTearDown]
    public async Task DisposeMySqlContainer()
    {
        if (SetUpDbContainer.MySqlContainer is not null)
        {
            await SetUpDbContainer.MySqlContainer.DisposeAsync();
        }
    }

    private async Task<MySqlContainer> UpMySqlContainer()
    {
        var container =
            new MySqlBuilder()
                .WithImage("mysql:8.0.33")
                .WithDatabase("fsharp_dapper_test_db")
                .WithUsername("admin")
                .WithPassword("admin")
                .Build();

        TestContext.Progress.WriteLine($"[{DateTime.Now:``HH:mm:ss:fff``}] MySqlTests: Starting MySqlContainer...");
        await container.StartAsync().ConfigureAwait(false);
        TestContext.Progress.WriteLine($"[{DateTime.Now:``HH:mm:ss:fff``}] MySqlTests: MySqlContainer has been started.");
        return container;
    }
}
