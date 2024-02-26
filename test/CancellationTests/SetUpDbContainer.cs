using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        var uri = new DirectoryInfo("D:\\Programming\\FSharp\\_open_source\\Dapper.FSharp\\tests\\Dapper.FSharp.MySQL.Tests\\init.sql");
        var fi = new FileInfo(uri.FullName);
        var container =
            new MySqlBuilder()
                .WithResourceMapping(fi, "/docker-entrypoint-initdb.d/")
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
