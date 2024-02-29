namespace Dapper.FSharp.MySQL.Tests

open System
open System.IO
open NUnit.Framework
open Testcontainers.MySql

[<SetUpFixture>]
type SetUpDbContainer() =
    let mySqlContainer () =
        task {
            let uri = DirectoryInfo(Path.Combine(__SOURCE_DIRECTORY__, ""))
            let fi = FileInfo(Path.Combine(uri.FullName, "init.sql"))
            let container =
                MySqlBuilder()
                    .WithResourceMapping(fi, "/docker-entrypoint-initdb.d/")
                    .WithDatabase("fsharp_dapper_test_db")
                    .WithUsername("admin")
                    .WithPassword("admin")
                    .Build()

            do TestContext.Progress.WriteLine($"[{DateTime.Now:``HH:mm:ss:fff``}] MySqlTests: Starting MySqlContainer...")
            do! container.StartAsync().ConfigureAwait(false)
            do TestContext.Progress.WriteLine($"[{DateTime.Now:``HH:mm:ss:fff``}] MySqlTests: MySqlContainer has been started.")
            return container
        }

    static member val MySqlContainer : MySqlContainer = Unchecked.defaultof<_> with get, set

    [<OneTimeSetUp>]
    member _.``create MySqlContainer`` () =
        task {
            Dapper.FSharp.MySQL.OptionTypes.register()
            let! mySqlContainer = mySqlContainer ()
            SetUpDbContainer.MySqlContainer <- mySqlContainer
        }

    [<OneTimeTearDown>]
    member _.``dispose MySqlContainer`` () =
        task {
            match SetUpDbContainer.MySqlContainer with
            | null -> return ()
            | _ ->
                do!
                    (SetUpDbContainer.MySqlContainer :> IAsyncDisposable).DisposeAsync()
                do TestContext.Progress.WriteLine($"[{DateTime.Now:``HH:mm:ss:fff``}] MySqlTests: MySqlContainer has been stopped.")
        }