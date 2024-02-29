namespace Dapper.FSharp.MySQL.Tests

open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open NUnit.Framework.Legacy
open Dapper.FSharp.MySQL
open Dapper.FSharp.Testing.Database
open Dapper.FSharp.MySQL.Tests.Database

open MySql.Data.MySqlClient

[<NonParallelizable>]
module DeleteTests =
    let personsView = table'<Persons.View> "Persons"

    let mutable conn: MySqlConnection = Unchecked.defaultof<_>

    [<OneTimeSetUp>]
    let ``setup DB``() =
        Dapper.FSharp.MySQL.OptionTypes.register()
        conn <- Database.getConnection2 ()

    [<OneTimeTearDown>]
    let ``tear down DB``() =
        task {
            match conn with
            | null -> ()
            | _ -> do! conn.DisposeAsync()
        }

    [<Test>]
    let ``deletes single records``() =
        task {
        do! Persons.init conn
        let rs = Persons.View.generateMany 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> conn.InsertAsync
        let! _ =
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> conn.DeleteAsync
        let! fromDb =
            select {
                for p in personsView do
                orderByDescending p.Position
            } |> conn.SelectAsync<Persons.View>
        
        ClassicAssert.AreEqual(9, Seq.length fromDb)
        ClassicAssert.AreEqual(9, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
    }

#if MySqlData_lt_8_0_33
    [<Test>]
    let ``cancellation works``() =
        task {
        do! Persons.init conn
        let rs = Persons.View.generateMany 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> conn.InsertAsync
        use cts = new CancellationTokenSource()
        cts.Cancel()
        let deleteCrud query =
            conn.DeleteAsync(query, cancellationToken = cts.Token) :> Task
        let action () = 
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> deleteCrud 
        
        Assert.ThrowsAsync<TaskCanceledException>(action) |> ignore
    }
#else
    [<Test>]
    let ``cancellation does not work``() =
        task {
        do! Persons.init conn
        let rs = Persons.View.generateMany 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> conn.InsertAsync
        use cts = new CancellationTokenSource()
        cts.Cancel()
        let deleteCrud query =
            conn.DeleteAsync(query, cancellationToken = cts.Token) :> Task
        let action () = 
            delete {
                for p in personsView do
                where (p.Position = 10)
            } |> deleteCrud 
        
        Assert.DoesNotThrowAsync(action) |> ignore
    }
#endif

    [<Test>]
    let ``deletes more records``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                delete {
                    for p in personsView do
                    where (p.Position >= 7)
                } |> conn.DeleteAsync

            let! fromDb =
                select {
                    for p in personsView do
                    selectAll
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(6, Seq.length fromDb)
        }