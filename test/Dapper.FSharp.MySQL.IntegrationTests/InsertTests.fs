namespace Dapper.FSharp.MySQL.Tests

open System
open System.Threading
open System.Threading.Tasks

open MySql.Data.MySqlClient

open NUnit.Framework
open NUnit.Framework.Legacy
open FsUnit

open Dapper.FSharp.MySQL
open Dapper.FSharp.Testing.Database
open Dapper.FSharp.MySQL.Tests.Database
open FsUnitTyped.TopLevelOperators

[<NonParallelizable>]
module InsertTests =

    let personsView = table'<Persons.View> "Persons"

    let mutable conn: MySqlConnection = Unchecked.defaultof<_>
    
    [<OneTimeSetUp>]
    let ``setup DB``() =
        conn <- Database.getConnection2 ()

    [<OneTimeTearDown>]
    let ``tear down DB``() =
        task {
            match conn with
            | null -> ()
            | _ -> do! conn.DisposeAsync()
        }
    
    [<Test>]
    let ``inserts new record``() = 
        task {
            do! Persons.init conn
            let r = Persons.View.generate ()
            let! _ =
                insert {
                    into personsView
                    value r
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.Id = r.Id)
                } |> conn.SelectAsync<Persons.View>

            fromDb |> Seq.head |> should equal r
        }

    [<Test>]
    let ``cancellation works``() = 
        task {
            do! Persons.init conn
            let r = Persons.View.generate ()

            let cts = new CancellationTokenSource()
            cts.Cancel()
            let insertCrud query =
                conn.InsertAsync(query, cancellationToken = cts.Token) :> Task

            let action () = 
                insert {
                    into personsView
                    value r
                } |> insertCrud 

            Assert.ThrowsAsync<System.Threading.Tasks.TaskCanceledException>(action) |> ignore
            cts.Dispose()
        }

    [<Test>]
    let ``inserts partial record``() = 
        task {        
            do! Persons.init conn
            let personsRequired = table'<Persons.ViewRequired> "Persons"

            let r =
                Persons.View.generate ()
                |> fun x -> ({ Id = x.Id; FirstName = x.FirstName; LastName = x.LastName; Position = x.Position } : Persons.ViewRequired)

            let! _ =
                insert {
                    into personsRequired
                    value r
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsRequired do
                    where (p.Id = r.Id)
                } |> conn.SelectAsync<Persons.ViewRequired>
            
            ClassicAssert.AreEqual(r, Seq.head fromDb)
        }
    
    [<Test>]
    let ``inserts partial record using 'excludeColumn'``() = 
        task {        
            do! Persons.init conn
            let personsView = table'<Persons.View> "Persons"

            let r = Persons.View.generate ()

            let! _ =
                insert {
                    for p in personsView do
                    value r
                    excludeColumn r.DateOfBirth
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.Id = r.Id)
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual({ r with DateOfBirth = None }, Seq.head fromDb)
        }
    
    [<Test>]
    let ``inserts more records``() = 
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    orderBy p.Position
                }
                |> conn.SelectAsync<Persons.View>

            fromDb |> should equivalent rs
        }
    
    [<Test>]
    let ``insert with 2 included fields``() = 
        task {
            let person = 
                {
                    Id = Guid.Empty
                    FirstName = "John"
                    LastName = "Doe"
                    Position = 100
                    DateOfBirth = None
                } : Persons.View

            let query =
                insert {
                    for p in table<Persons.View> do
                    value person
                    includeColumn p.FirstName
                    includeColumn p.LastName
                }
                
            ClassicAssert.AreEqual (query.Fields, [nameof(person.FirstName); nameof(person.LastName)])
        }

    [<Test>]
    let ``insertIgnore -> ignores duplacates`` () =
        task {
            do! Persons.init conn
            let person = Persons.View.generate ()
            let! _ =
                insert {
                    into personsView
                    value person
                } |> conn.InsertAsync

            let! _ =
                insert {
                    into personsView
                    value person
                } |> conn.InsertIgnoreAsync

            let! fromDb =
                select {
                    for p in personsView do
                    orderBy p.Position
                }
                |> conn.SelectAsync<Persons.View>

            fromDb |> should haveCount 1
            fromDb |> Seq.head |> shouldEqual person
        }