module Dapper.FSharp.IntegrationTests.SQLite.InsertTests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.SQLite
open Dapper.FSharp.Testing.Database
open Faqt
open Faqt.Operators

[<TestFixture>]
[<NonParallelizable>]
type InsertTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn
    
    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Inserts new record``() = 
        task {
            do! init.InitPersons()
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
            
           %(Seq.head fromDb).Should().Be(r)
        }
    
    [<Test>]
    member _.``Cancellation works``() = 
        task {
            do! init.InitPersons()
            let r = Persons.View.generate ()

            use cts = new CancellationTokenSource()
            cts.Cancel()
            let insertCrud query =
                conn.InsertAsync(query, cancellationToken = cts.Token) :> Task
            let action () = 
                insert {
                    into personsView
                    value r
                } |> insertCrud 
            
            Assert.ThrowsAsync<TaskCanceledException>(action) |> ignore
        }
    
    [<Test>]
    member _.``Inserts partial record``() = 
        task {        
            let personsRequired = table'<Persons.ViewRequired> "Persons"

            do! init.InitPersons()
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
            
            %(Seq.head fromDb).Should().Be(r)
        }
    
    [<Test>]
    member _.``Inserts partial record using 'excludeColumn'``() = 
        task {        
            let personsView = table'<Persons.View> "Persons"

            do! init.InitPersons()
            let r =
                Persons.View.generate ()

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
            
            fromDb |> Seq.head |> _.Should().Be({ r with DateOfBirth = None }) |> ignore
        }
    
    [<Test>]
    member _.``Inserts more records``() = 
        task {
            do! init.InitPersons()
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
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveSameItemsAs(rs)
        }
    
    [<Test>]
    member _.``Insert with 2 included fields``() = 
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

            %query.Fields.Should().Be([nameof(person.FirstName); nameof(person.LastName)])
        }

    [<Test>]
    member _.``Replace or Insert works``() = 
        task {
            do! init.InitPersons()

            let rs = Persons.View.generateMany 10

            // insert initial data
            let! _ = 
                insert { 
                    into personsView
                    values rs
                }
                |> conn.InsertOrReplaceAsync

            // modify everything
            let modified = 
                rs
                |> List.map (fun x -> { x with FirstName = "Updated" + x.FirstName })

            // insert again, everything should be updated
            let! conflictCount = 
                insert {
                    into personsView
                    values modified
                }
                |> conn.InsertOrReplaceAsync

            // get data back and ensure everything is modified
            let! fromDb = select { for p in personsView do orderBy p.Position } |> conn.SelectAsync<Persons.View>

            %conflictCount.Should().Be(10)
            %fromDb.Should().HaveSameItemsAs(modified)
        }