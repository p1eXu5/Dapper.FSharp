module Dapper.FSharp.IntegrationTests.SQLite.DeleteTests

open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open Dapper.FSharp.SQLite
open Dapper.FSharp.Testing.Database
open Faqt
open Faqt.Operators

[<TestFixture>]
[<NonParallelizable>]
type DeleteTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn

    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Deletes single records``() =
        task {
        do! init.InitPersons()
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
        
        fromDb |> Seq.length |> _.Should().Be(9) |> ignore
        fromDb |> Seq.head |> _.Position.Should().Be(9) |> ignore
    }
    
    [<Test>]
    member _.`` Cancellation works``() =
        task {
            do! init.InitPersons()
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
    
    [<Test>]
    member _.``Deletes more records``() =
        task {
            do! init.InitPersons()
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

            fromDb |> Seq.length |> _.Should().Be(6) |> ignore
        }
