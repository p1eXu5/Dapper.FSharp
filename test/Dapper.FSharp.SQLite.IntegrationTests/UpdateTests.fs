module Dapper.FSharp.IntegrationTests.SQLite.UpdateTests

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
type UpdateTests () =
    let personsView = table'<Persons.View> "Persons"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn

    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
    
    [<Test>]
    member _.``Updates single records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> conn.UpdateAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.LastName = "UPDATED")
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(1)
            fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position |> _.Should().Be(2) |> ignore
        }

    [<Test>]
    member _.``Cancellation works``() = 
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
            let updateCrud query =
                conn.UpdateAsync(query, cancellationToken = cts.Token) :> Task
            let action () = 
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> updateCrud

            Assert.ThrowsAsync<TaskCanceledException>(action) |> ignore
        }

    [<Test>]
    member _.``Updates option field to None``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generateMany 10 |> List.map (fun p -> { p with DateOfBirth = Some DateTime.UtcNow })
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                update {
                    for p in personsView do
                    setColumn p.DateOfBirth None
                    where (p.Position = 2)
                } |> conn.UpdateAsync
            let! fromDb =
                select {
                    for p in personsView do
                    where (p.Position = 2)
                } |> conn.SelectAsync<Persons.View>

            fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> _.Should().BeNone() |> ignore
            fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position |> _.Should().Be(2) |> ignore
        }

    [<Test>]
    member _.``Updates more records``() = 
        task {
            do! init.InitPersons()
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let! _ =
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position > 7)
                } |> conn.UpdateAsync

            let! fromDb =
                select {
                    for p in personsView do
                    where (p.LastName = "UPDATED")
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(3)
        }
    
    [<Test>]
    member _.``Update with 2 included fields``() = 
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
                update {
                    for p in table<Persons.View> do
                    set person
                    includeColumn p.FirstName
                    includeColumn p.LastName
                }

            %query.Fields.Should().Be([nameof(person.FirstName); nameof(person.LastName)])
        }
