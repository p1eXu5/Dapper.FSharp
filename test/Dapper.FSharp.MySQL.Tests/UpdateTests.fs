namespace Dapper.FSharp.MySQL.Tests

open System
open System.Threading
open System.Threading.Tasks
open NUnit.Framework
open NUnit.Framework.Legacy
open Dapper.FSharp.MySQL
open Dapper.FSharp.Testing.Database
open Dapper.FSharp.MySQL.Tests.Database
open MySql.Data.MySqlClient

[<NonParallelizable>]
module UpdateTests =
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
    let ``updates single records``() = 
        task {
            do! Persons.init conn
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
            
            ClassicAssert.AreEqual(1, Seq.length fromDb)
            ClassicAssert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
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
            let updateCrud query =
                conn.UpdateAsync(query, cancellationToken = cts.Token) :> Task
            let action () = 
                update {
                    for p in personsView do
                    setColumn p.LastName "UPDATED"
                    where (p.Position = 2)
                } |> updateCrud

            Assert.DoesNotThrowAsync(action) |> ignore
        }
#endif

    [<Test>]
    let ``updates option field to None``() = 
        task {
            do! Persons.init conn
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
            
            ClassicAssert.IsTrue(fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth |> Option.isNone)
            ClassicAssert.AreEqual(2, fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position)
        }

    [<Test>]
    let ``updates more records``() = 
        task {
            do! Persons.init conn
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
            
            ClassicAssert.AreEqual(3, Seq.length fromDb)
        }
    
    [<Test>]
    let ``update with 2 included fields``() = 
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
                
            ClassicAssert.AreEqual(query.Fields, [nameof(person.FirstName); nameof(person.LastName)])
        }