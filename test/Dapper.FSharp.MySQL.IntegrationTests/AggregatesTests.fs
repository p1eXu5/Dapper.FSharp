namespace Dapper.FSharp.MySQL.Tests

open NUnit.Framework
open NUnit.Framework.Legacy
open MySql.Data.MySqlClient
open Dapper.FSharp.MySQL
open Dapper.FSharp.Testing.Database
open Dapper.FSharp.MySQL.Tests.Database

[<TestFixture>]
[<NonParallelizable>]
module AggregatesTests =
    let dogsView = table'<Dogs.View> "Dogs"
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
    let ``selects with COUNT aggregate function``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    count "*" "Value"
                }
                |> conn.SelectAsync<{| Value : int64 |}>
                |> taskToList
                
            ClassicAssert.AreEqual(10L, fromDb.Head.Value)
        }

    [<Test>]
    let ``selects with COUNTBY aggregate function``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    countBy p.Id
                }
                |> conn.SelectAsync<{| Id : int64 |}>
                |> taskToList

            ClassicAssert.AreEqual(10L, fromDb.Head.Id)
        }


    [<Test>]
    let ``selects with COUNT aggregate function + column``() =
        task {
            do! Persons.init conn
            let rs =
                Persons.View.generateMany 10
                |> List.mapi (fun i x -> if i > 4 then { x with Position = 10 } else { x with Position = i + 1 })
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    count "*" "Value"
                    orderByDescending p.Position
                    groupBy p.Position
                }
                |> conn.SelectAsync<{| Value : int64; Position : int |}>
                |> taskToList
            
            ClassicAssert.AreEqual(6, fromDb.Length)
            ClassicAssert.AreEqual(10, fromDb.Head.Position)
            ClassicAssert.AreEqual(5L, fromDb.Head.Value)
        }
    
    [<Test>]
    let ``selects with COUNT aggregate function + WHERE``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    count "*" "Value"
                    where (p.Position > 5)
                }
                |> conn.SelectAsync<{| Value : int64 |}>
                |> taskToList
            
            ClassicAssert.AreEqual(5L, fromDb.Head.Value)
        }
    
    [<Test>]
    let ``selects with AVG aggregate function``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    avg "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : decimal |}>
                |> taskToList
            ClassicAssert.AreEqual(5.5M, fromDb.Head.Value)
        }
    
    [<Test>]
    let ``selects with SUM aggregate function``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    sum "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : decimal |}>
                |> taskToList
            ClassicAssert.AreEqual(55M, fromDb.Head.Value)
        }
    
    [<Test>]
    let ``selects with MIN aggregate function``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    min "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            
            ClassicAssert.AreEqual(1, fromDb.Head.Value)
        }
    
    [<Test>]
    let ``selects with MAX aggregate function``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    max "Position" "Value"
                }
                |> conn.SelectAsync<{| Value : int |}>
                |> taskToList
            ClassicAssert.AreEqual(10, fromDb.Head.Value)
        }
    
    [<Test>]
    let ``select distinct``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let ps = Persons.View.generateMany 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    distinct
                }
                |> conn.SelectAsync<{| FirstName: string; Position: int |}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Length)
        }

    [<Test>]
    let ``select countDistinct``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let ps = Persons.View.generateMany 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    countDistinct "Persons.Id" "Value"
                }
                |> conn.SelectAsync<{|Value:int64|}>
                |> taskToList

            ClassicAssert.AreEqual(10L, fromDb.Head.Value)
        }

    [<Test>]
    let ``select countByDistinct``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let ps = Persons.View.generateMany 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    countByDistinct (p.Id)
                }
                |> conn.SelectAsync<{|Id:int64|}>
                |> taskToList

            ClassicAssert.AreEqual(10L, fromDb.Head.Id)
        }
    
    [<Test>]
    let ``selects with multiple aggregate functions``() =
        task {
            do! Persons.init conn
            let rs = Persons.View.generateMany 10
            let! _ =
                insert {
                    into personsView
                    values rs
                } |> conn.InsertAsync
            let fromDb =
                select {
                    for p in personsView do
                    max "Position" "MaxValue"
                    min "Position" "MinValue"
                }
                |> conn.SelectAsync<{| MaxValue : int; MinValue : int |}>
                |> taskToList

            ClassicAssert.AreEqual(10, fromDb.Head.MaxValue)
            ClassicAssert.AreEqual(1, fromDb.Head.MinValue)
        }
    
    [<Test>]
    let ``select group by aggregate``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let ps = Persons.View.generateMany 10
            let ds = Dogs.View.generate1toN 5 ps.Head
            let! _ =
                insert {
                    into personsView
                    values ps
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let one,two =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    count "Persons.Position" "Count"
                    groupBy (p.Id, p.Position, d.OwnerId)
                    orderBy p.Position
                }
                |> conn.SelectAsync<{| Id: System.Guid; Position:int; Count:int64 |}, {| OwnerId : System.Guid |}>
                |> taskToList
                |> List.head
                
            ClassicAssert.AreEqual(5L, one.Count)
            ClassicAssert.AreEqual(1, one.Position)
            ClassicAssert.AreEqual(one.Id, two.OwnerId)
        }

    [<Test>]
    let ``select count inner join, #92``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let px = Persons.View.generateMany 10
            let ds = Dogs.View.generate1toN 5 px.Head
            let! _ =
                insert {
                    into personsView
                    values px
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values ds
                } |> conn.InsertAsync

            let fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    count "*" "Count"
                }
                |> conn.SelectAsync<{| Count:int64 |}>
                |> taskToList
                |> List.head

            ClassicAssert.AreEqual(5, fromDb.Count)
        }