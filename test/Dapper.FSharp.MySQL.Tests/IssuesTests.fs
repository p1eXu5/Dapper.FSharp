namespace Dapper.FSharp.MySQL.Tests

open NUnit.Framework
open NUnit.Framework.Legacy
open Dapper.FSharp.MySQL
open Dapper.FSharp.Testing.Database
open Dapper.FSharp.MySQL.Tests.Database
open MySql.Data.MySqlClient

[<NonParallelizable>]
module IssuesTests =
    let personsView = table'<Persons.View> "Persons"
    let dogsView = table'<Dogs.View> "Dogs"

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
    let ``select with inner join over constant #62``() = 
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1to1 persons
            
            let thirdPerson = persons.[2]
            let thirdDog = dogs.[2]
            let thirdPersonId = thirdPerson.Id
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values dogs
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on ((p.Id, thirdPersonId) = (d.OwnerId,d.OwnerId))
                    selectAll
                } |> conn.SelectAsync<Persons.View, Dogs.View>
            
            ClassicAssert.AreEqual(1, Seq.length fromDb)
            ClassicAssert.AreEqual((thirdPerson, thirdDog), (Seq.head fromDb))
        }
    
    [<Test>]
    let ``select with left join over constant #62``() = 
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let persons = Persons.View.generateMany 10
            let secondPerson = persons.[1]
            let secondPersonId = secondPerson.Id
            
            let dogs = Dogs.View.generate1toN 5 secondPerson
            
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
            let! _ =
                insert {
                    into dogsView
                    values dogs
                } |> conn.InsertAsync
            let! fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on ((p.Id, secondPersonId) = (d.OwnerId,d.OwnerId))
                    selectAll
                } |> conn.SelectAsyncOption<Persons.View, Dogs.View>
            
            ClassicAssert.AreEqual(14, Seq.length fromDb)
        }
        
    [<Test>]
    let ``condition parameters works in both directions``() = 
        task {
            do! Persons.init conn

            let persons = Persons.View.generateMany 10
            
            let! _ =
                insert {
                    into personsView
                    values persons
                } |> conn.InsertAsync
            
            let filterObj = {| Id = 5 |}
            
            let! resultsA =
                select {
                    for p in personsView do
                    where (filterObj.Id = p.Position)
                }
                |> conn.SelectAsync<Persons.View>

            let! resultsB =
                select {
                    for p in personsView do
                    where (p.Position = filterObj.Id)
                }
                |> conn.SelectAsync<Persons.View>

            ClassicAssert.AreEqual(1, Seq.length resultsA)
            ClassicAssert.AreEqual(5, resultsA |> Seq.head |> (fun x -> x.Position))

            ClassicAssert.AreEqual(1, Seq.length resultsB)
            ClassicAssert.AreEqual(5, resultsB |> Seq.head |> (fun x -> x.Position))
        }