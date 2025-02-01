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
module SelectTests =
    
    let personsView = table'<Persons.View> "Persons"
    let dogsView = table'<Dogs.View> "Dogs"
    let dogsWeightsView = table'<DogsWeights.View> "DogsWeights"
    let vaccinationsView = table'<DogVaccinations.View> "Vaccinations"
    let manufacturersView = table'<VaccinationManufacturers.View> "VaccinationManufacturers"

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
    let ``selects by single where condition``() =
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
                    where (p.Position = 5)
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual (rs |> List.find (fun x -> x.Position = 5), Seq.head fromDb)
        }

    [<Test>]
    let ``cancellation works`` () =
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
            let selectCrud query =
                conn.SelectAsync<Persons.View>(query, cancellationToken = cts.Token) :> Task
            let action () = 
                select {
                    for p in personsView do
                    where (p.Position = 5)
                } |> selectCrud
            
            Assert.ThrowsAsync<TaskCanceledException>(action) |> ignore
        }

    [<Test>]
    let ``cancellation works - one join``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1to1 persons
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
            use cts = new CancellationTokenSource()
            cts.Cancel()
            let selectCrud query =
                conn.SelectAsync<Persons.View, Dogs.View>(query, cancellationToken = cts.Token) :> Task
            let action () =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    selectAll
                } |> selectCrud

            Assert.ThrowsAsync<TaskCanceledException>(action) |> ignore
        }

    [<Test>]
    let ``selects by single where condition with table name used``() =
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
                    where (p.Position = 5)
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(rs |> List.find (fun x -> x.Position = 5), Seq.head fromDb)
        }
        
    [<Test>]
    let ``selects by IN where condition`` () =
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
                    where (isIn p.Position [5;6])
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            let exp1 = rs |> List.find (fun x -> x.Position = 5)
            let act1 = Seq.head fromDb
            ClassicAssert.AreEqual(exp1,act1)
            
            let exp2 = rs |> List.find (fun x -> x.Position = 6)
            let act2 = Seq.last fromDb
            ClassicAssert.AreEqual(exp2,act2)
        }
        
    [<Test>]
    let ``selects by NOT IN where condition``() =
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
                    where (isNotIn p.Position [1;2;3])
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(rs |> List.find (fun x -> x.Position = 4), Seq.head fromDb)
            ClassicAssert.AreEqual(rs |> List.find (fun x -> x.Position = 10), Seq.last fromDb)
            ClassicAssert.AreEqual(7, Seq.length fromDb)
        }
        
    [<Test>]
    let ``selects by IS NULL where condition``() =
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
                    where (p.DateOfBirth = None)
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual (rs |> List.find (fun x -> x.Position = 2), Seq.head fromDb)
            ClassicAssert.AreEqual (5, Seq.length fromDb)
    }
        
    [<Test>]
    let ``selects by IS NOT NULL where condition``() =
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
                    where (p.DateOfBirth <> None)
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual (rs |> List.find (fun x -> x.Position = 1), Seq.head fromDb)
            ClassicAssert.AreEqual (5, Seq.length fromDb)
        }
    
    [<Test>]
    let ``selects by LIKE where condition return matching rows``() =
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
                    where (like p.FirstName "First_1%")
                } |> conn.SelectAsync<Persons.View>
            
            CollectionAssert.IsNotEmpty fromDb
            ClassicAssert.AreEqual(2, Seq.length fromDb)
            ClassicAssert.IsTrue(fromDb |> Seq.forall (fun (p:Persons.View) -> p.FirstName.StartsWith "First"))
        }
    
    [<Test>]
    let ``selects by NOT LIKE where condition return matching rows``() =
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
                    where (notLike p.FirstName "First_1%")
                }
                |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(8, Seq.length fromDb)
        }
        
    [<Test>]
    let ``selects by NOT LIKE where condition do not return non-matching rows``() =
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
                    where (notLike p.FirstName "NonExistingName%")
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(10, Seq.length fromDb)
        }
    
    [<Test>]
    let ``selects by UNARY NOT where condition``() =
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
                    where (not(p.Position > 5 && p.DateOfBirth = None))
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(rs |> List.find (fun x -> x.Position = 9), Seq.last fromDb)
            ClassicAssert.AreEqual(7, Seq.length fromDb)
        }
        
    [<Test>]
    let ``selects by multiple where conditions``() =
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
                    where (p.Position > 2 && p.Position < 4)
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual (rs |> List.find (fun x -> x.Position = 3), Seq.head fromDb)
        }

    [<Test>]
    let ``selects by andWhere``() =
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
                    where (p.Position > 2)
                    andWhere (p.Position < 4)
                } |> conn.SelectAsync<Persons.View>

            ClassicAssert.AreEqual (rs |> List.find (fun x -> x.Position = 3), Seq.head fromDb)
            ClassicAssert.AreEqual(1, Seq.length fromDb)
        }

    [<Test>]
    let ``selects by orWhere``() =
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
                    where (p.Position < 2)
                    orWhere (p.Position > 8)
                } |> conn.SelectAsync<Persons.View>

            ClassicAssert.AreEqual(3, Seq.length fromDb)
        }

    [<Test>]
    let ``selects by just andWhere``() =
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
                    andWhere (p.Position < 2)
                } |> conn.SelectAsync<Persons.View>

            ClassicAssert.AreEqual (rs |> List.find (fun x -> x.Position = 1), Seq.head fromDb)
            ClassicAssert.AreEqual(1, Seq.length fromDb)
        }

    [<Test>]
    let ``selects by andWhere and orWhere``() =
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
                    where (p.Position > 2)
                    andWhere (p.Position < 4)
                    orWhere (p.Position > 9)
                } |> conn.SelectAsync<Persons.View>

            ClassicAssert.AreEqual(2, Seq.length fromDb)
        }

    [<TestCase(4)>]
    [<TestCase(7)>]
    [<TestCase(2)>]
    [<TestCase(null)>]
    let ``selects by andWhereIf`` pos =
        let pos = pos |> Option.ofNullable
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
                    where (p.Position > 2)
                    andWhereIf pos.IsSome (p.Position < pos.Value)
                } |> conn.SelectAsync<Persons.View>

            let expected = rs |> List.filter (fun x -> x.Position > 2 && Option.forall (fun p -> x.Position < p) pos) |> List.length
            ClassicAssert.AreEqual(expected, Seq.length fromDb)
        }

    [<TestCase(4)>]
    [<TestCase(7)>]
    [<TestCase(0)>]
    [<TestCase(null)>]
    let ``selects by orWhereIf`` pos =
        let pos = pos |> Option.ofNullable
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
                    where (p.Position < 2)
                    orWhereIf pos.IsSome (p.Position > pos.Value)
                } |> conn.SelectAsync<Persons.View>

            let expected = rs |> List.filter (fun x -> x.Position < 2 || Option.exists (fun p -> x.Position > p) pos) |> List.length
            ClassicAssert.AreEqual(expected, Seq.length fromDb)
        }

    [<Test>]
    let ``selects with order by``() =
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
                    orderByDescending p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(10, fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position))
        }
    
    [<Test>]
    let ``selects with skip parameter``() =
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
                    skip 5
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(6, fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position))
            ClassicAssert.AreEqual(5, fromDb |> Seq.length)
        }
    
    [<Test>]
    let ``selects with skipTake parameter``() =
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
                    skipTake 5 2
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(6, fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position))
            ClassicAssert.AreEqual(2, fromDb |> Seq.length)
        }
    
    [<Test>]
    let ``selects with skip and take parameters``() =
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
                    skip 5
                    take 2
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            ClassicAssert.AreEqual(6, fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position))
            ClassicAssert.AreEqual(2, fromDb |> Seq.length)
        }
    
    [<Test>]
    let ``selects with one inner join - 1:1``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1to1 persons
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
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    selectAll
                } |> conn.SelectAsync<Persons.View, Dogs.View>

            ClassicAssert.AreEqual(10, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head), (Seq.head fromDb))
        }
    
    [<Test>]
    let ``selects with one inner join - 1:N``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
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
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    selectAll
                } |> conn.SelectAsync<Persons.View, Dogs.View>

            let byOwner = fromDb |> Seq.groupBy fst

            ClassicAssert.AreEqual(5, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head), (Seq.head fromDb))
            ClassicAssert.AreEqual(1, Seq.length byOwner)
            ClassicAssert.AreEqual(5, byOwner |> Seq.head |> snd |> Seq.length)
        }

    [<Test>]
    let ``selects with one inner join - 1:N select only one table``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
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
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    distinct
                    selectAll
                } |> conn.SelectAsync<Persons.View>

            ClassicAssert.AreEqual(1, Seq.length fromDb)
            ClassicAssert.AreEqual(persons.Head, (Seq.head fromDb))
        }

    [<Test>]
    let ``selects with one left join``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
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
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    orderBy p.Position
                    thenBy d.Nickname
                } |> conn.SelectAsyncOption<Persons.View, Dogs.View>

            let byOwner = fromDb |> Seq.groupBy fst

            ClassicAssert.AreEqual(14, Seq.length fromDb)
            ClassicAssert.AreEqual(5, byOwner |> Seq.head |> snd |> Seq.length)
            ClassicAssert.IsTrue (fromDb |> Seq.last |> snd |> Option.isNone)
            ClassicAssert.AreEqual((dogs |> List.head |> Some), (fromDb |> Seq.head |> snd))
        }
    
    [<Test>]
    let ``selects with two inner joins - 1:1``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1to1 persons
            let weights = DogsWeights.View.generate1to1 dogs

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    orderBy p.Position
                }
                |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

            ClassicAssert.AreEqual(10, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head, weights.Head), (Seq.head fromDb))
        }
    
    [<Test>]
    let ``selects with two inner joins - 1:N``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
            let weights = DogsWeights.View.generate1toN 3 dogs.Head

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    orderBy p.Position
                    thenBy d.Nickname
                    thenBy dw.Year
                } |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View>

            ClassicAssert.AreEqual(3, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head, weights.Head), Seq.head fromDb)
        }
    
    [<Test>]
    let ``selects with two left joins``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
            let weights = DogsWeights.View.generate1toN 3 dogs.Head

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    leftJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    orderBy p.Position
                    thenBy d.Nickname
                    thenBy dw.Year
                } |> conn.SelectAsyncOption<Persons.View, Dogs.View, DogsWeights.View>

            let p1,d1,w1 = fromDb |> Seq.head
            ClassicAssert.AreEqual(persons.Head, p1)
            ClassicAssert.AreEqual(Some dogs.Head, d1)
            ClassicAssert.AreEqual(Some weights.Head, w1)

            let pn,dn,wn = fromDb |> Seq.last
            ClassicAssert.AreEqual((persons |> Seq.last), pn)
            ClassicAssert.AreEqual(None, dn)
            ClassicAssert.AreEqual(None, wn)
            ClassicAssert.AreEqual(16, Seq.length fromDb)
        }
        
    [<Test>]
    let ``selects with three inner joins - 1:1``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn
            do! Vaccinations.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1to1 persons
            let weights = DogsWeights.View.generate1to1 dogs
            let vaccinations = DogVaccinations.View.generate1to1 dogs

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into vaccinationsView
                    values vaccinations
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    innerJoin v in vaccinationsView on (d.Nickname = v.DogNickname)
                    orderBy p.Position
                }
                |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View, DogVaccinations.View>

            ClassicAssert.AreEqual(10, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head, weights.Head, vaccinations.Head), (Seq.head fromDb))
        }
    
    [<Test>]
    let ``selects with three inner joins - 1:N``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn
            do! Vaccinations.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
            let weights = DogsWeights.View.generate1toN 3 dogs.Head
            let vaccinations = DogVaccinations.View.generate1toN 3 dogs.Head

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into vaccinationsView
                    values vaccinations
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    leftJoin v in vaccinationsView on (dw.DogNickname = v.DogNickname)
                    orderBy p.Position
                    thenBy d.Nickname
                    thenBy dw.Year
                    thenBy v.Vaccination
                } |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View, DogVaccinations.View>

            ClassicAssert.AreEqual(9, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head, weights.Head, vaccinations.Head), Seq.head fromDb)
        }
        
    [<Test>]
    let ``selects with three left joins``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn
            do! Vaccinations.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
            let weights = DogsWeights.View.generate1toN 3 dogs.Head
            let vaccinations = DogVaccinations.View.generate1toN 3 dogs.Head

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into vaccinationsView
                    values vaccinations
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    leftJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    leftJoin v in vaccinationsView on (dw.DogNickname = v.DogNickname)
                    orderBy p.Position
                    thenBy d.Nickname
                    thenBy dw.Year
                    thenBy v.Vaccination
                } |> conn.SelectAsyncOption<Persons.View, Dogs.View, DogsWeights.View, DogVaccinations.View>

            let p1,d1,w1,v1 = fromDb |> Seq.head
            ClassicAssert.AreEqual(persons.Head, p1)
            ClassicAssert.AreEqual(Some dogs.Head, d1)
            ClassicAssert.AreEqual(Some weights.Head, w1)
            ClassicAssert.AreEqual(Some vaccinations.Head, v1)

            let pn,dn,wn,vn = fromDb |> Seq.last
            ClassicAssert.AreEqual((persons |> Seq.last), pn)
            ClassicAssert.AreEqual(None, dn)
            ClassicAssert.AreEqual(None, wn)
            ClassicAssert.AreEqual(None, vn)
        }
        
    [<Test>]
    let ``selects with four inner joins - 1:1``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn
            do! Vaccinations.init conn
            do! VaccinationManufacturers.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1to1 persons
            let weights = DogsWeights.View.generate1to1 dogs
            let vaccinations = DogVaccinations.View.generate1to1 dogs
            let manufacturers = VaccinationManufacturers.View.generate1to1 vaccinations

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into vaccinationsView
                    values vaccinations
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into manufacturersView
                    values manufacturers
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    innerJoin v in vaccinationsView on (d.Nickname = v.DogNickname)
                    innerJoin m in manufacturersView on (v.Vaccination = m.Vaccination)
                    orderBy p.Position
                }
                |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View, DogVaccinations.View, VaccinationManufacturers.View>

            ClassicAssert.AreEqual(10, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head, weights.Head, vaccinations.Head, manufacturers.Head), (Seq.head fromDb))
        }
    
    [<Test>]
    let ``selects with four inner joins - 1:N``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn
            do! Vaccinations.init conn
            do! VaccinationManufacturers.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
            let weights = DogsWeights.View.generate1toN 3 dogs.Head
            let vaccinations = DogVaccinations.View.generate1toN 3 dogs.Head
            let manufacturers = VaccinationManufacturers.View.generate1toN 3 vaccinations.Head

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into vaccinationsView
                    values vaccinations
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into manufacturersView
                    values manufacturers
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    innerJoin d in dogsView on (p.Id = d.OwnerId)
                    innerJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    innerJoin v in vaccinationsView on (dw.DogNickname = v.DogNickname)
                    innerJoin m in manufacturersView on (v.Vaccination = m.Vaccination)
                    orderBy p.Position
                    thenBy d.Nickname
                    thenBy dw.Year
                    thenBy m.Manufacturer
                } |> conn.SelectAsync<Persons.View, Dogs.View, DogsWeights.View, DogVaccinations.View, VaccinationManufacturers.View>

            ClassicAssert.AreEqual(9, Seq.length fromDb)
            ClassicAssert.AreEqual((persons.Head, dogs.Head, weights.Head, vaccinations.Head, manufacturers.Head), Seq.head fromDb)
        }
        
    [<Test>]
    let ``selects with four left joins``() =
        task {
            do! Persons.init conn
            do! Dogs.init conn
            do! DogsWeights.init conn
            do! Vaccinations.init conn
            do! VaccinationManufacturers.init conn

            let persons = Persons.View.generateMany 10
            let dogs = Dogs.View.generate1toN 5 persons.Head
            let weights = DogsWeights.View.generate1toN 3 dogs.Head
            let vaccinations = DogVaccinations.View.generate1toN 3 dogs.Head
            let manufacturers = VaccinationManufacturers.View.generate1toN 3 vaccinations.Head

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
            let! _ =
                insert {
                    into dogsWeightsView
                    values weights
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into vaccinationsView
                    values vaccinations
                } |> conn.InsertAsync
                
            let! _ =
                insert {
                    into manufacturersView
                    values manufacturers
                } |> conn.InsertAsync

            let! fromDb =
                select {
                    for p in personsView do
                    leftJoin d in dogsView on (p.Id = d.OwnerId)
                    leftJoin dw in dogsWeightsView on (d.Nickname = dw.DogNickname)
                    leftJoin v in vaccinationsView on (dw.DogNickname = v.DogNickname)
                    leftJoin m in manufacturersView on (v.Vaccination = m.Vaccination)
                    orderBy p.Position
                    thenBy d.Nickname
                    thenBy dw.Year
                    thenBy v.Vaccination
                    thenBy m.Manufacturer
                } |> conn.SelectAsyncOption<Persons.View, Dogs.View, DogsWeights.View, DogVaccinations.View, VaccinationManufacturers.View>

            let p1,d1,w1,v1,m1 = fromDb |> Seq.head
            ClassicAssert.AreEqual(persons.Head, p1)
            ClassicAssert.AreEqual(Some dogs.Head, d1)
            ClassicAssert.AreEqual(Some weights.Head, w1)
            ClassicAssert.AreEqual(Some vaccinations.Head, v1)
            ClassicAssert.AreEqual(Some manufacturers.Head, m1)

            let pn,dn,wn,vn, mn = fromDb |> Seq.last
            ClassicAssert.AreEqual((persons |> Seq.last), pn)
            ClassicAssert.AreEqual(None, dn)
            ClassicAssert.AreEqual(None, wn)
            ClassicAssert.AreEqual(None, vn)
            ClassicAssert.AreEqual(None, mn)
        }