module Dapper.FSharp.IntegrationTests.SQLite.SelectTests

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
type SelectTests () =
    
    let personsView = table'<Persons.View> "Persons"
    let dogsView = table'<Dogs.View> "Dogs"
    let dogsWeightsView = table'<DogsWeights.View> "DogsWeights"
    let vaccinationsView = table'<DogVaccinations.View> "Vaccinations"
    let manufacturersView = table'<VaccinationManufacturers.View> "VaccinationManufacturers"
    let conn = Database.getConnection()
    let init = Database.getInitializer conn

    [<OneTimeSetUp>]
    member _.``Setup DB``() = conn |> Database.safeInit
        
    [<Test>]
    member _.``Selects by single where condition``() =
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
                    where (p.Position = 5)
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (_.Position >> (=) 5)) |> ignore
        }
    
    [<Test>]
    member _.``Cancellation works`` () =
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
    member _.``Cancellation works - one join``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

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
    member _.``Selects by single where condition with table name used``() =
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
                    where (p.Position = 5)
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (fun x -> x.Position = 5)) |> ignore
        }
        
    [<Test>]
    member _.``Selects by IN where condition`` () =
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
                    where (isIn p.Position [5;6])
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            let exp1 = rs |> List.find (fun x -> x.Position = 5)
            let act1 = Seq.head fromDb
            %act1.Should().Be(exp1)
            
            let exp2 = rs |> List.find (fun x -> x.Position = 6)
            let act2 = Seq.last fromDb
            %act2.Should().Be(exp2)
        }
        
    [<Test>]
    member _.``Selects by NOT IN where condition``() =
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
                    where (isNotIn p.Position [1;2;3])
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (fun x -> x.Position = 4)) |> ignore
            fromDb |> Seq.last |> _.Should().Be(rs |> List.find (fun x -> x.Position = 10)) |> ignore
            fromDb |> Seq.length |> _.Should().Be(7) |> ignore
        }
        
    [<Test>]
    member _.``Selects by IS NULL where condition``() =
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
                    where (p.DateOfBirth = None)
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (fun x -> x.Position = 2)) |> ignore
            fromDb |> Seq.length |> _.Should().Be(5) |> ignore
    }
        
    [<Test>]
    member _.``Selects by IS NOT NULL where condition``() =
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
                    where (p.DateOfBirth <> None)
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (fun x -> x.Position = 1)) |> ignore
            fromDb |> Seq.length |> _.Should().Be(5) |> ignore
        }
    
    [<Test>]
    member _.``Selects by LIKE where condition return matching rows``() =
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
                    where (like p.FirstName "First_1%")
                } |> conn.SelectAsync<Persons.View>
            
            %fromDb.Should().NotBeEmpty()
            fromDb |> Seq.length |> _.Should().Be(2) |> ignore
            %fromDb.Should().AllSatisfy(fun x -> x.FirstName.StartsWith "First_1")
        }
    
    [<Test>]
    member _.``Selects by NOT LIKE where condition return matching rows``() =
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
                    where (notLike p.FirstName "First_1%")
                }
                |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(8) 
        }
        
    [<Test>]
    member _.``Selects by NOT LIKE where condition do not return non-matching rows``() =
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
                    where (notLike p.FirstName "NonExistingName%")
                } |> conn.SelectAsync<Persons.View>
            
            %fromDb.Should().HaveLength(10)
        }
    
    [<Test>]
    member _.``Selects by UNARY NOT where condition``() =
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
                    where (not(p.Position > 5 && p.DateOfBirth = None))
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.last |> _.Should().Be(rs |> List.find (fun x -> x.Position = 9)) |> ignore
            %fromDb.Should().HaveLength(7)
        }
        
    [<Test>]
    member _.``Selects by multiple where conditions``() =
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
                    where (p.Position > 2 && p.Position < 4)
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (fun x -> x.Position = 3)) |> ignore
        }

    [<Test>]
    member _.``Selects by andWhere``() =
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
                    where (p.Position > 2)
                    andWhere (p.Position < 4)
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(1)
            fromDb |> Seq.head |> _.Should().Be(rs |> List.find (fun x -> x.Position = 3)) |> ignore
        }

    [<Test>]
    member _.``Selects by orWhere``() =
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
                    where (p.Position < 2)
                    orWhere (p.Position > 8)
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(3)
        }

    [<Test>]
    member _.``Selects by just andWhere``() =
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
                    andWhere (p.Position < 2)
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(1)
            %fromDb.Should().ContainExactlyOneItemMatching(fun x -> x.Position = 1)
        }

    [<Test>]
    member _.``Selects by andWhere and orWhere``() =
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
                    where (p.Position > 2)
                    andWhere (p.Position < 4)
                    orWhere (p.Position > 9)
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(2)
        }

    [<TestCase(4)>]
    [<TestCase(7)>]
    [<TestCase(2)>]
    [<TestCase(null)>]
    member _.``Selects by andWhereIf`` pos =
        let pos = pos |> Option.ofNullable
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
                    where (p.Position > 2)
                    andWhereIf pos.IsSome (p.Position < pos.Value)
                } |> conn.SelectAsync<Persons.View>

            let expected = rs |> List.filter (fun x -> x.Position > 2 && Option.forall (fun p -> x.Position < p) pos) |> List.length
            %fromDb.Should().HaveLength(expected)
        }

    [<TestCase(4)>]
    [<TestCase(7)>]
    [<TestCase(0)>]
    [<TestCase(null)>]
    member _.``Selects by orWhereIf`` pos =
        let pos = pos |> Option.ofNullable
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
                    where (p.Position < 2)
                    orWhereIf pos.IsSome (p.Position > pos.Value)
                } |> conn.SelectAsync<Persons.View>

            let expected = rs |> List.filter (fun x -> x.Position < 2 || Option.exists (fun p -> x.Position > p) pos) |> List.length
            %fromDb.Should().HaveLength(expected)
        }

    [<Test>]
    member _.``Selects with order by``() =
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
                    orderByDescending p.Position
                } |> conn.SelectAsync<Persons.View>
            
            fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position) |> _.Should().Be(10) |> ignore
        }
    
    [<Test>]
    member _.``Selects with skip parameter``() =
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
                    skip 5 10
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(5)
            fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position) |> _.Should().Be(6) |> ignore
        }
    
    [<Test>]
    member _.``Selects with skipTake parameter``() =
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
                    skipTake 5 2
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(2)
            fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position) |> _.Should().Be(6) |> ignore
        }
    
    [<Test>]
    member _.``Selects with skip and take parameters``() =
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
                    skip 5 2
                    orderBy p.Position
                } |> conn.SelectAsync<Persons.View>

            %fromDb.Should().HaveLength(2)
            fromDb |> Seq.head |> (fun (x:Persons.View) -> x.Position) |> _.Should().Be(6) |> ignore
        }

    [<Test>]
    member _.``Selects with one inner join - 1:1``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()

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

            %fromDb.Should().HaveLength(10)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head)) |> ignore
        }
    
    [<Test>]
    member _.``Selects with one inner join - 1:N``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
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

            %fromDb.Should().HaveLength(5)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head)) |> ignore

            %byOwner.Should().HaveLength(1)
            byOwner |> Seq.head |> snd |> Seq.length |> _.Should().Be(5) |> ignore
        }

    [<Test>]
    member _.``Selects with one inner join - 1:N select only one table``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
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


            %fromDb.Should().HaveLength(1)
            fromDb |> Seq.head |> _.Should().Be(persons.Head) |> ignore
        }

    [<Test>]
    member _.``Selects with one left join``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
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

            %fromDb.Should().HaveLength(14)
            byOwner |> Seq.head |> snd |> Seq.length |> _.Should().Be(5) |> ignore

            fromDb |> Seq.last |> snd |> Option.isNone |> _.Should().BeTrue() |> ignore
            fromDb |> Seq.head |> snd |> _.Should().Be(dogs |> List.head |> Some) |> ignore
        }
    
    [<Test>]
    member _.``Selects with two inner joins - 1:1``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()

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

            %fromDb.Should().HaveLength(10)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head, weights.Head)) |> ignore
        }
    
    [<Test>]
    member _.``Selects with two inner joins - 1:N``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()

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

            %fromDb.Should().HaveLength(3)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head, weights.Head)) |> ignore
        }
    
    [<Test>]
    member _.``Selects with two left joins``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()

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

            %fromDb.Should().HaveLength(16)

            let p1,d1,w1 = fromDb |> Seq.head
            %p1.Should().Be(persons.Head)
            %d1.Should().Be(Some dogs.Head)
            %w1.Should().Be(Some weights.Head)

            let pn,dn,wn = fromDb |> Seq.last
            %pn.Should().Be(persons |> Seq.last)
            %dn.Should().BeNone()
            %wn.Should().BeNone()
        }
    
    [<Test>]
    member _.``Selects with three inner joins - 1:1``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()
            do! init.InitVaccinations()

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

            %fromDb.Should().HaveLength(10)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head, weights.Head, vaccinations.Head)) |> ignore
        }
    
    [<Test>]
    member _.``Selects with three inner joins - 1:N``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()
            do! init.InitVaccinations()

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

            %fromDb.Should().HaveLength(9)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head, weights.Head, vaccinations.Head)) |> ignore
        }
        
    [<Test>]
    member _.``Selects with three left joins``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()
            do! init.InitVaccinations()

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
            %p1.Should().Be(persons.Head)
            %d1.Should().Be(Some dogs.Head)
            %w1.Should().Be(Some weights.Head)
            %v1.Should().Be(Some vaccinations.Head)

            let pn,dn,wn,vn = fromDb |> Seq.last
            %pn.Should().Be(persons |> Seq.last)
            %dn.Should().BeNone()
            %wn.Should().BeNone()
            %vn.Should().BeNone()
        }
        
    [<Test>]
    member _.``Selects with four inner joins - 1:1``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()
            do! init.InitVaccinations()
            do! init.InitVaccinationManufacturers()

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

            %fromDb.Should().HaveLength(10)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head, weights.Head, vaccinations.Head, manufacturers.Head)) |> ignore
        }
    
    [<Test>]
    member _.``Selects with four inner joins - 1:N``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()
            do! init.InitVaccinations()
            do! init.InitVaccinationManufacturers()

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

            %fromDb.Should().HaveLength(9)
            fromDb |> Seq.head |> _.Should().Be((persons.Head, dogs.Head, weights.Head, vaccinations.Head, manufacturers.Head)) |> ignore
        }
        
    [<Test>]
    member _.``Selects with four left joins``() =
        task {
            do! init.InitPersons()
            do! init.InitDogs()
            do! init.InitDogsWeights()
            do! init.InitVaccinations()
            do! init.InitVaccinationManufacturers()

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
            %p1.Should().Be(persons.Head)
            %d1.Should().Be(Some dogs.Head)
            %w1.Should().Be(Some weights.Head)
            %v1.Should().Be(Some vaccinations.Head)
            %m1.Should().Be(Some manufacturers.Head)

            let pn,dn,wn,vn, mn = fromDb |> Seq.last
            %pn.Should().Be(persons |> Seq.last)
            %dn.Should().BeNone()
            %wn.Should().BeNone()
            %vn.Should().BeNone()
            %mn.Should().BeNone()
        }