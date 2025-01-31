module Dapper.FSharp.Testing.Database

open System
open System.Threading.Tasks

let [<Literal>] DbName = "DapperFSharpTests"
let [<Literal>] TestSchema = "tests"

type ICrudInitializer =
    abstract member InitPersons : unit -> Task<unit>
    abstract member InitPersonsSimple : unit -> Task<unit>
    abstract member InitPersonsSimpleDescs : unit -> Task<unit>
    abstract member InitArticles : unit -> Task<unit>
    abstract member InitGroups : unit -> Task<unit>
    abstract member InitSchemedGroups : unit -> Task<unit>
    abstract member InitDogs : unit -> Task<unit>
    abstract member InitDogsWeights : unit -> Task<unit>
    abstract member InitVaccinations : unit -> Task<unit>
    abstract member InitVaccinationManufacturers : unit -> Task<unit>

let taskToList (t:Task<seq<'a>>) = t |> Async.AwaitTask |> Async.RunSynchronously |> Seq.toList

module Persons =

    type View = {
        Id : Guid
        FirstName : string
        LastName : string
        Position : int
        DateOfBirth : DateTime option
    }

    type ViewRequired = {
        Id : Guid
        FirstName : string
        LastName : string
        Position : int
    }

    module View =
        let generateWithPosition position =
            {
                Id = faker.Random.Uuid()
                FirstName = sprintf "First_%i" position
                LastName = faker.Name.LastName()
                DateOfBirth = if position % 2 > 0 then faker.DateOfBirth() |> Some else None
                Position = position
            }

        let generate () =
            generateWithPosition (faker.Random.Int(1, 100))

        /// Generates Persons.View with Position set from 1 to 10
        let generateMany (count: int) =
            if count < 1 then []
            else List.init count (fun i -> generateWithPosition (i + 1))

module Dogs =

    type View = {
        Id : Guid
        OwnerId : Guid
        Nickname : string
    }

    module View =
        let generate1to1 (owners:Persons.View list) =
            owners
            |> List.mapi (fun i x ->
                {
                    Id = System.Guid.NewGuid()
                    OwnerId = x.Id
                    Nickname = sprintf "Dog_%i" i
                }
            )

        let generate1toN count (owner:Persons.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    Id = Guid.NewGuid()
                    OwnerId = owner.Id
                    Nickname = sprintf "Dog_%i" i
                }
            )

module DogVaccinations =
    type View = {
        PetOwnerId: Guid
        DogNickname : string
        Vaccination: string
    }

    module View =
        let generate1to1 (dogs: Dogs.View list) =
            dogs
            |> List.mapi (fun i x ->
                {
                    PetOwnerId = x.OwnerId
                    DogNickname = x.Nickname
                    Vaccination = sprintf "Vaccination_%i" i
                }
            )

        let generate1toN count (dog: Dogs.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    PetOwnerId = dog.OwnerId
                    DogNickname = dog.Nickname
                    Vaccination = sprintf "Vaccination_%i" i
                }
            )
            
module VaccinationManufacturers =
    type View = {
        Vaccination: string
        Manufacturer: string
    }

    module View =
        let generate1to1 (dogs: DogVaccinations.View list) =
            dogs
            |> List.mapi (fun i x ->
                {
                    Vaccination = x.Vaccination
                    Manufacturer = sprintf "Manufacturer_%i" i
                }
            )

        let generate1toN count (dog: DogVaccinations.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    Vaccination = dog.Vaccination
                    Manufacturer = sprintf "Manufacturer_%i" i
                }
            )

module DogsWeights =

    type View = {
        DogNickname : string
        Year : int16
        Weight : int16
    }

    module View =
        let generate1to1 (dogs:Dogs.View list) =
            dogs
            |> List.mapi (fun i x ->
                {
                    DogNickname = x.Nickname
                    Year = 2010s + (int16 i)
                    Weight = 10s + (int16 i)
                }
            )

        let generate1toN count (dog:Dogs.View) =
            [1..count]
            |> List.map (fun i ->
                {
                    DogNickname = dog.Nickname
                    Year = 2010s + (int16 i)
                    Weight = 10s + (int16 i)
                }

            )

module Issues =

    module PersonsSimple =

        type View = {
            Id : int
            Name : string
            Desc : string
        }

        module View =
            let generate x =
                [1..x]
                |> List.map (fun x ->
                    {
                        Id = x
                        Name = sprintf "Name_%i" x
                        Desc = sprintf "Desc_%i" x
                    }
                )

    module PersonsSimpleDescs =

        type View = {
            Id : int
            Desc : string
        }

        module View =
            let generate x =
                [1..x]
                |> List.map (fun x ->
                    {
                        Id = x
                        Desc = sprintf "Desc_%i" x
                    }
                )

module Articles =

    type View = {
        Id : int option
        Title : string
    }

module Group =

    type View = {
        Id : int
        Name : string
    }