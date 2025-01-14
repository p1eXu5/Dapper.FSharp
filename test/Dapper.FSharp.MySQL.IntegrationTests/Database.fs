module Dapper.FSharp.MySQL.Tests.Database

open System
open System.Data
open MySql.Data.MySqlClient

open Dapper.FSharp.Testing.Database
open Dapper.FSharp.Testing.Extensions

let getConnection2 () =
    new MySqlConnection(SetUpDbContainer.MySqlContainer.GetConnectionString())

module Persons =

    let init (conn:IDbConnection) =
        task {
            do! "drop table Persons" |> conn.ExecuteCatchIgnore
            do!
                """
                create table Persons
                (
                    Id char(36) not null,
                    FirstName nvarchar(255) not null,
                    LastName longtext not null,
                    Position int not null,
                    DateOfBirth datetime null
                );

                create unique index Persons_Id_uindex
                    on Persons (Id);
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Articles =

    let init (conn:IDbConnection) =
        task {
            do! "drop table Articles" |> conn.ExecuteCatchIgnore
            do!
                """
                create table Articles
                (
                    Id char(36) not null,
                    Title nvarchar(255) not null,
                );

                create unique index Articles_Id_uindex
                    on Articles (Id);
                """
                |> conn.ExecuteIgnore
        }

module Dogs =

    let init (conn:IDbConnection) =
        task {
            do! "drop table Dogs" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE Dogs (
                    Id char(36) not null,
                    OwnerId char(36) not null,
                    Nickname longtext not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Vaccinations =

    let init (conn:IDbConnection) =
        task {
            do! "drop table Vaccinations" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE Vaccinations (
                    PetOwnerId char(36) not null,
                    DogNickname longtext not null,
                    Vaccination longtext not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module VaccinationManufacturers =

    let init (conn:IDbConnection) =
        task {
            do! "drop table VaccinationManufacturers" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE VaccinationManufacturers (
                    Vaccination longtext not null,
                    Manufacturer longtext not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module DogsWeights =

    let init (conn:IDbConnection) =
        task {
            do! "drop table DogsWeights" |> conn.ExecuteCatchIgnore
            do!
                """
                create table DogsWeights (
                DogNickname longtext not null,
                Year smallint not null,
                Weight smallint not null
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Issues =

    module PersonsSimple =

        let init (conn:IDbConnection) =
            task {
                do! "drop table PersonsSimple" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table PersonsSimple (
                    Id int not null,
                    Name nvarchar(255) not null,
                    `Desc` nvarchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module PersonsSimpleDescs =

        let init (conn:IDbConnection) =
            task {
                do! "drop table PersonsSimpleDescs" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table PersonsSimpleDescs (
                    Id int not null,
                    `Desc` nvarchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module Group =
        let init (conn:IDbConnection) =
            task {
                do! "drop table `Group`" |> conn.ExecuteCatchIgnore
                do!
                    """
                    create table `Group`(
                    Id int not null,
                    Name nvarchar(255) not null
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module SchemedGroup =
        let init (conn:IDbConnection) =
            task {
                do! (sprintf "drop table `%s`.`SchemedGroup`" TestSchema) |> conn.ExecuteCatchIgnore
                do!
                    sprintf """
                    create table `%s`.`SchemedGroup`(
                    Id int not null,
                    SchemedName nvarchar(255) not null
                    )
                    """ TestSchema
                    |> conn.ExecuteIgnore
                return ()
            }

let getInitializer (conn:IDbConnection) =
    { new ICrudInitializer with
        member x.InitPersons () = Persons.init conn
        member x.InitPersonsSimple () = Issues.PersonsSimple.init conn
        member x.InitPersonsSimpleDescs () = Issues.PersonsSimpleDescs.init conn
        member x.InitArticles () = Articles.init conn
        member x.InitGroups () = Issues.Group.init conn
        member x.InitSchemedGroups () = Issues.SchemedGroup.init conn
        member x.InitDogs () = Dogs.init conn
        member x.InitDogsWeights () = DogsWeights.init conn
        member x.InitVaccinations () = Vaccinations.init conn
        member x.InitVaccinationManufacturers () = VaccinationManufacturers.init conn
    }