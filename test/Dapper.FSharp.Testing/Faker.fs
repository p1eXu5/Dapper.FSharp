namespace Dapper.FSharp.Testing

open Bogus
open System.Runtime.CompilerServices
open System


[<AutoOpen>]
module Faker =

    let faker = Faker "en"


[<Extension>]
type FakerExtensions =

    [<Extension>]
    static member Option<'T>(faker: Faker, value: 'T) : 'T option =
        faker.Random.ArrayElement([| None; value |> Some |])

    [<Extension>]
    static member Optionf<'T>(faker: Faker, value: unit -> 'T) : 'T option =
        faker.Random.ArrayElement([|
            (value >> Some)
            fun _ -> None 
        |]) ()

    [<Extension>]
    static member DateOfBirth<'T>(faker: Faker) : DateTime =
        // MySql datetime stored with no milliseconds
        DateTimeOffset.FromUnixTimeSeconds(faker.Date.PastOffset(1).ToUnixTimeSeconds()).DateTime
