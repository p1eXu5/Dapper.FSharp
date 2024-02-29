namespace Dapper.FSharp.MySQL

open System
open System.Collections.Generic
open Dapper.FSharp

/// Fully qualified entity type name
type [<Struct>] FQName = private FQName of string

[<RequireQualifiedAccess>]
module FQName =
    let ofType (t: Type) = FQName t.FullName


type TableMapping = { Name: string; Schema: string option }


type QuerySource<'T>(tableMappings) =
    interface IEnumerable<'T> with
        member _.GetEnumerator() = Seq.empty<'T>.GetEnumerator() :> Collections.IEnumerator
        member _.GetEnumerator() = Seq.empty<'T>.GetEnumerator()
    
    member _.TableMappings : Map<FQName, TableMapping> = tableMappings
    
    member this.GetOuterTableMapping() = 
        let outerEntity = typeof<'T>
        let fqn = 
            if outerEntity.Name.StartsWith "Tuple" // True for joined tables
            then outerEntity.GetGenericArguments() |> Array.head |> FQName.ofType
            else outerEntity |> FQName.ofType
        this.TableMappings.[fqn]


type QuerySource<'T, 'Query>(query, tableMappings) = 
    inherit QuerySource<'T>(tableMappings)
    member _.Query : 'Query = query


[<AutoOpen>]
module internal Utils =
    /// Fully qualifies a column with: {?schema}.{table}.{column}
    let fullyQualifyColumn (tables: Map<FQName, TableMapping>) (property: Reflection.MemberInfo) =
        let tbl = tables.[property.DeclaringType |> FQName.ofType]
        match tbl.Schema with
        | Some schema -> sprintf "%s.%s.%s" schema tbl.Name property.Name
        | None -> sprintf "%s.%s" tbl.Name property.Name

    let columnName (property: Reflection.MemberInfo) =
        property.Name

    let normalizeParamName (s: string) = s.Replace(".","_")

    let specialStrings = [ "*" ]

    let asterixOrInQuotes (s:string) =
        s.Split('.')
        |> Array.map (fun x -> if specialStrings |> List.contains(x) then x else sprintf "`%s`" x)
        |> String.concat "."

[<AutoOpen>]
module Table = 

    /// Maps the entity 'T to a table of the exact same name.
    let table<'T> = 
        let ent = typeof<'T>
        let tables = Map [ent |> FQName.ofType, { Name = ent.Name; Schema = None }]
        QuerySource<'T>(tables)

    /// Maps the entity 'T to a table of the given name.
    let table'<'T> (tableName: string) = 
        let ent = typeof<'T>
        let tables = Map [FQName.ofType ent, { Name = tableName; Schema = None }]
        QuerySource<'T>(tables)

    /// Maps the entity 'T to a schema of the given name.
    let inSchema<'T> (schemaName: string) (qs: QuerySource<'T>) =
        let ent = typeof<'T>
        let fqn = FQName.ofType ent
        let tbl = qs.TableMappings.[fqn]
        let tables = qs.TableMappings.Add(fqn, { tbl with Schema = Some schemaName })
        QuerySource<'T>(tables)