﻿module internal Dapper.FSharp.MySQL.GenericDeconstructor

open Dapper.FSharp

let private extractFieldsAndSplit<'a> (j:Join) =
    let table = j |> Join.tableName
    let f = typeof<'a> |> Reflection.getFields
    let fieldNames = f |> List.map (sprintf "%s.%s" table)
    fieldNames, f.Head

let private createSplitOn (xs:string list) = xs |> String.concat ","

let select1<'a> evalSelectQuery (q:SelectQuery) =
    let fields = typeof<'a> |> Reflection.getFields |> if q.Joins = [] then id else List.map (sprintf "%s.%s" q.Table)
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let joinMeta = JoinAnalyzer.getJoinMetadata q.Joins
    let query : string = evalSelectQuery fields meta joinMeta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList |> JoinAnalyzer.addToMap joinMeta
    query, pars

let select2<'a,'b> evalSelectQuery (q:SelectQuery) =
    let joinsArray = q.Joins |> Array.ofList
    let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
    let fieldsTwo, splitOn = extractFieldsAndSplit<'b> joinsArray.[0]
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let joinMeta = JoinAnalyzer.getJoinMetadata q.Joins
    let query : string = evalSelectQuery (fieldsOne @ fieldsTwo) meta joinMeta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList |> JoinAnalyzer.addToMap joinMeta
    query, pars, createSplitOn [splitOn]

let select3<'a,'b,'c> evalSelectQuery (q:SelectQuery) =
    let joinsArray = q.Joins |> Array.ofList
    let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
    let fieldsTwo, splitOn1 = extractFieldsAndSplit<'b> joinsArray.[0]
    let fieldsThree, splitOn2 = extractFieldsAndSplit<'c> joinsArray.[1]
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let joinMeta = JoinAnalyzer.getJoinMetadata q.Joins
    let query : string = evalSelectQuery (fieldsOne @ fieldsTwo @ fieldsThree) meta joinMeta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList |> JoinAnalyzer.addToMap joinMeta
    query, pars, createSplitOn [splitOn1;splitOn2]
    
let select4<'a,'b,'c,'d> evalSelectQuery (q:SelectQuery) =
    let joinsArray = q.Joins |> Array.ofList
    let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
    let fieldsTwo, splitOn1 = extractFieldsAndSplit<'b> joinsArray.[0]
    let fieldsThree, splitOn2 = extractFieldsAndSplit<'c> joinsArray.[1]
    let fieldsFour, splitOn3 = extractFieldsAndSplit<'d> joinsArray.[2]
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let joinMeta = JoinAnalyzer.getJoinMetadata q.Joins
    let query : string = evalSelectQuery (fieldsOne @ fieldsTwo @ fieldsThree @ fieldsFour) meta joinMeta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList |> JoinAnalyzer.addToMap joinMeta
    query, pars, createSplitOn [splitOn1;splitOn2;splitOn3]
    
let select5<'a,'b,'c,'d,'e> evalSelectQuery (q:SelectQuery) =
    let joinsArray = q.Joins |> Array.ofList
    let fieldsOne = typeof<'a> |> Reflection.getFields |> List.map (sprintf "%s.%s" q.Table)
    let fieldsTwo, splitOn1 = extractFieldsAndSplit<'b> joinsArray.[0]
    let fieldsThree, splitOn2 = extractFieldsAndSplit<'c> joinsArray.[1]
    let fieldsFour, splitOn3 = extractFieldsAndSplit<'d> joinsArray.[2]
    let fieldsFive, splitOn4 = extractFieldsAndSplit<'e> joinsArray.[3]
    // extract metadata
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let joinMeta = JoinAnalyzer.getJoinMetadata q.Joins
    let query : string = evalSelectQuery (fieldsOne @ fieldsTwo @ fieldsThree @ fieldsFour @ fieldsFive) meta joinMeta q
    let pars = WhereAnalyzer.extractWhereParams meta |> Map.ofList |> JoinAnalyzer.addToMap joinMeta
    query, pars, createSplitOn [splitOn1;splitOn2;splitOn3;splitOn4]

let private _insert evalInsertQuery (q:InsertQuery<_>) fields outputFields =
    let query : string = evalInsertQuery fields outputFields q
    let pars =
        q.Values
        |> List.map (Reflection.getValuesForFields fields >> List.zip fields)
        |> List.mapi (fun i values ->
            values |> List.map (fun (key,value) -> sprintf "%s%i" key i, Reflection.boxify value))
        |> List.collect id
        |> Map.ofList
    query, pars

let insert evalInsertQuery (q:InsertQuery<'a>) =
    let fields = 
        match q.Fields with
        | [] -> typeof<'a> |> Reflection.getFields
        | fields -> fields
    _insert evalInsertQuery q fields []

let private _update evalUpdateQuery (q:UpdateQuery<_>) fields =
    let whereMeta = WhereAnalyzer.getWhereMetadata [] q.Where
    match q.Value with
    | Some value ->
        let parameters =
            Reflection.getValuesForFields fields value
            |> List.map Reflection.boxify
            |> List.zip fields
            |> List.append (WhereAnalyzer.extractWhereParams whereMeta)
            |> Map.ofList

        let query = evalUpdateQuery (fields |> Choice1Of2) whereMeta q
        query, parameters
    | None ->
        let setExprMetaList = UpdateSetSection.build q.SetColumns
        let parameters =
            setExprMetaList.Parameters
            |> List.append (WhereAnalyzer.extractWhereParams whereMeta)
            |> Map.ofList
        let query = evalUpdateQuery (setExprMetaList |> Choice2Of2) whereMeta q
        query, parameters

let update<'a> evalUpdateQuery (q:UpdateQuery<'a>) : string * Map<string, obj>=
    let fields =
        match q.Value, q.Fields, q.SetColumns with
        | Some _, [], _ -> typeof<'a> |> Reflection.getFields
        | Some _, fields, _ -> fields
        | None, _, setCols -> setCols |> List.map fst
    _update evalUpdateQuery q fields

let private _delete evalDeleteQuery (q:DeleteQuery) outputFields =
    let meta = WhereAnalyzer.getWhereMetadata [] q.Where
    let pars = (WhereAnalyzer.extractWhereParams meta) |> Map.ofList
    let query : string = evalDeleteQuery outputFields meta q
    query, pars
    
let delete evalDeleteQuery (q:DeleteQuery) = _delete evalDeleteQuery q []