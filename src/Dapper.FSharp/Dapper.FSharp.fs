﻿namespace Dapper.FSharp

type OrderDirection =
    | Asc
    | Desc

type OrderBy = string * OrderDirection
    
type WhereComparison =
    | Eq of obj
    | Ne of obj
    | Gt of obj
    | Lt of obj
    | Ge of obj
    | Le of obj
    | In of obj list
    | NotIn of obj list
    | IsNull
    | IsNotNull
    
type WhereCombination =
    | And
    | Or

type Where =
    | Empty
    | Column of string * WhereComparison
    | Binary of Where * WhereCombination * Where
    static member (+) (a, b) = Binary(a, And, b)
    static member (*) (a, b) = Binary(a, Or, b)

type Pagination =
    | Skip of skip:int
    | SkipTake of skip:int * take:int

type Join =
    | InnerJoin of table:string * colName:string * equalsToColumn:string
    | LeftJoin of table:string * colName:string * equalsToColumn:string

module Join =
    let tableName = function
        | InnerJoin (t,_,_)
        | LeftJoin (t,_,_) -> t

type SelectQuery = {
    Table : string
    Where : Where
    OrderBy : OrderBy list
    Pagination : Pagination
    Joins : Join list
}

type InsertQuery<'a> = {
    Table : string
    Values : 'a list
}

type UpdateQuery<'a> = {
    Table : string
    Value : 'a
    Where : Where
}

type DeleteQuery = {
    Table : string
    Where : Where
}