namespace Dapper.FSharp.MySQL

open System
open System.Linq.Expressions
open Dapper.FSharp

type UpdateExpressionBuilder<'T>() =
    
    let getQueryOrDefault (state: QuerySource<'Result>) =
        match state with
        | :? QuerySource<'Result, UpdateQuery<'T>> as qs -> qs.Query
        | _ -> UpdateQuery.Default<'T>()

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        let tbl = state.GetOuterTableMapping()
        let query = state |> getQueryOrDefault
        QuerySource<'T, UpdateQuery<'T>>({ query with Table = tbl.Name; Schema = tbl.Schema }, state.TableMappings)

    member this.Yield _ =
        QuerySource<'T>(Map.empty)

    /// Sets a record to UPDATE
    [<CustomOperation("set", MaintainsVariableSpace = true)>]
    member this.Set (state: QuerySource<'T>, value: 'T) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, UpdateQuery<'T>>({ query with Value = Some value }, state.TableMappings)

    ///// Sets an individual column to UPDATE
    //[<CustomOperation("setColumn", MaintainsVariableSpace = true)>]
    //member _.SetColumn (state: QuerySource<'T>, [<ProjectionParameter>] propertySelector: Expression<Func<'T, 'Prop>>, value: 'Prop) = 
    //    let query = state |> getQueryOrDefault
    //    let prop = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector :?> Reflection.PropertyInfo
    //    QuerySource<'T, UpdateQuery<'T>>(
    //        {
    //            query with
    //                SetColumns = query.SetColumns @ [ prop.Name, box value |> SetExpr.Value ]
    //        },
    //        state.TableMappings
    //    )

    /// Sets an individual column to UPDATE
    [<CustomOperation("setColumn", MaintainsVariableSpace = true)>]
    member _.SetColumn (state: QuerySource<'T>, [<ProjectionParameter>] propertySelector: Expression<Func<'T, 'Prop>>, [<ProjectionParameter>] setExpression) = 
        let query = state |> getQueryOrDefault
        let prop = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector :?> Reflection.PropertyInfo
        let setExpr = LinqExpressionVisitors.visitSetExpr<'T, _> setExpression columnName
        QuerySource<'T, UpdateQuery<'T>>(
            {
                query with
                    SetColumns = query.SetColumns @ [ (prop.Name, setExpr) ]
            },
            state.TableMappings
        )

    /// Includes a column in the update query.
    [<CustomOperation("includeColumn", MaintainsVariableSpace = true)>]
    member this.IncludeColumn (state: QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let prop = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector
        let newQuery = { query with Fields = (query.Fields @ [prop.Name]) }
        QuerySource<'T, UpdateQuery<'T>>(newQuery, state.TableMappings)

    /// Excludes a column from the update query.
    [<CustomOperation("excludeColumn", MaintainsVariableSpace = true)>]
    member this.ExcludeColumn (state: QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let prop = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector
        let newQuery = 
            query.Fields
            |> function
                | [] -> Reflection.getFields typeof<'T>
                | fields -> fields
            |> List.filter (fun f -> f <> prop.Name)
            |> (fun x -> { query with Fields = x })
        QuerySource<'T, UpdateQuery<'T>>(newQuery, state.TableMappings)

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member this.Where (state: QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let query = state |> getQueryOrDefault
        let where = LinqExpressionVisitors.visitWhere<'T> whereExpression (fullyQualifyColumn state.TableMappings)
        QuerySource<'T, UpdateQuery<'T>>({ query with Where = where }, state.TableMappings)

    /// Unwraps the query
    member this.Run (state: QuerySource<'T>) =
        state |> getQueryOrDefault

