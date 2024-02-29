namespace Dapper.FSharp.MySQL

open Dapper.FSharp

type InsertExpressionBuilder<'T>() =

    let getQueryOrDefault (state: QuerySource<'Result>) =
        match state with
        | :? QuerySource<'Result, InsertQuery<'T>> as qs -> qs.Query
        | _ -> 
            { Schema = None
              Table = ""
              Fields = []
              Values = [] } : InsertQuery<'T>

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        let tbl = state.GetOuterTableMapping()
        let query = state |> getQueryOrDefault
        QuerySource<'T, InsertQuery<'T>>({ query with Table = tbl.Name; Schema = tbl.Schema }, state.TableMappings)

    /// Sets the TABLE name for query.
    [<CustomOperation("into")>]
    member this.Into (state: QuerySource<'T>, table: QuerySource<'T>) =
        let tbl = table.GetOuterTableMapping()
        let query = state |> getQueryOrDefault
        QuerySource<'T, InsertQuery<'T>>({ query with Table = tbl.Name; Schema = tbl.Schema }, state.TableMappings)

    member this.Yield _ =
        QuerySource<'T>(Map.empty)

    /// Sets the list of values for INSERT
    [<CustomOperation("values", MaintainsVariableSpace = true)>]
    member this.Values (state: QuerySource<'T>, values:'T list) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, InsertQuery<'T>>({ query with Values = values }, state.TableMappings)

    /// Sets the single value for INSERT
    [<CustomOperation("value", MaintainsVariableSpace = true)>]
    member this.Value (state:QuerySource<'T>, value:'T) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, InsertQuery<'T>>({ query with Values = [value] }, state.TableMappings)

    /// Includes a list of columns in the insert query.
    [<CustomOperation("includeColumns", MaintainsVariableSpace = true)>]
    member this.IncludeColumns (state: QuerySource<'T>, [<ProjectionParameter>] propertySelectors) = 
        let query = state |> getQueryOrDefault
        let props = propertySelectors |> List.map LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> |> List.map (fun x -> x.Name)
        let newQuery = { query with Fields = (query.Fields @ props) }
        QuerySource<'T, InsertQuery<'T>>(newQuery, state.TableMappings)
    
    /// Includes a column in the insert query.
    [<CustomOperation("includeColumn", MaintainsVariableSpace = true)>]
    member this.IncludeColumn (state: QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        this.IncludeColumns(state, [propertySelector])

    /// Excludes a column from the insert query.
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
        QuerySource<'T, InsertQuery<'T>>(newQuery, state.TableMappings)

    /// Unwraps the query
    member this.Run (state: QuerySource<'T>) =
        state |> getQueryOrDefault
