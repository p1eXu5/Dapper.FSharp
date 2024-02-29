namespace Dapper.FSharp.MySQL

open System
open System.Linq.Expressions
open Dapper.FSharp.MySQL.LinqExpressionVisitors

type SelectExpressionBuilder<'T>() =

    let getQueryOrDefault (state: QuerySource<'Result>) = // 'Result allows 'T to vary as the result of joins
        match state with
        | :? QuerySource<'Result, SelectQuery> as qs -> qs.Query
        | _ -> 
            { Schema = None
              Table = ""
              Where = Where.Empty
              OrderBy = []
              Pagination = { Skip = 0; Take = None }
              Joins = []
              Aggregates = []
              GroupBy = []
              Distinct = false } : SelectQuery    

    let mergeTableMappings (a: Map<FQName, TableMapping>, b: Map<FQName, TableMapping>) =
        Map (Seq.concat [ (Map.toSeq a); (Map.toSeq b) ])

    member this.For (state: QuerySource<'T>, f: 'T -> QuerySource<'T>) =
        let tbl = state.GetOuterTableMapping()
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Table = tbl.Name; Schema = tbl.Schema }, state.TableMappings)

    member this.Yield _ =
        QuerySource<'T>(Map.empty)

    // Prevents errors while typing join statement if rest of query is not filled in yet.
    member this.Zero _ = 
        QuerySource<'T>(Map.empty)

    /// Sets the WHERE condition
    [<CustomOperation("where", MaintainsVariableSpace = true)>]
    member this.Where (state:QuerySource<'T>, [<ProjectionParameter>] whereExpression) = 
        let query = state |> getQueryOrDefault
        let where = LinqExpressionVisitors.visitWhere<'T> whereExpression (fullyQualifyColumn state.TableMappings)
        QuerySource<'T, SelectQuery>({ query with Where = where }, state.TableMappings)

    /// Combine existing WHERE condition with AND
    [<CustomOperation("andWhere", MaintainsVariableSpace = true)>]
    member this.AndWhere (state: QuerySource<'T, SelectQuery>, [<ProjectionParameter>] whereExpression ) =
        let state2 = this.Where(state, whereExpression)
        let where2 = if state.Query.Where = Where.Empty then state2.Query.Where else Where.Binary(state.Query.Where, And, state2.Query.Where)
        QuerySource<'T, SelectQuery>( { state.Query with Where = where2 }, state.TableMappings)

    /// Combine existing WHERE condition with OR
    [<CustomOperation("orWhere", MaintainsVariableSpace = true)>]
    member this.OrWhere (state: QuerySource<'T, SelectQuery>, [<ProjectionParameter>] whereExpression ) =
        let state2 = this.Where(state, whereExpression)
        let where2 = if state.Query.Where = Where.Empty then state2.Query.Where else Where.Binary(state.Query.Where, Or, state2.Query.Where)
        QuerySource<'T, SelectQuery>( { state.Query with Where = where2 }, state.TableMappings)

    /// Combine existing WHERE condition with AND only if condition is true
    [<CustomOperation("andWhereIf", MaintainsVariableSpace = true)>]
    member this.AndWhereIf (state: QuerySource<'T, SelectQuery>, condition: bool, [<ProjectionParameter>] whereExpression ) =
        if condition then this.AndWhere(state, whereExpression) else state

    /// Combine existing WHERE condition with OR only if condition is true
    [<CustomOperation("orWhereIf", MaintainsVariableSpace = true)>]
    member this.OrWhereIf (state: QuerySource<'T, SelectQuery>, condition: bool, [<ProjectionParameter>] whereExpression ) =
        if condition then this.OrWhere(state, whereExpression) else state

    /// Sets the ORDER BY for single column
    [<CustomOperation("orderBy", MaintainsVariableSpace = true)>]
    member this.OrderBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Asc)
        QuerySource<'T, SelectQuery>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the ORDER BY for single column
    [<CustomOperation("thenBy", MaintainsVariableSpace = true)>]
    member this.ThenBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Asc)
        QuerySource<'T, SelectQuery>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("orderByDescending", MaintainsVariableSpace = true)>]
    member this.OrderByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Desc)
        QuerySource<'T, SelectQuery>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the ORDER BY DESC for single column
    [<CustomOperation("thenByDescending", MaintainsVariableSpace = true)>]
    member this.ThenByDescending (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let orderBy = OrderBy (propertyName, Desc)
        QuerySource<'T, SelectQuery>({ query with OrderBy = query.OrderBy @ [orderBy] }, state.TableMappings)

    /// Sets the SKIP value for query
    [<CustomOperation("skip", MaintainsVariableSpace = true)>]
    member this.Skip (state:QuerySource<'T>, skip) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Pagination = { query.Pagination with Skip = skip } }, state.TableMappings)
    
    /// Sets the TAKE value for query
    [<CustomOperation("take", MaintainsVariableSpace = true)>]
    member this.Take (state:QuerySource<'T>, take) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Pagination = { query.Pagination with Take = Some take } }, state.TableMappings)

    /// Sets the SKIP and TAKE value for query
    [<CustomOperation("skipTake", MaintainsVariableSpace = true)>]
    member this.SkipTake (state:QuerySource<'T>, skip, take) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Pagination = { query.Pagination with Skip = skip; Take = Some take } }, state.TableMappings)

    /// INNER JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("innerJoin", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member this.InnerJoin (outerSource: QuerySource<'TOuter>, 
                      innerSource: QuerySource<'TInner>, 
                      outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                      innerKeySelector: Expression<Func<'TInner,'Key>>, 
                      resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTableMappings (outerSource.TableMappings, innerSource.TableMappings)
        let innerProperties = 
            LinqExpressionVisitors.visitJoin<'TInner, 'Key> innerKeySelector
            |> List.choose (function | MI mi -> Some mi | Const _ -> failwith "Left side in join must be a column.")

        let outerProperties = LinqExpressionVisitors.visitJoin<'TOuter, 'Key> outerKeySelector

        let innerTableName = 
            innerProperties             
            |> List.map (fun p -> mergedTables.[FQName.ofType p.DeclaringType])
            |> List.map (fun tbl -> 
                match tbl.Schema with
                | Some schema -> sprintf "%s.%s" schema tbl.Name
                | None -> tbl.Name
            )
            |> List.head

        match innerProperties, outerProperties with
        | [innerProperty], [MI outerProperty] -> 
            // Only only fully qualify outer column (because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}")
            let join = InnerJoin (innerTableName, [innerProperty.Name, outerProperty |> fullyQualifyColumn mergedTables |> EqualsToColumn ])
            let outerQuery = outerSource |> getQueryOrDefault
            QuerySource<'Result, SelectQuery>({ outerQuery with Joins = outerQuery.Joins @ [join] }, mergedTables)
        | _ -> 
            // Only only fully qualify outer column (because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}")
            let joinPairs = 
                List.zip innerProperties outerProperties 
                |> List.map (fun (innerProp, outerProp) -> 
                    match outerProp with
                    | MI outerProp -> 
                        innerProp.Name, (outerProp |> fullyQualifyColumn mergedTables |> EqualsToColumn)
                    | Const value -> 
                        innerProp.Name, (value |> EqualsToConstant)
                )
            let join = InnerJoin (innerTableName, joinPairs)
            let outerQuery = outerSource |> getQueryOrDefault
            QuerySource<'Result, SelectQuery>({ outerQuery with Joins = outerQuery.Joins @ [join] }, mergedTables)

    /// LEFT JOIN table where COLNAME equals to another COLUMN (including TABLE name)
    [<CustomOperation("leftJoin", MaintainsVariableSpace = true, IsLikeJoin = true, JoinConditionWord = "on")>]
    member this.LeftJoin (outerSource: QuerySource<'TOuter>, 
                          innerSource: QuerySource<'TInner>, 
                          outerKeySelector: Expression<Func<'TOuter,'Key>>, 
                          innerKeySelector: Expression<Func<'TInner,'Key>>, 
                          resultSelector: Expression<Func<'TOuter,'TInner,'Result>> ) = 

        let mergedTables = mergeTableMappings (outerSource.TableMappings, innerSource.TableMappings)
        let innerProperties = 
            LinqExpressionVisitors.visitJoin<'TInner, 'Key> innerKeySelector
            |> List.choose (function | MI mi -> Some mi | Const _ -> failwith "Left side in join must be a column.")

        let outerProperties = LinqExpressionVisitors.visitJoin<'TOuter, 'Key> outerKeySelector

        let innerTableName = 
            innerProperties 
            |> List.map (fun p -> mergedTables.[FQName.ofType p.DeclaringType])
            |> List.map (fun tbl -> 
                match tbl.Schema with
                | Some schema -> sprintf "%s.%s" schema tbl.Name
                | None -> tbl.Name
            )
            |> List.head

        match innerProperties, outerProperties with
        | [innerProperty], [MI outerProperty] -> 
            // Only only fully qualify outer column (because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}")
            let join = LeftJoin (innerTableName, [innerProperty.Name, outerProperty |> fullyQualifyColumn mergedTables |> EqualsToColumn])
            let outerQuery = outerSource |> getQueryOrDefault
            QuerySource<'Result, SelectQuery>({ outerQuery with Joins = outerQuery.Joins @ [join] }, mergedTables)
        | _ -> 
            // Only only fully qualify outer column (because Dapper.FSharp later appends "{innerTableName}.{innerPropertyName}")
            let joinPairs = 
                List.zip innerProperties outerProperties 
                |> List.map (fun (innerProp, outerProp) -> 
                    match outerProp with
                    | MI outerProp -> 
                        innerProp.Name, (outerProp |> fullyQualifyColumn mergedTables |> EqualsToColumn)
                    | Const value -> 
                        innerProp.Name, (value |> EqualsToConstant)
                )
            let join = LeftJoin (innerTableName, joinPairs)
            let outerQuery = outerSource |> getQueryOrDefault
            QuerySource<'Result, SelectQuery>({ outerQuery with Joins = outerQuery.Joins @ [join] }, mergedTables)

    /// Sets the GROUP BY for one or more columns.
    [<CustomOperation("groupBy", MaintainsVariableSpace = true)>]
    member this.GroupBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
        let query = state |> getQueryOrDefault
        let properties = LinqExpressionVisitors.visitGroupBy<'T, 'Prop> propertySelector (fullyQualifyColumn state.TableMappings)
        QuerySource<'T, SelectQuery>({ query with GroupBy = query.GroupBy @ properties}, state.TableMappings)

    /// COUNT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("count", MaintainsVariableSpace = true)>]
    member this.Count (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Count(colName, alias)] }, state.TableMappings)

    /// COUNT aggregate function for the selected column
    [<CustomOperation("countBy", MaintainsVariableSpace = true)>]
    member this.CountBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) =
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let alias = propertyName.Split('.') |> Array.last
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Count(propertyName, alias)] }, state.TableMappings)

    /// COUNT DISTINCT aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("countDistinct", MaintainsVariableSpace = true)>]
    member this.CountDistinct (state:QuerySource<'T>, colName, alias) =
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.CountDistinct(colName, alias)] }, state.TableMappings)

    /// COUNT DISTINCT aggregate function for the selected column
    [<CustomOperation("countByDistinct", MaintainsVariableSpace = true)>]
    member this.CountByDistinct (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) =
        let query = state |> getQueryOrDefault
        let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
        let alias = propertyName.Split('.') |> Array.last
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.CountDistinct(propertyName, alias)] }, state.TableMappings)

    /// AVG aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("avg", MaintainsVariableSpace = true)>]
    member this.Avg (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Avg(colName, alias)] }, state.TableMappings)

    /// AVG aggregate function for the selected column
    //[<CustomOperation("avgBy", MaintainsVariableSpace = true)>]
    //member this.AvgBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
    //    let query = state |> getQueryOrDefault
    //    let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
    //    QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Avg(propertyName, propertyName)] }, state.TableMappings)
    
    /// SUM aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("sum", MaintainsVariableSpace = true)>]
    member this.Sum (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Sum(colName, alias)] }, state.TableMappings)

    /// SUM aggregate function for the selected column
    //[<CustomOperation("sumBy", MaintainsVariableSpace = true)>]
    //member this.SumBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
    //    let query = state |> getQueryOrDefault
    //    let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
    //    QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Sum(propertyName, propertyName)] }, state.TableMappings)
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("min", MaintainsVariableSpace = true)>]
    member this.Min (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Min(colName, alias)] }, state.TableMappings)

    /// MIN aggregate function for the selected column
    //[<CustomOperation("minBy", MaintainsVariableSpace = true)>]
    //member this.MinBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
    //    let query = state |> getQueryOrDefault
    //    let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
    //    QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Min(propertyName, propertyName)] }, state.TableMappings)
    
    /// MIN aggregate function for COLNAME (or * symbol) and map it to ALIAS
    [<CustomOperation("max", MaintainsVariableSpace = true)>]
    member this.Max (state:QuerySource<'T>, colName, alias) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Max(colName, alias)] }, state.TableMappings)

    /// MIN aggregate function for the selected column
    //[<CustomOperation("maxBy", MaintainsVariableSpace = true)>]
    //member this.MaxBy (state:QuerySource<'T>, [<ProjectionParameter>] propertySelector) = 
    //    let query = state |> getQueryOrDefault
    //    let propertyName = LinqExpressionVisitors.visitPropertySelector<'T, 'Prop> propertySelector |> fullyQualifyColumn state.TableMappings
    //    QuerySource<'T, SelectQuery>({ query with Aggregates = query.Aggregates @ [Aggregate.Max(propertyName, propertyName)] }, state.TableMappings)
    
    /// Sets query to return DISTINCT values
    [<CustomOperation("distinct", MaintainsVariableSpace = true)>]
    member this.Distinct (state:QuerySource<'T>) = 
        let query = state |> getQueryOrDefault
        QuerySource<'T, SelectQuery>({ query with Distinct = true }, state.TableMappings)

    /// Selects all (needed only when there are no other clauses after "for" or "join")
    [<CustomOperation("selectAll", MaintainsVariableSpace = true)>]
    member this.SelectAll (state:QuerySource<'T>) = 
        state :?> QuerySource<'T, SelectQuery>

    /// Unwraps the query
    member this.Run (state: QuerySource<'T>) =
        state |> getQueryOrDefault
