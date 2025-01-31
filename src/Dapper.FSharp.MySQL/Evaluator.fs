module internal Dapper.FSharp.MySQL.Evaluator

open System.Linq
open System.Text

let private safeTableName schema table =
    match schema, table with
    | None, table -> table |> asterixOrInQuotes
    | Some schema, table -> (schema |> asterixOrInQuotes) + "." + (table |> asterixOrInQuotes)

let evalBinary = function
    | And -> "AND"
    | Or -> "OR"
    | Add -> "+"

let evalOrderDirection = function
    | Asc -> "ASC"
    | Desc -> "DESC"

let rec evalWhere (meta:WhereAnalyzer.FieldWhereMetadata list) (w:Where) =
    match w with
    | Where.Empty -> ""
    | Where.Expr expr -> expr
    | Where.Column (field, comp) ->
        // TODO: check case when `where z > ... AND z > ...`
        let fieldMeta = meta |> List.find (fun x -> x.Key = (field,comp))
        let withField op = sprintf "%s %s @%s" (asterixOrInQuotes fieldMeta.Name) op fieldMeta.ParameterName
        match comp with
        | Eq _ -> withField "="
        | Ne _ -> withField "<>"
        | Gt _ -> withField ">"
        | Lt _ -> withField "<"
        | Ge _ -> withField ">="
        | Le _ -> withField "<="
        | In _ -> withField "IN"
        | NotIn _ -> withField "NOT IN"
        | Like _ -> withField "LIKE"
        | NotLike _ -> withField "NOT LIKE"
        | IsNull -> sprintf "%s IS NULL" fieldMeta.Name
        | IsNotNull -> sprintf "%s IS NOT NULL" fieldMeta.Name
    | Where.Binary(w1, comb, w2) ->
        match evalWhere meta w1, evalWhere meta w2 with
        | "", fq | fq , "" -> fq
        | fq1, fq2 -> sprintf "(%s %s %s)" fq1 (evalBinary comb) fq2
    | Where.Unary (Not, w) ->
        match evalWhere meta w with
        | "" -> ""
        | v -> sprintf "NOT (%s)" v

let evalOrderBy (xs:OrderBy list) =
    xs
    |> List.map (fun (n,s) -> sprintf "%s %s" n (evalOrderDirection s))
    |> String.concat ", "

let evalPagination (pag:Pagination) =
    match pag with
    | { Take = None; Skip = x } when x <= 0 -> ""
    | { Take = None; Skip = o } -> sprintf "LIMIT %i, %i" o (System.UInt64.MaxValue)
    | { Take = Some f; Skip = o } -> sprintf "LIMIT %i, %i" o f

let buildjoinType (meta:JoinAnalyzer.JoinMetadata list) = function
    | EqualsToColumn eqToCol -> (asterixOrInQuotes eqToCol)
    | EqualsToConstant con -> meta |> List.find (fun x -> x.Key = con) |> (fun x -> "@" + x.ParameterName)

let buildJoinOnMany meta joinType tableName (joinList: List<string * JoinType>) =
    joinList
    |> List.map (fun (colName, jt) -> sprintf "%s.%s=%s" (asterixOrInQuotes tableName) (asterixOrInQuotes colName) (buildjoinType meta jt))
    |> List.reduce (fun s1 s2 -> s1 + " AND " + s2 )
    |> sprintf " %s JOIN %s ON %s" joinType tableName

let evalJoins (meta:JoinAnalyzer.JoinMetadata list) (joins:Join list) =
    let sb = StringBuilder()
    let evalJoin = function
        | InnerJoin(table, list) -> buildJoinOnMany meta "INNER" table list
        | LeftJoin(table, list) -> buildJoinOnMany meta "LEFT" table list
    joins |> List.map evalJoin |> List.iter (sb.Append >> ignore)
    sb.ToString()

let evalAggregates (ags:Aggregate list) =
    let comparableName (column:string) (alias:string) =
        match column.Split '.' with
        | [| _ |] -> alias
        | [| table; _ |] -> sprintf "%s.%s" table alias
        | _ -> failwith "Aggregate column format should be either <table>.<column> or <column>"

    ags |> List.map (function
    | Count (column,alias) -> comparableName column alias, sprintf "COUNT(%s) AS %s" (asterixOrInQuotes column) (asterixOrInQuotes alias)
    | CountDistinct (column,alias) -> comparableName column alias, sprintf "COUNT(DISTINCT %s) AS %s" (asterixOrInQuotes column) (asterixOrInQuotes alias)
    | Avg (column,alias) -> comparableName column alias, sprintf "AVG(%s) AS %s" (asterixOrInQuotes column) (asterixOrInQuotes alias)
    | Sum (column,alias) -> comparableName column alias, sprintf "SUM(%s) AS %s" (asterixOrInQuotes column) (asterixOrInQuotes alias)
    | Min (column,alias) -> comparableName column alias, sprintf "MIN(%s) AS %s" (asterixOrInQuotes column) (asterixOrInQuotes alias)
    | Max (column,alias) -> comparableName column alias, sprintf "MAX(%s) AS %s" (asterixOrInQuotes column) (asterixOrInQuotes alias)
    )

let replaceFieldWithAggregate (aggr:(string * string) list) (field:string) =
    aggr
    |> List.tryPick (fun (aggrColumn, replace) ->
        match aggrColumn.Split '.', field.Split '.' with
        | [| _; c |], [| _ |] when c = field -> Some replace // aggrColumn is <table>.<column> but field is <column>
        | [| _ |], [| _; c |] when aggrColumn = c -> Some replace // aggrColumn is <column> but field is <table>.<column>
        | _ when aggrColumn = field -> Some replace
        | _ -> None)
    |> Option.defaultValue (asterixOrInQuotes field)

let evalGroupBy (cols:string list) =
    cols
    |> List.map asterixOrInQuotes
    |> String.concat ", "

let evalSelectQuery fields meta joinMeta (q:SelectQuery) =
    let aggregates = q.Aggregates |> evalAggregates
    let fieldNames =
        fields
        |> List.map (replaceFieldWithAggregate aggregates)
        |> String.concat ", "
    // distinct
    let distinct = if q.Distinct then "DISTINCT " else ""
    // basic query
    let sb = StringBuilder(sprintf "SELECT %s%s FROM %s" distinct fieldNames (safeTableName q.Schema q.Table))
    // joins
    let joins = evalJoins joinMeta q.Joins
    if joins.Length > 0 then sb.Append joins |> ignore
    // where
    let where = evalWhere meta q.Where
    if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
    // group by
    let groupBy = evalGroupBy q.GroupBy
    if groupBy.Length > 0 then sb.Append (sprintf " GROUP BY %s" groupBy) |> ignore
    // order by
    let orderBy = evalOrderBy q.OrderBy
    if orderBy.Length > 0 then sb.Append (sprintf " ORDER BY %s" orderBy) |> ignore
    // pagination
    let pagination = evalPagination q.Pagination
    if pagination.Length > 0 then sb.Append (sprintf " %s" pagination) |> ignore
    sb.ToString()

let evalInsertIgnoreQuery fields _ (q:InsertQuery<_>) =
    let fieldNames = fields |> List.map asterixOrInQuotes |> String.concat ", " |> sprintf "(%s)"
    let values =
        q.Values
        |> List.mapi (fun i _ -> fields |> List.map (fun field -> sprintf "@%s%i" field i ) |> String.concat ", " |> sprintf "(%s)")
        |> String.concat ", "
    sprintf "INSERT IGNORE INTO %s %s VALUES %s" (safeTableName q.Schema q.Table) fieldNames values

let evalInsertQuery fields _ (q:InsertQuery<_>) =
    let fieldNames = fields |> List.map asterixOrInQuotes |> String.concat ", " |> sprintf "(%s)"
    let values =
        q.Values
        |> List.mapi (fun i _ -> fields |> List.map (fun field -> sprintf "@%s%i" field i ) |> String.concat ", " |> sprintf "(%s)")
        |> String.concat ", "
    sprintf "INSERT INTO %s %s VALUES %s" (safeTableName q.Schema q.Table) fieldNames values

let evalUpdateQuery (fieldsChoice: Choice<string list, UpdateSetSection>) meta (q:UpdateQuery<'a>) =
    // basic query
    let fieldAssignments =
        match fieldsChoice with
        | Choice1Of2 fields ->
            fields |> List.map (fun x -> sprintf "%s=@%s" (asterixOrInQuotes x) x) |> String.concat ", "
        | Choice2Of2 setExprMetaList ->
            setExprMetaList.SqlScript

    let baseQuery = sprintf "UPDATE %s SET %s" (safeTableName q.Schema q.Table) fieldAssignments
    let sb = StringBuilder(baseQuery)
    // where
    let where = evalWhere meta q.Where
    if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
    sb.ToString()

let evalDeleteQuery _ meta (q:DeleteQuery) =
    let baseQuery = sprintf "DELETE FROM %s" (safeTableName q.Schema q.Table)
    // basic query
    let sb = StringBuilder(baseQuery)
    // where
    let where = evalWhere meta q.Where
    if where.Length > 0 then sb.Append (sprintf " WHERE %s" where) |> ignore
    sb.ToString()