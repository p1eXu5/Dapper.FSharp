namespace Dapper.FSharp.MySQL

open System.Text
open Dapper.FSharp
open LinqExpressionVisitors

type internal UpdateSetSection =
    {
        SqlScript: string
        Parameters: (string * obj) list
    }

module internal UpdateSetSection = 

    let build (setColumnList: (string * SetExpr) list) =
        let rec parse setColumnName (sb: StringBuilder) (parameters: (string * obj) list) (setExpr: SetExpr) =
            match setExpr with
            | SetExpr.Value v ->
                let parameterName = sprintf "Set_%s%i" setColumnName parameters.Length
                (
                    sb.Append('@').Append(parameterName)
                    , (parameterName, v |> Reflection.boxify) :: parameters
                )

            | SetExpr.Column columnName ->
                (sb.AppendFormat("`{0}`", columnName), parameters)

            | SetExpr.Binary (e1, op, e2) ->
                parse setColumnName sb parameters e1
                |> fun (sb, parameters) ->
                    match op with
                    | BinaryOperation.Add ->
                        sb.Append(" + "), parameters
                    | BinaryOperation.Sub ->
                        sb.Append(" - "), parameters
                    | _ -> notImplMsg ($"Cannot build set expresion with operation {op}")
                |> fun (sb, parameters) ->
                    parse setColumnName sb parameters e2

            | SetExpr.Unary (op, expr) ->
                match op with
                | BinaryOperation.Sub ->
                    sb.Append("-"), parameters
                | _ -> notImplMsg ($"Cannot build set expresion with operation {op}")
                |> fun (sb, parameters) ->
                    parse setColumnName sb parameters expr


        if setColumnList.Length = 0 then
            { SqlScript = ""; Parameters = [] }
        else
            setColumnList
            |> List.fold (fun (sb: StringBuilder, parameters) (setColumnName, setExpr) ->
                let (sb, parameters) =
                    parse
                        setColumnName
                        (sb.AppendFormat("`{0}` = ", setColumnName))
                        parameters
                        setExpr
                (sb.Append(", "), parameters)
            ) (StringBuilder(), [])
            |> fun (sb, parameters) ->
                {
                    SqlScript = sb.Remove(sb.Length - 2, 2).ToString()
                    Parameters = parameters
                }
