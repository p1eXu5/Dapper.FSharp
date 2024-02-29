module internal Dapper.FSharp.MySQL.WhereAnalyzer

open System.Linq
open Dapper.FSharp.Reflection

type FieldWhereMetadata = {
    Key : string * ColumnComparison
    Name : string
    ParameterName : string
}

let extractWhereParams (meta: FieldWhereMetadata list) =
    let fn (m:FieldWhereMetadata) =
        match m.Key |> snd with
        | Eq p | Ne p | Gt p
        | Lt p | Ge p | Le p -> (m.ParameterName, p) |> Some
        | In p | NotIn p ->
            match p |> Seq.tryHead with
            | Some h ->
                let x = ReflectiveListBuilder.BuildTypedResizeArray (h.GetType()) p
                (m.ParameterName, x) |> Some
            | None -> (m.ParameterName, p.ToArray() :> obj) |> Some
        | Like str -> (m.ParameterName, str :> obj) |> Some
        | NotLike str -> (m.ParameterName, str :> obj) |> Some
        | IsNull | IsNotNull -> None
    meta
    |> List.choose fn


/// Adds to meta list only cases with field references to extract parameters.
let rec getWhereMetadata (meta: FieldWhereMetadata list) (w: Where)  =
    match w with
    | Where.Empty -> meta
    | Where.Expr _ -> meta
    | Where.Column (field, comp) ->
        let parName =
            // calculate next parameter index
            meta
            |> List.filter (fun x -> System.String.Equals(x.Name, field, System.StringComparison.OrdinalIgnoreCase))
            |> List.length
            |> fun l -> sprintf "Where_%s%i" field (l + 1)
            |> normalizeParamName

        { Key = (field, comp); Name = field; ParameterName = parName } :: meta
        |> List.rev
    | Where.Binary(w1, _, w2) -> [w1;w2] |> List.fold getWhereMetadata meta
    | Where.Unary(_, w) -> w |> getWhereMetadata meta