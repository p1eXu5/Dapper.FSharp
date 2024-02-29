namespace Dapper.FSharp.MySQL.UnitTests

open System
open NUnit.Framework
open Dapper.FSharp.MySQL

type UpdateSetSection () =

    static member SetColumnsCases =
        seq {
            TestCaseData(
                List.empty<string * SetExpr>,
                { SqlScript = ""; Parameters = [] }
            ).SetName("01 - empty list -> empty SqlScript and Parameters")
        }


module UpdateSetSectionTests =

    [<TestCaseSource(typeof<UpdateSetSection>, nameof UpdateSetSection.SetColumnsCases)>]
    let ``build tests`` (setColumns: (string * SetExpr) list, setExprMeta : obj) =
        let setExprMeta' : UpdateSetSection = setExprMeta :?> UpdateSetSection
        match box setExprMeta with
        | null -> raise (AssertionException("setExprMeta is not FieldSetExprMetadata list"))
        | _ ->
            let actual = UpdateSetSection.build setColumns
            Assert.That(actual, Is.EqualTo(setExprMeta'))
