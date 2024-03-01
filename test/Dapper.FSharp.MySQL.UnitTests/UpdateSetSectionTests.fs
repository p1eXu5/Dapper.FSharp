namespace Dapper.FSharp.MySQL.UnitTests

open System
open NUnit.Framework
open Dapper.FSharp.MySQL

type UpdateSetSectionCases () =

    static member SetColumnsCases =
        seq {
            TestCaseData(
                List.empty<string * SetExpr>,
                { SqlScript = ""; Parameters = [] }
            ).SetName("01: empty list -> empty SqlScript and Parameters")

            TestCaseData(
                [("index", SetExpr.Binary (SetExpr.Column "index", Sub, SetExpr.Value 1))],
                { SqlScript = "`index` = `index` - @Set_index0"; Parameters = ["Set_index0", 1 |> box] }
            ).SetName("02: (index - 1) expresion")

            TestCaseData(
                [("index", SetExpr.Binary ((SetExpr.Unary(Sub, SetExpr.Column "index")), Sub, SetExpr.Value 1))],
                { SqlScript = "`index` = -`index` - @Set_index0"; Parameters = ["Set_index0", 1 |> box] }
            ).SetName("03: (-index - 1) expresion")

            TestCaseData(
                [("index", SetExpr.Unary(Sub, SetExpr.Column "index"))],
                { SqlScript = "`index` = -`index`"; Parameters = [] }
            ).SetName("04: (-index) expresion")
        }


module UpdateSetSectionTests =

    [<TestCaseSource(typeof<UpdateSetSectionCases>, nameof UpdateSetSectionCases.SetColumnsCases)>]
    let ``build tests`` (setColumns: (string * SetExpr) list, setExprMeta : obj) =
        let setExprMeta' : UpdateSetSection = setExprMeta :?> UpdateSetSection
        match box setExprMeta with
        | null -> raise (AssertionException("setExprMeta is not FieldSetExprMetadata list"))
        | _ ->
            let actual = UpdateSetSection.build setColumns
            Assert.That(actual, Is.EqualTo(setExprMeta'))
