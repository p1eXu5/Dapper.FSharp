namespace Dapper.FSharp.MySQL.UnitTests.Builders

open NUnit.Framework
open Dapper.FSharp.MySQL
open Dapper.FSharp.MySQL.UnitTests


type UpdateExpressionBuilderTestCases() =

    static let fooTable = table<Foo>

    static let defaultUpdateQuery =
        {
            Schema = None
            Table = "Foo"
            Value = None
            SetColumns = []
            Fields = []
            Where = Where.Empty
        } : UpdateQuery<Foo>

    static member UpdateCases =
        seq {
            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index 2
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Value 2)] }
            ).SetName("01: setColumn index 2")

            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index (f.index + 1)
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Binary (SetExpr.Column "index", Add, SetExpr.Value 1))] }
            ).SetName("02: setColumn index (index + 1)")

            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index (f.index - 1)
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Binary (SetExpr.Column "index", Sub, SetExpr.Value 1))] }
            ).SetName("03: setColumn index (index - 1)")

            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index (-f.index)
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Unary (Sub, SetExpr.Column "index"))] }
            ).SetName("04: setColumn index (-index)")

            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index (-f.index - 1)
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Binary ((SetExpr.Unary(Sub, SetExpr.Column "index")), Sub, SetExpr.Value 1))] }
            ).SetName("05: setColumn index (-index - 1)")

            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index f.index
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Column "index")] }
            ).SetName("06: setColumn index index")

            TestCaseData(
                update {
                    for f in fooTable do
                    setColumn f.index 2
                    setColumn f.bar "xyzzy2"
                },
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Value 2); ("bar", SetExpr.Value "xyzzy2")] }
            ).SetName("07: two setColumns with value")

            TestCaseData(
                update {
                    for f in fooTable do
                    set Foo.Sample1
                },
                { defaultUpdateQuery with Value = Foo.Sample1 |> Some }
            ).SetName("08: set <foo_instance> -> sets Value to passed instance ")

            TestCaseData(
                update {
                    for f in fooTable do
                    set Foo.Sample1
                    set Foo.Sample2
                },
                { defaultUpdateQuery with Value = Foo.Sample2 |> Some }
            ).SetName("09: double set <foo_instance> -> sets Value to passed second instance")

            TestCaseData(
                update {
                    for f in fooTable do
                    where (f.index > 2)
                },
                { defaultUpdateQuery with Where = Where.Column ("Foo.index", ColumnComparison.Gt 2) }
            ).SetName("10: for and where clauses -> sets Where")
        }

module UpdateExpressionBuilderTests =

    [<TestCaseSource(typeof<UpdateExpressionBuilderTestCases>, nameof UpdateExpressionBuilderTestCases.UpdateCases)>]
    let ``CE returns expected UpdateQuery`` (updateQueryExpression: UpdateQuery<Foo>, expectedUpdateQuery: UpdateQuery<Foo>) =
        Assert.That(updateQueryExpression, Is.EqualTo(expectedUpdateQuery))
