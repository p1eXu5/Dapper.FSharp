namespace Dapper.FSharp.MySQL.UnitTests

open System
open NUnit.Framework
open Dapper.FSharp.MySQL



type DeconstructorTestCases() =

    static let defaultUpdateQuery =
        {
            Schema = "unit_test" |> Some
            Table = "foo"
            Value = None
            SetColumns = []
            Fields = []
            Where = Where.Empty
        } : UpdateQuery<Foo>

    static member UpdateCases =
        seq {
            TestCaseData(
                defaultUpdateQuery,
                ("UPDATE `unit_test`.`foo` SET ", Map.empty<string, obj>)
            ).SetName("01 - default UpdateQuery")

            TestCaseData(
                { defaultUpdateQuery with Value = Foo.Sample1 |> Some },
                (
                    "UPDATE `unit_test`.`foo` SET `id`=@id, `index`=@index, `bar`=@bar"
                    , [
                        nameof Foo.Sample1.id, Foo.Sample1.id |> box
                        nameof Foo.Sample1.index, Foo.Sample1.index |> box
                        nameof Foo.Sample1.bar, Foo.Sample1.bar |> box
                    ] |> Map.ofList)
            ).SetName("01 - SetColumns with value")

            TestCaseData(
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Value 2)] },
                (
                    "UPDATE `unit_test`.`foo` SET `index` = @Set_index0"
                    , ["Set_index0", 2 |> box] |> Map.ofList
                )
            ).SetName("02 - SetColumns with value")

            TestCaseData(
                { defaultUpdateQuery with SetColumns = [("index", SetExpr.Column "index")] },
                (
                    "UPDATE `unit_test`.`foo` SET `index` = `index`"
                    , Map.empty<string, obj>
                )
            ).SetName("03 - SetColumns with column")
        }


module DeconstructorTests =

    // ------------------ update

    [<TestCaseSource(typeof<DeconstructorTestCases>, nameof DeconstructorTestCases.UpdateCases)>]
    let ``Deconstructor update tests`` (q: UpdateQuery<Foo>, expectedSql: string * Map<string, obj>) =
        let sql = Deconstructor.update q
        Assert.That(sql, Is.EqualTo(expectedSql))
