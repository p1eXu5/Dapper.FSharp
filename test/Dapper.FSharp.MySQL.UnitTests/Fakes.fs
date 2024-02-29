namespace Dapper.FSharp.MySQL.UnitTests

type Foo =
    {
        id: System.Guid
        index: int
        bar: string
    }
    with
        static member Sample1 =
            {
                id = System.Guid.Parse("11111111-1111-1111-1111-111111111111")
                index = 1
                bar = "qux"
            }
        static member Sample2 =
            {
                id = System.Guid.Parse("22222222-2222-2222-2222-222222222222")
                index = 2
                bar = "waldo"
            }