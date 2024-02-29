namespace Dapper.FSharp.MySQL

[<AutoOpen>]
module BuilderCE =

    let select<'T> = SelectExpressionBuilder<'T>()
    let delete<'T> = DeleteExpressionBuilder<'T>()
    let insert<'T> = InsertExpressionBuilder<'T>()
    let update<'T> = UpdateExpressionBuilder<'T>()

    /// WHERE column is IN values
    let isIn<'P> (prop: 'P) (values: 'P list) = true

    /// WHERE column is NOT IN values
    let isNotIn<'P> (prop: 'P) (values: 'P list) = true

    /// WHERE column like value   
    let like<'P> (prop: 'P) (pattern: string) = true

    /// WHERE column not like value   
    let notLike<'P> (prop: 'P) (pattern: string) = true

    /// WHERE column IS NULL
    let isNullValue<'P> (prop: 'P) = true

    /// WHERE column IS NOT NULL
    let isNotNullValue<'P> (prop: 'P) = true
