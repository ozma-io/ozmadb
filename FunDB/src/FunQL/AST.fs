namespace FunWithFlags.FunDB.FunQL.AST

open FunWithFlags.FunDB.FunQL

type ColumnName = string
type TableName = string

type SortOrder = Asc | Desc

type ResultExpr<'e, 'f> =
    | REField of 'f

type Result<'e, 'f> =
    | RField of 'f
    | RExpr of ResultExpr<'e, 'f> * ColumnName

type JoinType = Inner | Left | Right | Outer

type QueryExpr<'e, 'f> =
    { results: (Result<'e, 'f> * AttributeMap) list;
      from: FromExpr<'e, 'f>;
      where: WhereExpr<'e, 'f> option;
      orderBy: ('f * SortOrder) list;
    }

and WhereExpr<'e, 'f> =
    | WField of 'f
    | WInt of int
    | WFloat of double
    | WString of string
    | WBool of bool
    | WEq of WhereExpr<'e, 'f> * WhereExpr<'e, 'f>

and FromExpr<'e, 'f> =
    | FEntity of 'e
    | FJoin of JoinType * FromExpr<'e, 'f> * FromExpr<'e, 'f> * WhereExpr<'e, 'f>
    | FSubExpr of QueryExpr<'e, 'f> * TableName
