module FunWithFlags.FunDB.Permissions.Compile

open FunWithFlags.FunDB.FunQL.Resolve
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.Permissions.Types
open FunWithFlags.FunDB.FunQL.AST
module SQL = FunWithFlags.FunDB.SQL.AST

let private namesToRefs (ref : ResolvedEntityRef) =
    let resolveColumn (name : LinkedFieldName) : LinkedBoundFieldRef =
        let resFieldRef = { entity = ref; name = name.ref } : ResolvedFieldRef
        let fieldRef = { entity = Some <| relaxEntityRef ref; name = name.ref } : FieldRef
        { ref = { ref = fieldRef; bound = Some { ref = resFieldRef; immediate = true } }; path = name.path }
    let voidQuery query =
        failwith <| sprintf "Queries are not allowed in access conditions: %O" query
    mapFieldExpr id resolveColumn id voidQuery

let compileRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.SelectExpr =
    let clause =
        { from = FEntity (None, ref)
          where = Some <| namesToRefs ref restr.expression
        }
    let compiled = compileSingleFromClause layout arguments clause
    let select =
        { columns = [| compileResolvedEntityRef ref |> Some |> SQL.SCAll |]
          clause = Some compiled
          orderLimit = SQL.emptyOrderLimitClause
        } : SQL.SingleSelectExpr
    SQL.SSelect select

let compileValueRestriction (layout : Layout) (ref : ResolvedEntityRef) (arguments : CompiledArgumentsMap) (restr : Restriction) : SQL.ValueExpr =
    let clause =
        { from = FEntity (None, ref)
          where = Some <| namesToRefs ref restr.expression
        }
    let compiled = compileSingleFromClause layout arguments clause
    match compiled.from with
    | SQL.FTable _ -> compiled.where |> Option.get
    | _ ->
        let select =
            { columns = [| SQL.SCColumn { table = Some <| compileResolvedEntityRef ref; name = sqlFunId } |]
              clause = Some compiled
              orderLimit = SQL.emptyOrderLimitClause
            } : SQL.SingleSelectExpr
        let subexpr = SQL.SSelect select
        let idColumn = SQL.VEColumn { table = None; name = sqlFunId }
        SQL.VEInQuery (idColumn, subexpr)