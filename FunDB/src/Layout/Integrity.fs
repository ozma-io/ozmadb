module FunWithFlags.FunDB.Layout.Integrity

open System.Threading.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST
module SQL = FunWithFlags.FunDB.SQL.DDL
module PLPgSQL = FunWithFlags.FunDB.SQL.PLPgSQL

type LayoutIntegrityException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = LayoutIntegrityException (message, null)

// High-level assertions which can be compared.
type LayoutAssertion =
    | LAColumnOfType of ResolvedFieldRef * ResolvedEntityRef

type private AssertionsBuilder (layout : Layout) =
    let columnFieldAssertions (fieldRef : ResolvedFieldRef) (field : ResolvedColumnField) : LayoutAssertion seq =
        seq {
            match field.fieldType with
            | FTReference (toRef, maybeConstr) ->
                assert Option.isNone maybeConstr
                let refEntity = layout.FindEntity toRef |> Option.get
                if Option.isSome refEntity.inheritance then
                    yield LAColumnOfType (fieldRef, toRef)
            | _ -> ()
        }

    let entityAssertions (entityRef : ResolvedEntityRef) (entity : ResolvedEntity) : LayoutAssertion seq =
        seq {
            for KeyValue (colName, col) in entity.columnFields do
                let ref = { entity = entityRef; name = colName }
                yield! columnFieldAssertions ref col
        }

    let schemaAssertions (schemaName : SchemaName) (schema : ResolvedSchema) =
        seq {
            for KeyValue (entityName, entity) in schema.entities do
                let ref = { schema = schemaName; name = entityName }
                yield! entityAssertions ref entity
        }

    let layoutAssertions (subLayout : Layout) =
        seq {
            for KeyValue (schemaName, schema) in subLayout.schemas do
                yield! schemaAssertions schemaName schema
        }

    member this.BuildAssertions subLayout = layoutAssertions subLayout

let buildAssertions (layout : Layout) (subLayout : Layout) : Set<LayoutAssertion> =
    let builder = AssertionsBuilder layout
    builder.BuildAssertions subLayout |> Set.ofSeq

let sqlOldRow : SQL.TableRef = { schema = None; name = SQL.SQLName "old" }
let sqlNewRow : SQL.TableRef = { schema = None; name = SQL.SQLName "new" }

let buildAssertionMeta (layout : Layout) : LayoutAssertion -> SQL.DatabaseMeta = function
    | LAColumnOfType (fromFieldRef, toEntityRef) ->
        let entity = layout.FindEntity fromFieldRef.entity |> Option.get
        let field = Map.find fromFieldRef.name entity.columnFields
        let refEntity = layout.FindEntity toEntityRef |> Option.get
        let inheritance = Option.get refEntity.inheritance

        let toRef = compileResolvedEntityRef refEntity.root
        let fromSchema = compileName entity.root.schema
        let fromTable = compileName entity.root.name
        let fromColumn = SQL.VEColumn { table = Some sqlNewRow; name = field.columnName }
        let toColumn = SQL.VEColumn { table = Some toRef; name = sqlFunId }
        let whereExpr = SQL.VEEq (fromColumn, toColumn)
        let singleSelect =
            { columns = [| SQL.SCExpr (None, inheritance.checkExpr) |]
              from = Some <| SQL.FTable (null, None, toRef)
              where = Some whereExpr
              groupBy = [||]
              orderLimit = SQL.emptyOrderLimitClause
              extra = null
            } : SQL.SingleSelectExpr
        let raiseCall =
            { level = PLPgSQL.RLException
              message = None
              options =
                Map.ofList
                    [ (PLPgSQL.ROErrcode, SQL.VEValue (SQL.VString "integrity_constraint_violation"))
                      (PLPgSQL.ROColumn, SQL.VEValue (SQL.VString <| fromFieldRef.name.ToString()))
                      (PLPgSQL.ROTable, SQL.VEValue (SQL.VString <| fromFieldRef.entity.name.ToString()))
                      (PLPgSQL.ROSchema, SQL.VEValue (SQL.VString <| fromFieldRef.entity.schema.ToString()))
                    ]
            } : PLPgSQL.RaiseStatement
        let selectExpr = { ctes = Map.empty; tree = SQL.SSelect singleSelect } : SQL.SelectExpr
        let checkStmt = PLPgSQL.StIfThenElse ([| (SQL.VENot (SQL.VESubquery selectExpr), [| PLPgSQL.StRaise raiseCall |]) |], None)
        // A small hack; we don't return a "column" but a full row.
        let returnStmt = PLPgSQL.StReturn (SQL.VEColumn { table = None; name = SQL.SQLName "new" })

        let checkProgram =
            { body = [| checkStmt; returnStmt |]
            } : PLPgSQL.Program
        let checkFunctionDefinition =
            { arguments = [||]
              returnValue = SQL.FRValue (SQL.SQLRawString "trigger")
              behaviour = SQL.FBStable
              language = PLPgSQL.plPgSQLName
              definition = checkProgram.ToPLPgSQLString()
            } : SQL.FunctionDefinition
        let checkFunctionObject = SQL.OMFunction <| Map.singleton { items = [||] } checkFunctionDefinition
        let checkFunctionName = SQL.SQLName <| sprintf "__ref_type_check__%s__%s" entity.hashName field.hashName

        let checkOldColumn = SQL.VEColumn { table = Some sqlOldRow; name = field.columnName }
        let checkNewColumn = SQL.VEColumn { table = Some sqlNewRow; name = field.columnName }
        let checkUpdateTriggerCondition =
            if field.isNullable then
                SQL.VEAnd (SQL.VEIsNotNull (checkNewColumn), SQL.VEDistinct (checkOldColumn, checkNewColumn))
            else
                SQL.VENotEq (checkOldColumn, checkNewColumn)
        let checkUpdateTriggerDefinition =
            { isConstraint = true
              order = SQL.TOAfter
              events = [| SQL.TEUpdate (Some [| field.columnName |]) |]
              mode = SQL.TMEachRow
              condition = Some checkUpdateTriggerCondition
              functionName = { schema = Some fromSchema; name = checkFunctionName }
              functionArgs = [||]
            } : SQL.TriggerDefinition
        let checkUpdateTriggerName = SQL.SQLName <| sprintf "__ref_type_update__%s__%s" entity.hashName field.hashName
        let checkUpdateTriggerObject = SQL.OMTrigger (fromTable, checkUpdateTriggerDefinition)

        let checkInsertTriggerDefinition =
            { isConstraint = true
              order = SQL.TOAfter
              events = [| SQL.TEInsert |]
              mode = SQL.TMEachRow
              condition = None
              functionName = { schema = Some fromSchema; name = checkFunctionName }
              functionArgs = [||]
            } : SQL.TriggerDefinition
        let checkInsertTriggerName = SQL.SQLName <| sprintf "__ref_type_insert__%s__%s" entity.hashName field.hashName
        let checkInsertTriggerObject = SQL.OMTrigger (fromTable, checkInsertTriggerDefinition)

        let objects =
            Map.ofList
                [ (checkFunctionName, checkFunctionObject)
                  (checkUpdateTriggerName, checkUpdateTriggerObject)
                  (checkInsertTriggerName, checkInsertTriggerObject)
                ]
        { schemas = Map.singleton fromSchema { objects = objects } }

let buildAssertionsMeta (layout : Layout) : LayoutAssertion seq -> SQL.DatabaseMeta =
    Seq.map (buildAssertionMeta layout) >> Seq.fold SQL.unionDatabaseMeta SQL.emptyDatabaseMeta

let private compileAssertionCheck (layout : Layout) : LayoutAssertion -> SQL.SelectExpr = function
    | LAColumnOfType (fromFieldRef, toEntityRef) ->
        let entity = layout.FindEntity fromFieldRef.entity |> Option.get
        let field = Map.find fromFieldRef.name entity.columnFields
        let refEntity = layout.FindEntity toEntityRef |> Option.get
        let inheritance = Option.get refEntity.inheritance

        let fromRef = compileResolvedEntityRef entity.root
        let toRef = compileResolvedEntityRef refEntity.root
        let fromColumn = SQL.VEColumn { table = Some fromRef; name = field.columnName }
        let toColumn = SQL.VEColumn { table = Some toRef; name = sqlFunId }
        let joinExpr = SQL.VEEq (fromColumn, toColumn)
        let join = SQL.FJoin (SQL.JoinType.Left, SQL.FTable (null, None, fromRef), SQL.FTable (null, Some (SQL.SQLName "__joined"), toRef), joinExpr)
        let subEntityRef = { table = Some toRef; name = sqlFunSubEntity } : SQL.ColumnRef
        let checkExpr = replaceColumnRefs subEntityRef inheritance.checkExpr
        let singleSelect =
            { columns = [| SQL.SCExpr (None, SQL.VEAggFunc (SQL.SQLName "bool_and", SQL.AEAll [| checkExpr |])) |]
              from = Some join
              where = None
              groupBy = [||]
              orderLimit = SQL.emptyOrderLimitClause
              extra = null
            } : SQL.SingleSelectExpr
        { ctes = Map.empty; tree = SQL.SSelect singleSelect }

let checkAssertion (conn : QueryConnection) (layout : Layout) (assertion : LayoutAssertion) : Task<unit> =
    task {
        let query = compileAssertionCheck layout assertion
        try
            match! conn.ExecuteValueQuery (query.ToSQLString()) Map.empty with
            | SQL.VBool true -> ()
            | SQL.VNull -> () // No rows found
            | ret -> raisef LayoutIntegrityException "Failed integrity check, got %O" ret
        with
        | :? QueryException as ex -> raisefWithInner LayoutIntegrityException ex "Failed to run integrity check"
    }