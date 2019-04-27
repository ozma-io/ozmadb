module FunWithFlags.FunDB.Layout.Meta

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.SQL.Meta
module SQL = FunWithFlags.FunDB.SQL.AST

let rec private compileComputedExpr (entity : ResolvedEntity) : LinkedLocalFieldExpr -> SQL.ValueExpr =
    let makeFullName = function
        | { ref = name; path = [||] } -> compileComputedName entity name
        | c-> failwith <| sprintf "Unexpected dereference in computed field expression: %O" c
    let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in computed field expression: %O" c
    let voidQuery _ = failwith <| sprintf "Unexpected query in computed field expression"
    genericCompileFieldExpr makeFullName voidPlaceholder voidQuery

and compileComputedName (entity : ResolvedEntity) (name : FieldName) =
    let (realName, field) = entity.FindField name |> Option.get
    match field with
    | RId
    | RColumnField _ -> SQL.VEColumn { table = None; name = compileName realName }
    | RComputedField comp -> compileComputedExpr entity comp.expression

let private compileCheckExpr (entity : ResolvedEntity) : LocalFieldExpr -> SQL.ValueExpr =
    let compileColumn = compileComputedName entity
    let voidPlaceholder c = failwith <| sprintf "Unexpected placeholder in check expression: %O" c
    let voidQuery c = failwith <| sprintf "Unexpected subquery in check expression: %O" c
    // Normalization is needed so that expression will be in same form as ones from pg_catalog.
    normalizeLocalExpr << genericCompileFieldExpr compileColumn voidPlaceholder voidQuery

let private makeUniqueConstraintMeta (constr : ResolvedUniqueConstraint) : SQL.ConstraintMeta =
    SQL.CMUnique <| Array.map (fun name -> SQL.SQLName <| name.ToString()) constr.columns

let private makeCheckConstraintMeta (entity : ResolvedEntity) (constr : ResolvedCheckConstraint) : SQL.ConstraintMeta =
    SQL.CMCheck <| compileCheckExpr entity constr.expression

let private makeColumnFieldMeta (columnName : SQL.ResolvedColumnRef) (field : ResolvedColumnField) : SQL.ColumnMeta * (SQL.ConstraintName * SQL.ConstraintMeta) option =
    let res =
        { columnType = SQL.mapValueType (fun (x : SQL.SimpleType) -> x.ToSQLRawString()) (compileFieldType field.fieldType)
          isNullable = field.isNullable
          defaultExpr = Option.map (normalizeLocalExpr << compileFieldValue) field.defaultValue
        } : SQL.ColumnMeta
    let constr =
        match field.fieldType with
            | FTReference (entityRef, restriction) ->
                // FIXME: support restrictions!
                let tableRef = compileResolvedEntityRef entityRef
                Some (SQL.SQLName <| sprintf "%O__Foreign__%O" columnName.table.name columnName.name, SQL.CMForeignKey (tableRef, [| (columnName.name, SQL.SQLName "Id") |]))
            | FTEnum vals ->
                let expr =
                    if Set.isEmpty vals then
                        SQL.VEValue (SQL.VBool false)
                    else
                        let makeCheck value =
                            let col = SQL.VEColumn { table = None; name = columnName.name }
                            SQL.VEEq (col, SQL.VEValue (SQL.VString value))
                        vals |> Set.toSeq |> Seq.map makeCheck |> Seq.fold1 (fun a b -> SQL.VEOr (a, b))
                Some (SQL.SQLName <| sprintf "%O__Enum__%O" columnName.table.name columnName.name, SQL.CMCheck expr)
            | _ -> None
    (res, constr)

let private makeEntityMeta (tableName : SQL.TableRef) (entity : ResolvedEntity) : (SQL.SQLName * SQL.ObjectMeta) seq =
    let idSeq = SQL.SQLName <| sprintf "%O__Seq__Id" tableName.name
    let idConstraints =
        let name = SQL.SQLName <| sprintf "%O__Primary__Id" tableName.name
        let constr = SQL.CMPrimaryKey [| SQL.SQLName "Id" |]
        Seq.singleton (name, constr)

    let idColumns =
        let col =
            { columnType = SQL.VTScalar (SQL.STInt.ToSQLRawString())
              isNullable = false
              defaultExpr = Some <| SQL.VEFunc (SQL.SQLName "nextval", [| SQL.VEValue (SQL.VRegclass { schema = tableName.schema; name = idSeq }) |])
            } : SQL.ColumnMeta
        Seq.singleton (SQL.SQLName (funId.ToString()), col)

    let makeColumn (name, field) =
        let columnName = SQL.SQLName (name.ToString())
        (columnName, makeColumnFieldMeta { table = tableName; name = columnName } field)
    let makeUniqueConstraint (name, constr) = (SQL.SQLName <| sprintf "%O__Unique__%O" tableName.name name, makeUniqueConstraintMeta constr)
    let makeCheckConstraint (name, constr) = (SQL.SQLName <| sprintf "%O__Check__%O" tableName.name name, makeCheckConstraintMeta entity constr)

    let columnObjects = entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
    let userColumns = columnObjects |> Seq.map (fun (name, (column, maybeConstr)) -> (name, column))
    let columnConstraints = columnObjects |> Seq.mapMaybe (fun (name, (column, maybeConstr)) -> maybeConstr)
    let uniqueConstraints = entity.uniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
    let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

    let res = { columns = Seq.append idColumns userColumns |> Map.ofSeq } : SQL.TableMeta
    let constraints = Seq.append (Seq.append (Seq.append idConstraints uniqueConstraints) checkConstraints) columnConstraints
    let objects = List.toSeq [ (tableName.name, SQL.OMTable res); (idSeq, SQL.OMSequence) ]
    Seq.append objects (Seq.map (fun (name, constr) -> (name, SQL.OMConstraint (tableName.name, constr))) constraints)

let private makeEntitiesMeta (schemaName : SQL.SchemaName option) (entities : Map<EntityName, ResolvedEntity>) : SQL.SchemaMeta =
    let makeEntity (name, entity) =
        let tableName = { schema = schemaName; name = SQL.SQLName <| name.ToString() } : SQL.TableRef
        makeEntityMeta tableName entity
    let objects = entities |> Map.toSeq |> Seq.collect makeEntity |> Map.ofSeq
    { objects = objects }

let private makeSchemaMeta (schemaName : SQL.SchemaName) (schema : ResolvedSchema) : SQL.SchemaMeta =
    makeEntitiesMeta (Some schemaName) schema.entities

let buildLayoutMeta (layout : Layout) : SQL.DatabaseMeta =
    let makeSchema (name, schema) =
        let schemaName = SQL.SQLName (name.ToString())
        (schemaName, makeSchemaMeta schemaName schema)

    let schemas = layout.schemas |> Map.toSeq |> Seq.map makeSchema |> Map.ofSeq
    { SQL.schemas = schemas }
