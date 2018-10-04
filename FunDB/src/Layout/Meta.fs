module FunWithFlags.FunDB.Layout.Meta

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.FunQL.Compiler
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.FunQL.AST

exception LayoutMetaError of info : string with
    override this.Message = this.info

let private compileDefaultExpr : PureFieldExpr -> AST.PureValueExpr =
    let voidColumn c = raise (LayoutMetaError <| sprintf "Unexpected column in default expression: %O" c)
    let voidPlaceholder c = raise (LayoutMetaError <| sprintf "Unexpected placeholder in default expression: %O" c)
    genericCompileFieldExpr voidColumn voidPlaceholder

let private compileCheckExpr : LocalFieldExpr -> AST.LocalValueExpr =
    let compileColumn c = AST.SQLName <| c.ToString()
    let voidPlaceholder c = raise (LayoutMetaError <| sprintf "Unexpected placeholder in check expression: %O" c)
    genericCompileFieldExpr compileColumn voidPlaceholder

let private makeUniqueConstraintMeta (constr : ResolvedUniqueConstraint) : AST.ConstraintMeta =
    AST.CMUnique <| Array.map (fun name -> AST.SQLName <| name.ToString()) constr.columns

let private makeCheckConstraintMeta (constr : ResolvedCheckConstraint) : AST.ConstraintMeta =
    AST.CMCheck <| compileCheckExpr constr.expression

let private makeColumnFieldMeta (columnName : AST.ResolvedColumnRef) (field : ResolvedColumnField) : AST.ColumnMeta * (AST.ConstraintName * AST.ConstraintMeta) option =
    let res =
        { columnType = AST.mapValueType (fun (x : AST.SimpleType) -> x.ToSQLName()) (compileFieldType field.fieldType)
          isNullable = field.isNullable
          defaultExpr = Option.map compileDefaultExpr field.defaultExpr
        } : AST.ColumnMeta
    let constr =
        match field.fieldType with
            | FTReference (entityRef, restriction) ->
                // FIXME: support restrictions!
                let tableRef = compileEntityRef entityRef
                Some (AST.SQLName <| sprintf "%O__Foreign__%O" columnName.table.name columnName.name, AST.CMForeignKey (tableRef, [| (columnName.name, AST.SQLName "Id") |]))
            | _ -> None
    (res, constr)

let private makeEntityMeta (tableName : AST.TableRef) (entity : ResolvedEntity) : (AST.SQLName * AST.ObjectMeta) seq =
    let idSeq = AST.SQLName <| sprintf "%O__Seq__Id" tableName.name
    let idConstraints =
        let name = AST.SQLName <| sprintf "%O__Primary__Id" tableName.name
        let constr = AST.CMPrimaryKey [| AST.SQLName "Id" |]
        Seq.singleton (name, constr)
        
    let idColumns =
        let col =
            { columnType = AST.VTScalar (AST.STInt.ToSQLName())
              isNullable = false
              defaultExpr = Some <| AST.VEFunc (AST.SQLName "nextval", [| AST.VEValue (AST.VRegclass { schema = tableName.schema; name = idSeq }) |])
            } : AST.ColumnMeta
        Seq.singleton (AST.SQLName (funId.ToString()), col)

    let makeColumn (name, field) =
        let columnName = AST.SQLName (name.ToString())
        (columnName, makeColumnFieldMeta { table = tableName; name = columnName } field)
    let makeUniqueConstraint (name, constr) = (AST.SQLName <| sprintf "%O__Unique__%O" tableName.name name, makeUniqueConstraintMeta constr)
    let makeCheckConstraint (name, constr) = (AST.SQLName <| sprintf "%O__Check__%O" tableName.name name, makeCheckConstraintMeta constr)

    let columnObjects = entity.columnFields |> Map.toSeq |> Seq.map makeColumn |> Seq.cache
    let userColumns = columnObjects |> Seq.map (fun (name, (column, maybeConstr)) -> (name, column))
    let columnConstraints = columnObjects |> Seq.mapMaybe (fun (name, (column, maybeConstr)) -> maybeConstr)
    let uniqueConstraints = entity.uniqueConstraints |> Map.toSeq |> Seq.map makeUniqueConstraint
    // FIXME: disabled while we cannot parse them from PostgreSQL schema
    //let checkConstraints = entity.checkConstraints |> Map.toSeq |> Seq.map makeCheckConstraint

    let res = { columns = Seq.append idColumns userColumns |> Map.ofSeq } : AST.TableMeta
    //let constraints = Seq.append (Seq.append (Seq.append idConstraints uniqueConstraints) checkConstraints) columnConstraints
    let constraints = Seq.append (Seq.append idConstraints uniqueConstraints) columnConstraints
    let objects = List.toSeq [ (tableName.name, AST.OMTable res); (idSeq, AST.OMSequence) ]
    Seq.append objects (Seq.map (fun (name, constr) -> (name, AST.OMConstraint (tableName.name, constr))) constraints)

let private makeEntitiesMeta (schemaName : AST.SchemaName option) (entities : Map<EntityName, ResolvedEntity>) : AST.SchemaMeta =
    let makeEntity (name, entity) =
        let tableName = { schema = schemaName; name = AST.SQLName <| name.ToString() } : AST.TableRef
        makeEntityMeta tableName entity
    let objects = entities |> Map.toSeq |> Seq.map makeEntity |> Seq.concat |> Map.ofSeq
    { objects = objects }

let private makeSchemaMeta (schemaName : AST.SchemaName) (schema : ResolvedSchema) : AST.SchemaMeta =
    makeEntitiesMeta (Some schemaName) schema.entities

let buildLayoutMeta (layout : Layout) : AST.DatabaseMeta =
    let makeSchema (name, schema) =
        let schemaName = AST.SQLName (name.ToString())
        (Some schemaName, makeSchemaMeta schemaName schema)
        
    let schemas = layout.schemas |> Map.toSeq |> Seq.map makeSchema |> Map.ofSeq
    let systemEntities = makeEntitiesMeta None layout.systemEntities
    { AST.schemas = Map.add None systemEntities schemas }
