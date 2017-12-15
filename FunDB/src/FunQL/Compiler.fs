module internal FunWithFlags.FunDB.FunQL.Compiler

open System.Linq

open FunWithFlags.FunCore
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier.Name
open FunWithFlags.FunDB.SQL

let makeEntity (fentity : Entity) : AST.Table =
    let schema =
        if fentity.SchemaId.HasValue then
            Some(fentity.Schema.Name)
        else
            None
    { AST.schema = schema;
      AST.name = fentity.Name;
    }

let compileField = function
    | QFField(field) ->
        { AST.table = makeEntity field.Field.Entity;
          AST.Column.name = field.Field.Name;
        }
    | QFEntityId(entity) ->
        { AST.table = makeEntity entity.Entity;
          AST.Column.name = "Id";
        }
    | QFSubquery(entityName, fieldName) ->
        { AST.table = { schema = None; name = entityName; };
          AST.Column.name = fieldName;
        }

let compileFieldType = function
    | FTInt -> AST.VTInt
    | FTString -> AST.VTString
    | FTBool -> AST.VTBool
    | FTDateTime -> AST.VTDateTime
    | FTDate -> AST.VTDate
    | FTReference(_) -> AST.VTInt
    | FTEnum(_) -> AST.VTString

let compileOrder = function
    | Asc -> AST.Asc
    | Desc -> AST.Desc

let compileJoin = function
    | Left -> AST.Left
    | Right -> AST.Right
    | Inner -> AST.Inner
    | Outer -> AST.Full

let compileFieldValue = function
    | FInt(i) -> AST.VInt(i)
    | FString(s) -> AST.VString(s)
    | FBool(b) -> AST.VBool(b)
    | FDateTime(dt) -> AST.VDateTime(dt)
    | FDate(d) -> AST.VDate(d)
    | FNull -> AST.VNull

let rec compileFieldExpr = function
    | FEValue(v) -> AST.VEValue(compileFieldValue v)
    | FEColumn(c) -> AST.VEColumn(compileField c)
    | FENot(a) -> AST.VENot(compileFieldExpr a)
    | FEConcat(a, b) -> AST.VEConcat(compileFieldExpr a, compileFieldExpr b)
    | FEEq(a, b) -> AST.VEEq(compileFieldExpr a, compileFieldExpr b)
    | FEIn(a, arr) -> AST.VEIn(compileFieldExpr a, Array.map compileFieldExpr arr)
    | FEAnd(a, b) -> AST.VEAnd(compileFieldExpr a, compileFieldExpr b)

let rec compileQueryExpr (entities : QEntities) query =
    { AST.columns = Array.map (fun (res, attr) -> compileResult res) query.results;
      AST.from = compileFrom entities query.from;
      AST.where = Option.map compileFieldExpr query.where;
      AST.orderBy = Array.map (fun (expr, ord) -> (compileFieldExpr expr, compileOrder ord)) query.orderBy;
      // FIXME: support them!
      AST.limit = None;
      AST.offset = None;
    }

and compileFrom (entities : QEntities) = function
    | FEntity(QEEntity(rawEntity)) ->
        let entity = entities.[rawEntity.Name]
        let entityName = makeEntity entity.Entity
        if Map.isEmpty entity.computedFields
        then AST.FTable(entityName)
        else
            // Add computed columns
            let columnFields = entity.columnFields |> Map.toSeq |> Seq.map (fun (_, field) -> AST.SCColumn({ AST.table = entityName; AST.Column.name = field.Field.Name; }))
            let computedFields = entity.computedFields |> Map.toSeq |> Seq.map (fun (_, field) -> AST.SCExpr(compileFieldExpr field.expression, field.Field.Name))
            let subquery = { AST.columns = Seq.append columnFields computedFields |> Seq.toArray;
                            AST.from = AST.FTable(entityName);
                            AST.where = None;
                            AST.orderBy = [||];
                            AST.limit = None;
                            AST.offset = None;
                          }
            AST.FSubExpr(subquery, entity.Entity.Name)
    | FEntity(QESubquery(name)) ->
        AST.FTable({ AST.schema = None;
                     AST.name = name;
                   })
    | FJoin(jt, e1, e2, where) -> AST.FJoin(compileJoin jt, compileFrom entities e1, compileFrom entities e2, compileFieldExpr where)
    | FSubExpr(q, name) -> AST.FSubExpr(compileQueryExpr entities q, name)

and compileResult = function
    | RField(f) -> AST.SCColumn(compileField f)
    | RExpr(e, name) -> AST.SCExpr(compileFieldExpr e, name)

and compileQuery (query : QualifiedQuery) = compileQueryExpr query.entities query.expression
