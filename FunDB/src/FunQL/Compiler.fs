module internal FunWithFlags.FunDB.FunQL.Compiler

open FunWithFlags.FunCore
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier.Name

let makeEntity (fentity : Entity) : AST.Table =
    let schema =
        if fentity.Schema = null then
            None
        else
            Some(fentity.Schema.Name)
    { AST.schema = schema;
      AST.name = fentity.Name;
    }

let compileEntity = function
    | QEEntity(entity) -> makeEntity entity
    | QESubquery(name) ->
        { AST.schema = None;
          AST.name = name;
        }

let compileField = function
    | QFField(field) ->
        { AST.Column.table = makeEntity field.Entity;
          AST.Column.name = field.Name;
        }
    | QFEntityId(entity) ->
        { AST.Column.table = makeEntity entity;
          AST.Column.name = "Id";
        }
    | QFSubquery(entityName, fieldName) ->
        { AST.Column.table = { schema = None; name = entityName; };
          AST.Column.name = fieldName;
        }

let compileValueType = function
    | FTInt -> VTInt
    | FTString -> VTString
    | FTBool -> VTBool
    | FTReference(_) -> VTInt

let compileOrder = function
    | Asc -> AST.Asc
    | Desc -> AST.Desc

let compileJoin = function
    | Left -> AST.Left
    | Right -> AST.Right
    | Inner -> AST.Inner
    | Outer -> AST.Full

let rec compileValueExpr = function
    | WValue(v) -> WValue(v)
    | WColumn(c) -> WColumn(compileField c)
    | WEq(a, b) -> WEq(compileValueExpr a, compileValueExpr b)
    | WAnd(a, b) -> WAnd(compileValueExpr a, compileValueExpr b)

let rec compileQuery query =
    { AST.columns = Array.map (fun (res, attr) -> compileResult res) query.results;
      AST.from = compileFrom query.from;
      AST.where = Option.map compileValueExpr query.where;
      AST.orderBy = Array.map (fun (field, ord) -> (compileField field, compileOrder ord)) query.orderBy;
      // FIXME: support them!
      AST.limit = None;
      AST.offset = None;
    }

and compileFrom = function
    | FEntity(e) -> AST.FTable(compileEntity e)
    | FJoin(jt, e1, e2, where) -> AST.FJoin(compileJoin jt, compileFrom e1, compileFrom e2, compileValueExpr where)
    | FSubExpr(q, name) -> AST.FSubExpr(compileQuery q, name)

and compileResult = function
    | RField(f) -> AST.SCColumn(compileField f)
    | RExpr(e, name) -> AST.SCExpr(compileValueExpr e, name)
