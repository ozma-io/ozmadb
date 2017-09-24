module FunWithFlags.FunDB.FunQL.Compiler

open FunWithFlags.FunCore
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB

let makeEntity (fentity : Entity) : QueryST.Table =
    let schema =
        if fentity.Schema = null then
            None
        else
            Some(fentity.Schema.Name)
    { QueryST.schema = schema;
      QueryST.name = fentity.Name;
    }

let compileEntity = function
    | QEEntity(entity, fields) -> makeEntity entity
    | QESubquery(name, fields) ->
        { QueryST.schema = None;
          QueryST.name = name;
        }

let compileField = function
    | QFField(fentity, field) ->
        { QueryST.Column.table = makeEntity fentity;
          QueryST.Column.name = field.Name;
        }
    | QFSubquery(entityName, fieldName) ->
        { QueryST.Column.table = { schema = None; name = entityName; };
          QueryST.Column.name = fieldName;
        }

let compileOrder = function
    | Asc -> QueryST.Asc
    | Desc -> QueryST.Desc

let compileJoin = function
    | Left -> QueryST.Left
    | Right -> QueryST.Right
    | Inner -> QueryST.Inner
    | Outer -> QueryST.Full

let rec compileQuery query =
    { QueryST.columns = List.map (fun (res, attr) -> compileResult res) query.results;
      QueryST.from = compileFrom query.from;
      QueryST.where = Option.map compileWhere query.where;
      QueryST.orderBy = List.map (fun (field, ord) -> (compileField field, compileOrder ord)) query.orderBy;
      // FIXME: support them!
      QueryST.limit = None;
      QueryST.offset = None;
    }

and compileFrom = function
    | FEntity(e) -> QueryST.FTable(compileEntity e)
    | FJoin(jt, e1, e2, where) -> QueryST.FJoin(compileJoin jt, compileFrom e1, compileFrom e2, compileWhere where)
    | FSubExpr(q, name) -> QueryST.FSubExpr(compileQuery q, name)

and compileWhere = function
    | WField(f) -> QueryST.WColumn(compileField f)
    | WInt(i) -> QueryST.WInt(i)
    // FIXME
    | WFloat(f) -> invalidOp "Not supported"
    | WString(s) -> QueryST.WString(s)
    | WBool(b) -> QueryST.WBool(b)
    | WEq(a, b) -> QueryST.WEq(compileWhere a, compileWhere b)

and compileResult = function
    | RField(f) -> QueryST.SCColumn(compileField f)
    | RExpr(e, name) -> QueryST.SCExpr(compileResultExpr e, name)

and compileResultExpr = function
    | REField(f) -> QueryST.CColumn(compileField f)
