module internal FunWithFlags.FunDB.FunQL.Compiler

open FunWithFlags.FunCore
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB.Query

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
    | QEEntity(entity, fields) -> makeEntity entity
    | QESubquery(name, fields) ->
        { AST.schema = None;
          AST.name = name;
        }

let compileField = function
    | QFField(field) ->
        { AST.Column.table = makeEntity field.Entity;
          AST.Column.name = field.Name;
        }
    | QFSubquery(entityName, fieldName) ->
        { AST.Column.table = { schema = None; name = entityName; };
          AST.Column.name = fieldName;
        }

let compileOrder = function
    | Asc -> AST.Asc
    | Desc -> AST.Desc

let compileJoin = function
    | Left -> AST.Left
    | Right -> AST.Right
    | Inner -> AST.Inner
    | Outer -> AST.Full

let rec compileQuery query =
    { AST.columns = List.map (fun (res, attr) -> compileResult res) query.results;
      AST.from = compileFrom query.from;
      AST.where = Option.map compileWhere query.where;
      AST.orderBy = List.map (fun (field, ord) -> (compileField field, compileOrder ord)) query.orderBy;
      // FIXME: support them!
      AST.limit = None;
      AST.offset = None;
    }

and compileFrom = function
    | FEntity(e) -> AST.FTable(compileEntity e)
    | FJoin(jt, e1, e2, where) -> AST.FJoin(compileJoin jt, compileFrom e1, compileFrom e2, compileWhere where)
    | FSubExpr(q, name) -> AST.FSubExpr(compileQuery q, name)

and compileWhere = function
    | WField(f) -> AST.WColumn(compileField f)
    | WInt(i) -> AST.WInt(i)
    // FIXME
    | WFloat(f) -> invalidOp "Not supported"
    | WString(s) -> AST.WString(s)
    | WBool(b) -> AST.WBool(b)
    | WEq(a, b) -> AST.WEq(compileWhere a, compileWhere b)

and compileResult = function
    | RField(f) -> AST.SCColumn(compileField f)
    | RExpr(e, name) -> AST.SCExpr(compileResultExpr e, name)

and compileResultExpr = function
    | REField(f) -> AST.CColumn(compileField f)
