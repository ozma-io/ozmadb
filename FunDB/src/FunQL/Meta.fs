module internal FunWithFlags.FunDB.FunQL.Meta

open System.Linq
open Microsoft.EntityFrameworkCore
open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.Value
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.FunQL
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB.FunQL.Qualifier.Name
open FunWithFlags.FunDB.FunQL.Compiler

exception FunMetaError of string

let rec qualifyDefaultExpr = function
    | WValue(v) -> WValue(v)
    | WColumn(_) -> raise <| FunMetaError "Column references are not supported in default values"
    | WNot(a) -> WNot(qualifyDefaultExpr a)
    | WConcat(a, b) -> WConcat(qualifyDefaultExpr a, qualifyDefaultExpr b)
    | WEq(a, b) -> WEq(qualifyDefaultExpr a, qualifyDefaultExpr b)
    | WIn(a, b) -> WIn(qualifyDefaultExpr a, qualifyDefaultExpr b)
    | WAnd(a, b) -> WAnd(qualifyDefaultExpr a, qualifyDefaultExpr b)
    | WFunc(name, args) -> WFunc(name, Array.map qualifyDefaultExpr args)
    | WCast(a, typ) -> WCast(qualifyDefaultExpr a, typ)

let makeColumnMeta (ftype : QualifiedFieldType) (field : Field) : ColumnMeta =
    { colType = Compiler.compileValueType ftype;
      nullable = field.Nullable;
      defaultValue =
          if field.Default = ""
          then None
          else
              let lexbuf = LexBuffer<char>.FromString field.Default
              let expr = Parser.valueExpr Lexer.tokenstream lexbuf
              Some(qualifyDefaultExpr expr)
    }

let makeConstraintsMeta (ftype : QualifiedFieldType) (field : Field) : (ConstraintName * ConstraintMeta) seq =
    seq { match ftype with
              | FTReference(QDEEntity(rent)) ->
                  let rname =
                      if rent.SchemaId.HasValue then
                          sprintf "%s.%s" rent.Schema.Name rent.Name
                      else
                          rent.Name
                  let name = sprintf "FK_%s.%s_%s" field.Entity.Name field.Name rname
                  let constr = CMForeignKey(LocalColumn(field.Name), { table = makeEntity rent; name = "Id"; })
                  yield (name, constr)
              | _ -> ()
        }

let makeTableMeta (qualifier : Qualifier) (entity : Entity) : SchemaMeta =
    let primaryName = sprintf "PK_%s" entity.Name
    let primaryConstraint = CMPrimaryKey(set [| LocalColumn("Id") |])
    let primarySequence = sprintf "%s_Id_seq" entity.Name
    let primaryColumn =
        { colType = VTInt;
          nullable = false;
          defaultValue = Some(WFunc("nextval", [| WCast(WValue(VString(renderSqlName primarySequence)), VTObject) |]));
        }

    let toValueType name =
        let lexbuf = LexBuffer<char>.FromString name
        let ftype =
            try
                Parser.fieldType Lexer.tokenstream lexbuf
            with
                | Failure(msg) -> raise <| FunMetaError msg
        try
            qualifier.QualifyType ftype
        with
            | QualifierError(msg) -> raise <| FunMetaError(msg)

    let types = entity.Fields |> Seq.map (fun f -> (f.Name, toValueType f.Type)) |> Map.ofSeq

    try
        let entityColumns = entity.Fields |> Seq.map (fun fld -> (LocalColumn(fld.Name), makeColumnMeta types.[fld.Name] fld))
        let table = { columns = Seq.append [| (LocalColumn("Id"), primaryColumn) |] entityColumns |> mapOfSeqUnique;
                    }
        let entityConstraints = entity.Fields |> Seq.map (fun fld -> makeConstraintsMeta types.[fld.Name] fld) |> Seq.concat |> Seq.map (fun constr -> (entity.Name, constr))
        { tables = Map.ofArray [| (entity.Name, table) |];
          sequences = set [| primarySequence |];
          constraints = Seq.append [| (primaryName, (entity.Name, primaryConstraint)) |] entityConstraints |> mapOfSeqUnique;
        }
    with
        | Failure(msg) -> raise <| FunMetaError msg

let makeSchemaMeta (qualifier : Qualifier) (schema : Schema) : SchemaMeta = schema.Entities |> Seq.map (makeTableMeta qualifier) |> Seq.fold mergeSchemaMeta emptySchemaMeta

let buildFunMeta (db : DatabaseContext) (qualifier : Qualifier) : DatabaseMeta =
    let q = db.Schemas
                .Include(fun sch -> sch.Entities)
                .ThenInclude(fun ents -> ents.Select(fun ent -> ent.Fields))
                .ToList()
    
    q |> Seq.map (fun sch -> (sch.Name, makeSchemaMeta qualifier sch)) |> Map.ofSeq
