module internal FunWithFlags.FunDB.FunQL.Meta

open System.Linq
open Microsoft.EntityFrameworkCore
open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunCore
open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.SQL.Utils
open FunWithFlags.FunDB.SQL.AST
open FunWithFlags.FunDB.SQL.Qualifier
open FunWithFlags.FunDB.FunQL
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Qualifier
open FunWithFlags.FunDB.FunQL.Qualifier.Name
open FunWithFlags.FunDB.FunQL.Compiler

exception FunMetaError of string

let rec qualifyDefaultExpr = function
    | FEValue(v) -> FEValue(v)
    | FEColumn(c) -> raise <| FunMetaError "Column references are not supported in default values"
    | FENot(a) -> FENot(qualifyDefaultExpr a)
    | FEConcat(a, b) -> FEConcat(qualifyDefaultExpr a, qualifyDefaultExpr b)
    | FEEq(a, b) -> FEEq(qualifyDefaultExpr a, qualifyDefaultExpr b)
    | FEIn(a, arr) -> FEIn(qualifyDefaultExpr a, Array.map qualifyDefaultExpr arr)
    | FEAnd(a, b) -> FEAnd(qualifyDefaultExpr a, qualifyDefaultExpr b)

let makeColumnMeta (ftype : QualifiedFieldType) (field : Field) : ColumnMeta =
    { colType = compileFieldType ftype;
      nullable = field.Nullable;
      defaultValue =
          if field.Default = null
          then None
          else
              let lexbuf = LexBuffer<char>.FromString field.Default
              let expr = Parser.fieldExpr Lexer.tokenstream lexbuf
              Some(expr |> qualifyDefaultExpr |> compileFieldExpr |> norefValueExpr)
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
    let primarySequenceName = sprintf "%s_Id_seq" entity.Name
    let primarySequence = ObjectRef([| entity.Schema.Name; sprintf "%s_Id_seq" entity.Name; |])

    let primaryColumn =
        { colType = VTInt;
          nullable = false;
          defaultValue = Some(VEFunc ("nextval", [| VEValue(VObject(primarySequence)) |]));
        }

    let toFieldType name =
        let lexbuf = LexBuffer<char>.FromString name
        let ftype =
            try
                Parser.fieldType Lexer.tokenstream lexbuf
            with
                | Failure(msg) ->
                    printfn "Ftype: %s" name
                    raise <| FunMetaError msg
        try
            qualifier.QualifyType ftype
        with
            | QualifierError(msg) -> raise <| FunMetaError msg

    let types = entity.Fields |> Seq.map (fun f -> (f.Name, toFieldType f.Type)) |> Map.ofSeq

    try
        let entityColumns = entity.Fields |> Seq.map (fun fld -> (LocalColumn(fld.Name), makeColumnMeta types.[fld.Name] fld))
        let table = { columns = Seq.append [| (LocalColumn("Id"), primaryColumn) |] entityColumns |> mapOfSeqUnique;
                    }
        let entityConstraints = entity.Fields |> Seq.map (fun fld -> makeConstraintsMeta types.[fld.Name] fld) |> Seq.concat |> Seq.map (fun (constrName, constrMeta) -> (constrName, (entity.Name, constrMeta)))
        { tables = Map.ofArray [| (entity.Name, table) |];
          sequences = set [| primarySequenceName |];
          constraints = Seq.append [| (primaryName, (entity.Name, primaryConstraint)) |] entityConstraints |> mapOfSeqUnique;
        }
    with
        | Failure(msg) -> raise <| FunMetaError msg

let makeSchemaMeta (qualifier : Qualifier) (schema : Schema) : SchemaMeta = schema.Entities |> Seq.map (makeTableMeta qualifier) |> Seq.fold mergeSchemaMeta emptySchemaMeta

let buildFunMeta (db : DatabaseContext) (qualifier : Qualifier) : DatabaseMeta =
    let q = db.Schemas
                .Include(fun sch -> sch.Entities)
                // XXX: Workaround https://github.com/aspnet/EntityFrameworkCore/issues/6560
                // .ThenInclude(fun (ents : Entity) -> ents.Fields)
                .ToList()
    for schema in q do
        for entity in schema.Entities do
            db.Entry(entity).Collection("Fields").Load()
    
    q |> Seq.map (fun sch -> (sch.Name, makeSchemaMeta qualifier sch)) |> Map.ofSeq
