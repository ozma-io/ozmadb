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

exception FunMetaError of info: string with
    override this.Message = this.info

let makeColumnMeta (ftype : QualifiedFieldType) (field : ColumnField) : ColumnMeta =
    { colType = compileFieldType ftype;
      nullable = field.Nullable;
      defaultValue =
          if field.Default = null
          then None
          else
              let lexbuf = LexBuffer<char>.FromString field.Default
              let expr = Parser.fieldExpr Lexer.tokenstream lexbuf
              Some(expr |> Qualifier.QualifyDefaultExpr |> compileFieldExpr |> norefValueExpr)
    }

let makeConstraintsMeta (ftype : QualifiedFieldType) (field : ColumnField) : (ConstraintName * ConstraintMeta) seq =
    seq { match ftype with
              | FTReference(WrappedEntity(rent)) ->
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

    let types = entity.ColumnFields |> Seq.map (fun f -> (f.Name, toFieldType f.Type)) |> Map.ofSeq

    try
        let entityColumns = entity.ColumnFields |> Seq.map (fun fld -> (LocalColumn(fld.Name), makeColumnMeta types.[fld.Name] fld))
        let table = { columns = Seq.append [| (LocalColumn("Id"), primaryColumn) |] entityColumns |> mapOfSeqUnique;
                    }
        let entityConstraints = entity.ColumnFields |> Seq.map (fun fld -> makeConstraintsMeta types.[fld.Name] fld) |> Seq.concat |> Seq.map (fun (constrName, constrMeta) -> (constrName, (entity.Name, constrMeta)))
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
                // .ThenInclude(fun (ents : Entity) -> ents.ColumnFields)
                .ToList()
    for schema in q do
        for entity in schema.Entities do
            db.Entry(entity).Collection("Fields").Load()
    
    q |> Seq.map (fun sch -> (sch.Name, makeSchemaMeta qualifier sch)) |> Map.ofSeq
