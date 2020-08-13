module FunWithFlags.FunDB.Operations.SaveRestore

open System.ComponentModel
open System.Threading
open System.Threading.Tasks
open System.Text.RegularExpressions
open System.IO
open System.IO.Compression
open Newtonsoft.Json
open YamlDotNet.Serialization.NamingConventions
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunUtils
open FunWithFlags.FunUtils.IO
open FunWithFlags.FunUtils.Parsing
open FunWithFlags.FunUtils.Serialization.Yaml
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.Layout.Source
open FunWithFlags.FunDB.Layout.Schema
open FunWithFlags.FunDB.Layout.Update
open FunWithFlags.FunDB.Permissions.Source
open FunWithFlags.FunDB.Permissions.Schema
open FunWithFlags.FunDB.Permissions.Update
open FunWithFlags.FunDB.UserViews.Source
open FunWithFlags.FunDB.UserViews.Schema
open FunWithFlags.FunDB.UserViews.Update
open FunWithFlags.FunDB.Attributes.Source
open FunWithFlags.FunDB.Attributes.Schema
open FunWithFlags.FunDB.Attributes.Update
open FunWithFlags.FunDB.Triggers.Source
open FunWithFlags.FunDB.Triggers.Schema
open FunWithFlags.FunDB.Triggers.Update
open FunWithFlags.FunDBSchema.System

type SaveSchemaErrorInfo =
    | SENotFound
    with
        member this.Message =
            match this with
            | SENotFound -> "Schema not found"

type SaveSchemaException (info : SaveSchemaErrorInfo) =
    inherit Exception(info.Message)

    member this.Info = info

type RestoreSchemaException (message : string, innerException : Exception) =
    inherit Exception(message, innerException)

    new (message : string) = RestoreSchemaException (message, null)

type PrettyColumnField =
    { Type : string
      DefaultValue : string option
      IsNullable : bool
      IsImmutable : bool
      DefaultAttributes : SourceAttributesField option
    }

type PrettyComputedField =
    { Expression : string
      AllowBroken : bool
      IsVirtual : bool
      DefaultAttributes : SourceAttributesField option
    }

type PrettyEntity =
    { ColumnFields : Map<FieldName, PrettyColumnField>
      ComputedFields : Map<FieldName, PrettyComputedField>
      UniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      CheckConstraints : Map<ConstraintName, SourceCheckConstraint>
      MainField : FieldName
      ForbidExternalReferences : bool
      ForbidTriggers : bool
      IsHidden : bool
      IsAbstract : bool
      Parent : ResolvedEntityRef option
      SystemDefaultAttributes : Map<FieldName, SourceAttributesField>
    }

type PrettyTriggerMeta =
    { AllowBroken : bool
      [<DefaultValue(0)>]
      Priority : int
      Time : TriggerTime
      OnInsert : bool
      OnUpdateFields : FieldName[]
      OnDelete : bool
    }

type PrettyUserViewMeta =
    { AllowBroken : bool
    }

type PrettyUserViewsGeneratorScriptMeta =
    { AllowBroken : bool
    }

type SchemaDump =
    { Entities : Map<EntityName, SourceEntity>
      Roles : Map<RoleName, SourceRole>
      UserViews : Map<UserViewName, SourceUserView>
      UserViewsGeneratorScript : SourceUserViewsGeneratorScript option
      DefaultAttributes : Map<SchemaName, SourceAttributesSchema>
      Triggers : Map<SchemaName, SourceTriggersSchema>
    }

let emptySchemaDump : SchemaDump =
    { Entities = Map.empty
      Roles = Map.empty
      UserViews = Map.empty
      UserViewsGeneratorScript = None
      DefaultAttributes = Map.empty
      Triggers = Map.empty
    }

let mergeSchemaDump (a : SchemaDump) (b : SchemaDump) : SchemaDump =
    { Entities = Map.unionUnique a.Entities b.Entities
      Roles = Map.unionUnique a.Roles b.Roles
      UserViews = Map.unionUnique a.UserViews b.UserViews
      UserViewsGeneratorScript = Option.unionUnique a.UserViewsGeneratorScript b.UserViewsGeneratorScript
      DefaultAttributes = Map.unionWith (fun name -> mergeSourceAttributesSchema) a.DefaultAttributes b.DefaultAttributes
      Triggers = Map.unionWith (fun name -> mergeSourceTriggersSchema) a.Triggers b.Triggers
    }

let saveSchema (db : SystemContext) (name : SchemaName) (cancellationToken : CancellationToken) : Task<SchemaDump> =
    task {
        let! entitiesData = buildSchemaLayout db cancellationToken
        let! rolesData = buildSchemaPermissions db cancellationToken
        let! userViewsData = buildSchemaUserViews db cancellationToken
        let! attributesData = buildSchemaAttributes db cancellationToken
        let! triggersData = buildSchemaTriggers db cancellationToken

        let findOrFail m =
            match Map.tryFind name m with
            | None -> raise <| SaveSchemaException SENotFound
            | Some v -> v
        let entities = findOrFail entitiesData.Schemas
        let roles = findOrFail rolesData.Schemas
        let userViews = findOrFail userViewsData.Schemas
        let attributes = findOrFail attributesData.Schemas
        let triggers = findOrFail triggersData.Schemas
        return
            { Entities = entities.Entities
              Roles = roles.Roles
              UserViews = if Option.isSome userViews.GeneratorScript then Map.empty else userViews.UserViews
              UserViewsGeneratorScript = userViews.GeneratorScript
              DefaultAttributes = attributes.Schemas
              Triggers = triggers.Schemas
            }
    }

let restoreSchema (db : SystemContext) (name : SchemaName) (dump : SchemaDump) (cancellationToken : CancellationToken) : Task<bool> =
    task {
        let newLayout = { Schemas = Map.singleton name { Entities = dump.Entities } } : SourceLayout
        let newPerms = { Schemas = Map.singleton name { Roles = dump.Roles } } : SourcePermissions
        let newUserViews =
            let uvs =
                { UserViews = if Option.isSome dump.UserViewsGeneratorScript then Map.empty else dump.UserViews
                  GeneratorScript = dump.UserViewsGeneratorScript
                }
            { Schemas = Map.singleton name uvs } : SourceUserViews
        let newAttributes = { Schemas = Map.singleton name { Schemas = dump.DefaultAttributes } } : SourceDefaultAttributes
        let newTriggers = { Schemas = Map.singleton name { Schemas = dump.Triggers } } : SourceTriggers

        let! updated1 = updateLayout db newLayout cancellationToken
        let! updated2 = updatePermissions db newPerms cancellationToken
        let! updated3 = updateUserViews db newUserViews cancellationToken
        let! updated4 = updateAttributes db newAttributes cancellationToken
        let! updated5 = updateTriggers db newTriggers cancellationToken

        return updated1 || updated2 || updated3 || updated4 || updated5
    }

let private prettifyTriggerMeta (trigger : SourceTrigger) : PrettyTriggerMeta =
    { AllowBroken = trigger.AllowBroken
      Priority = trigger.Priority
      Time = trigger.Time
      OnInsert = trigger.OnInsert
      OnUpdateFields = trigger.OnUpdateFields
      OnDelete = trigger.OnDelete
    }

let private deprettifyTrigger (meta : PrettyTriggerMeta) (procedure : string) : SourceTrigger =
    { AllowBroken = meta.AllowBroken
      Priority = meta.Priority
      Time = meta.Time
      OnInsert = meta.OnInsert
      OnUpdateFields = meta.OnUpdateFields
      OnDelete = meta.OnDelete
      Procedure = procedure
    }

let private prettifyColumnField (defaultAttrs : SourceAttributesField option) (field : SourceColumnField) : PrettyColumnField =
    { Type = field.Type
      DefaultValue = field.DefaultValue
      IsNullable = field.IsNullable
      IsImmutable = field.IsImmutable
      DefaultAttributes = defaultAttrs
    }

let private deprettifyColumnField (field : PrettyColumnField) : SourceAttributesField option * SourceColumnField =
    let ret =
        { Type = field.Type
          DefaultValue = field.DefaultValue
          IsNullable = field.IsNullable
          IsImmutable = field.IsImmutable
        }
    (field.DefaultAttributes, ret)

let private prettifyComputedField (defaultAttrs : SourceAttributesField option) (field : SourceComputedField) : PrettyComputedField =
    { Expression = field.Expression
      AllowBroken = field.AllowBroken
      IsVirtual = field.IsVirtual
      DefaultAttributes = defaultAttrs
    }

let private deprettifyComputedField (field : PrettyComputedField) : SourceAttributesField option * SourceComputedField =
    let ret =
        { Expression = field.Expression
          AllowBroken = field.AllowBroken
          IsVirtual = field.IsVirtual
        }
    (field.DefaultAttributes, ret)

let private prettifyEntity (defaultAttrs : SourceAttributesEntity) (entity : SourceEntity) : PrettyEntity =
    let applyAttrs fn name = fn (defaultAttrs.FindField name)
    { ColumnFields = Map.map (applyAttrs prettifyColumnField) entity.ColumnFields
      ComputedFields = Map.map (applyAttrs prettifyComputedField) entity.ComputedFields
      UniqueConstraints = entity.UniqueConstraints
      CheckConstraints = entity.CheckConstraints
      MainField = entity.MainField
      ForbidExternalReferences = entity.ForbidExternalReferences
      ForbidTriggers = entity.ForbidTriggers
      IsHidden = entity.IsHidden
      IsAbstract = entity.IsAbstract
      Parent = entity.Parent
      SystemDefaultAttributes = defaultAttrs.Fields |> Map.filter (fun name attrs -> Set.contains name systemColumns)
    }

let private deprettifyEntity (entity : PrettyEntity) : SourceAttributesEntity option * SourceEntity =
    if not <| Set.isEmpty (Set.difference (Map.keysSet entity.SystemDefaultAttributes) systemColumns) then
        raisef RestoreSchemaException "`systemDefaultAttributes` can contain only attributes for system columns"
    let mutable defaultAttrs = entity.SystemDefaultAttributes
    let extractAttrs name (maybeFieldAttrs, entry) =
        match maybeFieldAttrs with
        | Some fieldAttrs ->
            defaultAttrs <- Map.add name fieldAttrs defaultAttrs
        | None -> ()
        entry

    let ret =
        { ColumnFields = Map.map (fun name -> deprettifyColumnField >> extractAttrs name) entity.ColumnFields
          ComputedFields = Map.map (fun name -> deprettifyComputedField >> extractAttrs name) entity.ComputedFields
          UniqueConstraints = entity.UniqueConstraints
          CheckConstraints = entity.CheckConstraints
          MainField = entity.MainField
          ForbidExternalReferences = entity.ForbidExternalReferences
          ForbidTriggers = entity.ForbidTriggers
          IsHidden = entity.IsHidden
          IsAbstract = entity.IsAbstract
          Parent = entity.Parent
        }
    let attrsRet = if Map.isEmpty defaultAttrs then None else Some { Fields = defaultAttrs }
    (attrsRet, ret)

let private extraDefaultAttributesEntry = "extra_default_attributes.yaml"

let private userViewsGeneratorMetaEntry = "user_views_generator.yaml"
let private userViewsGeneratorEntry = "user_views_generator.js"

let private maxFilesSize = 1L * 1024L * 1024L // 1MB

let myYamlSerializer = makeYamlSerializer { defaultYamlSerializerSettings with NamingConvention = CamelCaseNamingConvention.Instance }

let myYamlDeserializer = makeYamlDeserializer { defaultYamlDeserializerSettings with NamingConvention = CamelCaseNamingConvention.Instance }

let schemasToZipFile (schemas : Map<SchemaName, SchemaDump>) (stream : Stream) =
    use zip = new ZipArchive(stream, ZipArchiveMode.Create, true)
    let mutable totalSize = 0L

    for KeyValue(schemaName, dump) in schemas do
        let useEntry (path : string) (fn : StreamWriter -> unit) =
            let entry = zip.CreateEntry(sprintf "%O/%s" schemaName path)
            // https://superuser.com/questions/603068/unzipping-file-whilst-getting-correct-permissions
            entry.ExternalAttributes <- 0o644 <<< 16
            use writer = new StreamWriter(entry.Open())
            fn writer
            totalSize <- totalSize + writer.BaseStream.Position

        let dumpToEntry (path : string) (document : 'a) =
            useEntry path <| fun writer ->
                myYamlSerializer.Serialize(writer, document)

        for KeyValue(name, entity) in dump.Entities do
            let defaultAttrs = dump.DefaultAttributes |> Map.tryFind schemaName |> Option.bind (fun schema -> Map.tryFind name schema.Entities) |> Option.defaultValue emptySourceAttributesEntity
            let prettyEntity = prettifyEntity defaultAttrs entity
            dumpToEntry (sprintf "entities/%O.yaml" name) prettyEntity

        for KeyValue(name, role) in dump.Roles do
            dumpToEntry (sprintf "roles/%O.yaml" name) role

        for KeyValue(name, uv) in dump.UserViews do
            useEntry (sprintf "user_views/%O.funql" name) <| fun writer -> writer.Write(uv.Query)
            if uv.AllowBroken then
                let uvMeta = { AllowBroken = true } : PrettyUserViewMeta
                dumpToEntry (sprintf "user_views/%O.yaml" name) uvMeta

        for KeyValue(schemaName, schemaTriggers) in dump.Triggers do
            for KeyValue(entityName, entityTriggers) in schemaTriggers.Entities do
                for KeyValue(triggerName, trigger) in entityTriggers.Triggers do
                    let prettyMeta = prettifyTriggerMeta trigger
                    dumpToEntry (sprintf "triggers/%O/%O/%O.yaml" schemaName entityName triggerName) prettyMeta
                    useEntry (sprintf "triggers/%O/%O/%O.js" schemaName entityName triggerName) <| fun writer ->
                        writer.Write(trigger.Procedure)

        let extraAttributes = dump.DefaultAttributes |> Map.filter (fun name schema -> name <> schemaName)
        if not <| Map.isEmpty extraAttributes then
            dumpToEntry extraDefaultAttributesEntry extraAttributes

        match dump.UserViewsGeneratorScript with
        | None -> ()
        | Some script ->
            useEntry userViewsGeneratorEntry <| fun writer -> writer.Write(script)
            if script.AllowBroken then
                let scriptMeta = { AllowBroken = true } : PrettyUserViewsGeneratorScriptMeta
                dumpToEntry userViewsGeneratorMetaEntry scriptMeta

    if totalSize > maxFilesSize then
        failwithf "Total files size in archive is %i, which is too large" totalSize

let schemasFromZipFile (stream: Stream) : Map<SchemaName, SchemaDump> =
    use zip = new ZipArchive(stream, ZipArchiveMode.Read)
    let mutable leftSize = maxFilesSize

    let readEntry (entry : ZipArchiveEntry) (fn : StreamReader -> 'a) : 'a =
        if leftSize - entry.Length < 0L then
            raisef RestoreSchemaException "Archive entry %s length is %i, which exceeds allowed max length of %i bytes" entry.FullName entry.Length leftSize
        let stream = new MaxLengthStream(entry.Open(), leftSize)
        use reader = new StreamReader(stream)
        let ret =
            try
                fn reader
            with
            | :? IOException as e -> raisefWithInner RestoreSchemaException e "Error during reading archive entry %s" entry.FullName
        leftSize <- leftSize - stream.BytesRead
        ret

    let deserializeEntry (entry : ZipArchiveEntry) : 'a = readEntry entry <| fun reader ->
        try
            downcast myYamlDeserializer.Deserialize(reader, typeof<'a>)
        with
        | :? JsonSerializationException as e -> raisefWithInner RestoreSchemaException e "Error during deserializing archive entry %s" entry.FullName

    let mutable encounteredTriggers : Map<SchemaName * ResolvedFieldRef, PrettyTriggerMeta option * string> = Map.empty
    let mutable encounteredUserViews : Map<SchemaName * UserViewName, PrettyUserViewMeta * string> = Map.empty
    let mutable encounteredUserViewGeneratorScripts : Map<SchemaName, PrettyUserViewsGeneratorScriptMeta * string> = Map.empty

    let parseZipEntry (entry : ZipArchiveEntry) : SchemaName * SchemaDump =
        let (schemaName, rawPath) =
            match entry.FullName with
            | CIRegex @"^([^/]+)/(.*)$" [rawSchemaName; rawPath] ->
                (FunQLName rawSchemaName, rawPath)
            | fileName -> raisef RestoreSchemaException "Invalid archive entry %s" fileName
        let dump =
            if entry.Name = "" && entry.Length = 0L then
                // Directory
                emptySchemaDump
            else
                match rawPath with
                | CIRegex @"^entities/([^/]+)\.yaml$" [rawName] ->
                    let name = FunQLName rawName
                    let prettyEntity : PrettyEntity = deserializeEntry entry
                    let (maybeEntityAttrs, entity) = deprettifyEntity prettyEntity
                    { emptySchemaDump with
                          Entities = Map.singleton name entity
                          DefaultAttributes =
                              match maybeEntityAttrs with
                              | Some attrs -> Map.singleton schemaName { Entities = Map.singleton name attrs }
                              | None -> Map.empty
                    }
                | CIRegex @"^triggers/([^/]+)/([^/]+)/([^/]+)\.yaml$" [rawSchemaName; rawEntityName; rawTriggerName] ->
                    let ref = { entity = { schema = FunQLName rawSchemaName; name = FunQLName rawEntityName }; name = FunQLName rawTriggerName }
                    let prettyTriggerMeta : PrettyTriggerMeta = deserializeEntry entry
                    let (prevMeta, prevProc) = Map.findWithDefault (schemaName, ref) (fun () -> (None, "")) encounteredTriggers
                    assert (Option.isNone prevMeta)
                    encounteredTriggers <- Map.add (schemaName, ref) (Some prettyTriggerMeta, prevProc) encounteredTriggers
                    emptySchemaDump
                | CIRegex @"^triggers/([^/]+)/([^/]+)/([^/]+)\.js$" [rawSchemaName; rawEntityName; rawTriggerName] ->
                    let ref = { entity = { schema = FunQLName rawSchemaName; name = FunQLName rawEntityName }; name = FunQLName rawTriggerName }
                    let rawProcedure = readEntry entry <| fun reader -> reader.ReadToEnd()
                    let (prevMeta, prevProc) = Map.findWithDefault (schemaName, ref) (fun () -> (None, "")) encounteredTriggers
                    assert (prevProc = "")
                    encounteredTriggers <- Map.add (schemaName, ref) (prevMeta, rawProcedure) encounteredTriggers
                    emptySchemaDump
                | CIRegex @"^roles/([^/]+)\.yaml$" [rawName] ->
                    let name = FunQLName rawName
                    let role : SourceRole = deserializeEntry entry
                    { emptySchemaDump with
                          Roles = Map.singleton name role
                    }
                | CIRegex @"^user_views/([^/]+)\.yaml$" [rawName] ->
                    let name = FunQLName rawName
                    let prettyUvMeta : PrettyUserViewMeta = deserializeEntry entry
                    let (prevMeta, prevUv) = Map.findWithDefault (schemaName, name) (fun () -> (({ AllowBroken = false } : PrettyUserViewMeta), "")) encounteredUserViews
                    encounteredUserViews <- Map.add (schemaName, name) (prettyUvMeta, prevUv) encounteredUserViews
                    emptySchemaDump
                | CIRegex @"^user_views/([^/]+)\.funql$" [rawName] ->
                    let name = FunQLName rawName
                    let rawUv = readEntry entry <| fun reader -> reader.ReadToEnd()

                    let (prevMeta, prevUv) = Map.findWithDefault (schemaName, name) (fun () -> (({ AllowBroken = false } : PrettyUserViewMeta), "")) encounteredUserViews
                    assert (prevUv = "")
                    encounteredUserViews <- Map.add (schemaName, name) (prevMeta, rawUv) encounteredUserViews
                    emptySchemaDump
                | fileName when fileName = extraDefaultAttributesEntry ->
                    let defaultAttrs : Map<SchemaName, SourceAttributesSchema> = deserializeEntry entry
                    { emptySchemaDump with
                          DefaultAttributes = defaultAttrs
                    }
                | fileName when fileName = userViewsGeneratorMetaEntry ->
                    let prettyGeneratorMeta : PrettyUserViewsGeneratorScriptMeta = deserializeEntry entry
                    let (prevMeta, prevScript) = Map.findWithDefault schemaName (fun () -> (({ AllowBroken = false } : PrettyUserViewsGeneratorScriptMeta), "")) encounteredUserViewGeneratorScripts
                    encounteredUserViewGeneratorScripts <- Map.add schemaName (prettyGeneratorMeta, prevScript) encounteredUserViewGeneratorScripts
                    emptySchemaDump
                | fileName when fileName = userViewsGeneratorEntry ->
                    let script = readEntry entry <| fun reader -> reader.ReadToEnd()
                    let (prevMeta, prevScript) = Map.findWithDefault schemaName (fun () -> (({ AllowBroken = false } : PrettyUserViewsGeneratorScriptMeta), "")) encounteredUserViewGeneratorScripts
                    encounteredUserViewGeneratorScripts <- Map.add schemaName (prevMeta, script) encounteredUserViewGeneratorScripts
                    emptySchemaDump
                | fileName -> raisef RestoreSchemaException "Invalid archive entry %O/%s" schemaName fileName
        (schemaName, dump)

    let dump =
        zip.Entries
            |> Seq.map (parseZipEntry >> uncurry Map.singleton)
            |> Seq.fold (Map.unionWith (fun name -> mergeSchemaDump)) Map.empty

    let convertTrigger (KeyValue((schemaName, ref), data)) =
        match data with
        | (Some meta, proc) ->
            let entityTriggers =
                { Triggers = Map.singleton ref.name (deprettifyTrigger meta proc) }
            let schemaTriggers =
                { Entities = Map.singleton ref.entity.name entityTriggers }
            let ret =
                { emptySchemaDump with
                      Triggers = Map.singleton ref.entity.schema schemaTriggers
                }
            (schemaName, ret)
        | (None, _) -> raisef RestoreSchemaException "No meta description for trigger %O" ref
    let dump =
        encounteredTriggers
            |> Seq.map (convertTrigger >> uncurry Map.singleton)
            |> Seq.fold (Map.unionWith (fun name -> mergeSchemaDump)) dump
    let convertUserView (KeyValue((schemaName, name), (meta : PrettyUserViewMeta, uv))) =
        let ret = { emptySchemaDump with UserViews = Map.singleton name { Query = uv; AllowBroken = meta.AllowBroken } }
        (schemaName, ret)
    let dump =
        encounteredUserViews
            |> Seq.map (convertUserView >> uncurry Map.singleton)
            |> Seq.fold (Map.unionWith (fun name -> mergeSchemaDump)) dump
    let convertUserViewsGeneratorScript (KeyValue(schemaName, (meta : PrettyUserViewsGeneratorScriptMeta, script))) =
        let ret = { emptySchemaDump with UserViewsGeneratorScript = Some { Script = script; AllowBroken = meta.AllowBroken } }
        (schemaName, ret)
    let dump =
        encounteredUserViewGeneratorScripts
            |> Seq.map (convertUserViewsGeneratorScript >> uncurry Map.singleton)
            |> Seq.fold (Map.unionWith (fun name -> mergeSchemaDump)) dump

    dump