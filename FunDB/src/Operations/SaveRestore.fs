module FunWithFlags.FunDB.Operations.SaveRestore

open System.Threading.Tasks
open System.Text.RegularExpressions
open System.IO
open System.IO.Compression
open Newtonsoft.Json
open FSharp.Control.Tasks.V2.ContextInsensitive

open FunWithFlags.FunDB.Utils
open FunWithFlags.FunDB.Serialization.Yaml
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
    { fieldType : string
      defaultValue : string option
      [<JsonProperty(Required=Required.DisallowNull)>]
      isNullable : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      isImmutable : bool
      defaultAttributes : SourceAttributesField option
    }

type PrettyComputedField =
    { expression : string
      [<JsonProperty(Required=Required.DisallowNull)>]
      allowBroken : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      isVirtual : bool
      defaultAttributes : SourceAttributesField option
    }

type PrettyEntity =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      columnFields : Map<FieldName, PrettyColumnField>
      [<JsonProperty(Required=Required.DisallowNull)>]
      computedFields : Map<FieldName, PrettyComputedField>
      [<JsonProperty(Required=Required.DisallowNull)>]
      uniqueConstraints : Map<ConstraintName, SourceUniqueConstraint>
      [<JsonProperty(Required=Required.DisallowNull)>]
      checkConstraints : Map<ConstraintName, SourceCheckConstraint>
      mainField : FieldName
      [<JsonProperty(Required=Required.DisallowNull)>]
      forbidExternalReferences : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      isHidden : bool
      [<JsonProperty(Required=Required.DisallowNull)>]
      isAbstract : bool
      parent : ResolvedEntityRef option
      [<JsonProperty(Required=Required.DisallowNull)>]
      systemDefaultAttributes : Map<FieldName, SourceAttributesField>
    }

type SchemaDump =
    { [<JsonProperty(Required=Required.DisallowNull)>]
      entities : Map<EntityName, SourceEntity>
      [<JsonProperty(Required=Required.DisallowNull)>]
      roles : Map<RoleName, SourceRole>
      [<JsonProperty(Required=Required.DisallowNull)>]
      userViews : Map<UserViewName, SourceUserView>
      [<JsonProperty(Required=Required.DisallowNull)>]
      defaultAttributes : Map<SchemaName, SourceAttributesSchema>
    }

let emptySchemaDump : SchemaDump =
    { entities = Map.empty
      roles = Map.empty
      userViews = Map.empty
      defaultAttributes = Map.empty
    }

let mergeSchemaDump (a : SchemaDump) (b : SchemaDump) : SchemaDump =
    { entities = Map.unionUnique a.entities b.entities
      roles = Map.unionUnique a.roles b.roles
      userViews = Map.unionUnique a.userViews b.userViews
      defaultAttributes = Map.unionWith (fun name -> mergeSourceAttributesSchema) a.defaultAttributes b.defaultAttributes
    }

let saveSchema (db : SystemContext) (name : SchemaName) : Task<SchemaDump> =
    task {
        let! entitiesData = buildSchemaLayout db
        let! rolesData = buildSchemaPermissions db
        let! userViewsData = buildSchemaUserViews db
        let! attributesData = buildSchemaAttributes db

        let findOrFail m =
            match Map.tryFind name m with
            | None -> raise <| SaveSchemaException SENotFound
            | Some v -> v
        let entities = findOrFail entitiesData.schemas
        let roles = findOrFail rolesData.schemas
        let userViews = findOrFail userViewsData.schemas
        let attributes = findOrFail attributesData.schemas
        return
            { entities = entities.entities
              roles = roles.roles
              userViews = userViews.userViews
              defaultAttributes = attributes.schemas
            }
    }

let restoreSchema (db : SystemContext) (name : SchemaName) (dump : SchemaDump) : Task<bool> =
    task {
        let newLayout = { schemas = Map.singleton name { entities = dump.entities } } : SourceLayout
        let newPerms = { schemas = Map.singleton name { roles = dump.roles } } : SourcePermissions
        let newUserViews = { schemas = Map.singleton name { userViews = dump.userViews } } : SourceUserViews
        let newAttributes = { schemas = Map.singleton name { schemas = dump.defaultAttributes } } : SourceDefaultAttributes

        let! updated1 = updateLayout db newLayout
        let! updated2 = updatePermissions db newPerms
        let! updated3 = updateUserViews db newUserViews
        let! updated4 = updateAttributes db newAttributes

        return updated1 || updated2 || updated3 || updated4
    }

let private prettifyColumnField (defaultAttrs : SourceAttributesField option) (field : SourceColumnField) : PrettyColumnField =
    { fieldType = field.fieldType
      defaultValue = field.defaultValue
      isNullable = field.isNullable
      isImmutable = field.isImmutable
      defaultAttributes = defaultAttrs
    }

let private prettifyComputedField (defaultAttrs : SourceAttributesField option) (field : SourceComputedField) : PrettyComputedField =
    { expression = field.expression
      allowBroken = field.allowBroken
      isVirtual = field.isVirtual
      defaultAttributes = defaultAttrs
    }

let private prettifyEntity (defaultAttrs : SourceAttributesEntity) (entity : SourceEntity) : PrettyEntity =
    let applyAttrs fn name = fn (defaultAttrs.FindField name)
    { columnFields = Map.map (applyAttrs prettifyColumnField) entity.columnFields
      computedFields = Map.map (applyAttrs prettifyComputedField) entity.computedFields
      uniqueConstraints = entity.uniqueConstraints
      checkConstraints = entity.checkConstraints
      mainField = entity.mainField
      forbidExternalReferences = entity.forbidExternalReferences
      isHidden = entity.isHidden
      isAbstract = entity.isAbstract
      parent = entity.parent
      systemDefaultAttributes = defaultAttrs.fields |> Map.filter (fun name attrs -> Set.contains name systemColumns)
    }

let private deprettifyColumnField (field : PrettyColumnField) : SourceAttributesField option * SourceColumnField =
    let ret =
        { fieldType = field.fieldType
          defaultValue = field.defaultValue
          isNullable = field.isNullable
          isImmutable = field.isImmutable
        }
    (field.defaultAttributes, ret)

let private deprettifyComputedField (field : PrettyComputedField) : SourceAttributesField option * SourceComputedField =
    let ret =
        { expression = field.expression
          allowBroken = field.allowBroken
          isVirtual = field.isVirtual
        }
    (field.defaultAttributes, ret)

let private deprettifyEntity (entity : PrettyEntity) : SourceAttributesEntity option * SourceEntity =
    if not <| Set.isEmpty (Set.difference (Map.keysSet entity.systemDefaultAttributes) systemColumns) then
        raisef RestoreSchemaException "`systemDefaultAttributes` can contain only attributes for system columns"
    let mutable defaultAttrs = entity.systemDefaultAttributes
    let extractAttrs name (maybeFieldAttrs, entry) =
        match maybeFieldAttrs with
        | Some fieldAttrs ->
            defaultAttrs <- Map.add name fieldAttrs defaultAttrs
        | None -> ()
        entry
        
    let ret =
        { columnFields = Map.map (fun name -> deprettifyColumnField >> extractAttrs name) entity.columnFields
          computedFields = Map.map (fun name -> deprettifyComputedField >> extractAttrs name) entity.computedFields
          uniqueConstraints = entity.uniqueConstraints
          checkConstraints = entity.checkConstraints
          mainField = entity.mainField
          forbidExternalReferences = entity.forbidExternalReferences
          isHidden = entity.isHidden
          isAbstract = entity.isAbstract
          parent = entity.parent
        }
    let attrsRet = if Map.isEmpty defaultAttrs then None else Some { fields = defaultAttrs }
    (attrsRet, ret)

let private extraDefaultAttributesEntry = "extra_default_attributes.yaml"

let private maxFilesSize = 1L * 1024L * 1024L // 1MB

let private uvPragmaAllowBroken = "--#allow_broken"

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
                defaultYamlSerializer.Serialize(writer, document)

        for KeyValue(name, entity) in dump.entities do
            let defaultAttrs = dump.defaultAttributes |> Map.tryFind schemaName |> Option.bind (fun schema -> Map.tryFind name schema.entities) |> Option.defaultValue emptySourceAttributesEntity
            let prettyEntity = prettifyEntity defaultAttrs entity
            dumpToEntry (sprintf "entities/%O.yaml" name) prettyEntity

        for KeyValue(name, role) in dump.roles do
            dumpToEntry (sprintf "roles/%O.yaml" name) role

        for KeyValue(name, uv) in dump.userViews do
            useEntry (sprintf "user_views/%O.funql" name) <| fun writer ->
                if uv.allowBroken then
                    writer.WriteLine(uvPragmaAllowBroken)
                writer.Write(uv.query)

        let extraAttributes = dump.defaultAttributes |> Map.filter (fun name schema -> name <> schemaName)
        if not <| Map.isEmpty extraAttributes then
            dumpToEntry extraDefaultAttributesEntry extraAttributes

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
            downcast defaultYamlDeserializer.Deserialize(reader, typeof<'a>)
        with
        | :? JsonSerializationException as e -> raisefWithInner RestoreSchemaException e "Error during deserializing archive entry %s" entry.FullName

    let parseZipEntry (entry : ZipArchiveEntry) : SchemaName * SchemaDump =
        let (schemaName, rawPath) =
            match entry.FullName with
            | CIRegex @"^([^/]+)/(.*)$" [rawSchemaName; rawPath] ->
                (FunQLName rawSchemaName, rawPath)
            | fileName -> raisef RestoreSchemaException "Invalid archive entry %s" fileName
        let dump =
            match rawPath with
            | CIRegex @"^entities/([^/]+)\.yaml$" [rawName] ->
                let name = FunQLName rawName
                let prettyEntity : PrettyEntity = deserializeEntry entry
                let (maybeEntityAttrs, entity) = deprettifyEntity prettyEntity
                { emptySchemaDump with
                      entities = Map.singleton name entity
                      defaultAttributes =
                          match maybeEntityAttrs with
                          | Some attrs -> Map.singleton schemaName { entities = Map.singleton name attrs }
                          | None -> Map.empty
                }
            | CIRegex @"^roles/([^/]+)\.yaml$" [rawName] ->
                let name = FunQLName rawName
                let role : SourceRole = deserializeEntry entry
                { emptySchemaDump with
                      roles = Map.singleton name role
                }
            | CIRegex @"^user_views/([^/]+)\.funql$" [rawName] ->
                let name = FunQLName rawName
                let rawUv = readEntry entry <| fun reader -> reader.ReadToEnd()
                let uv =
                    match regexMatch @"^[ \t\r\n]*--#allow_broken[ \t]*(?:\r|\n|\r\n)(.*)$" (RegexOptions.Singleline ||| RegexOptions.IgnoreCase) rawUv with
                    | Some [query] ->
                        { allowBroken = true
                          query = query
                        }
                    | Some _ -> failwith "Impossible"
                    | None ->
                        { allowBroken = false
                          query = rawUv
                        }
                { emptySchemaDump with
                      userViews = Map.singleton name uv
                }
            | fileName when fileName = extraDefaultAttributesEntry ->
                let defaultAttrs : Map<SchemaName, SourceAttributesSchema> = deserializeEntry entry
                { emptySchemaDump with
                      defaultAttributes = defaultAttrs
                }
            | fileName -> raisef RestoreSchemaException "Invalid archive entry %s" fileName
        (schemaName, dump)

    zip.Entries
        |> Seq.map (parseZipEntry >> uncurry Map.singleton)
        |> Seq.fold (Map.unionWith (fun name -> mergeSchemaDump)) Map.empty
