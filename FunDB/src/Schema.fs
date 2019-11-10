module FunWithFlags.FunDB.Schema

open System
open System.Linq
open Microsoft.EntityFrameworkCore
open System.ComponentModel.DataAnnotations

open FunWithFlags.FunDB.Layout.System

type SystemContext (options : DbContextOptions<SystemContext>) =
    inherit DbContext (options)

    // All of this shit is because of how EF Core works.
    [<DefaultValue>]
    val mutable state : DbSet<StateValue>
    [<Entity("Name", ForbidExternalReferences=true, Hidden=true)>]
    [<UniqueConstraint("Name", [|"Name"|])>]
    [<CheckConstraint("NotEmpty", "\"Name\" <> ''")>]
    member this.State
        with get () = this.state
        and set value = this.state <- value

    [<DefaultValue>]
    val mutable schemas : DbSet<Schema>
    [<Entity("Name", ForbidExternalReferences=true)>]
    [<UniqueConstraint("Name", [|"Name"|])>]
    member this.Schemas
        with get () = this.schemas
        and set value = this.schemas <- value

    [<DefaultValue>]
    val mutable entities : DbSet<Entity>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"SchemaId"; "Name"|])>]
    member this.Entities
        with get () = this.entities
        and set value = this.entities <- value

    [<DefaultValue>]
    val mutable columnFields : DbSet<ColumnField>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    member this.ColumnFields
        with get () = this.columnFields
        and set value = this.columnFields <- value

    [<DefaultValue>]
    val mutable computedFields : DbSet<ComputedField>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> '' AND \"Name\" <> 'Id' AND \"Name\" <> 'SubEntity'")>]
    member this.ComputedFields
        with get () = this.computedFields
        and set value = this.computedFields <- value

    [<DefaultValue>]
    val mutable uniqueConstraints : DbSet<UniqueConstraint>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> ''")>]
    [<CheckConstraint("NotEmpty", "\"Columns\" <> ([] :: array(string))")>]
    member this.UniqueConstraints
        with get () = this.uniqueConstraints
        and set value = this.uniqueConstraints <- value

    [<DefaultValue>]
    val mutable checkConstraints : DbSet<CheckConstraint>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"EntityId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> ''")>]
    member this.CheckConstraints
        with get () = this.checkConstraints
        and set value = this.checkConstraints <- value

    [<DefaultValue>]
    val mutable userViews : DbSet<UserView>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"SchemaId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member this.UserViews
        with get () = this.userViews
        and set value = this.userViews <- value

    [<DefaultValue>]
    val mutable users : DbSet<User>
    [<Entity("Name")>]
    [<UniqueConstraint("Name", [|"Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member this.Users
        with get () = this.users
        and set value = this.users <- value

    [<DefaultValue>]
    val mutable roles : DbSet<Role>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"SchemaId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member this.Roles
        with get () = this.roles
        and set value = this.roles <- value

    [<DefaultValue>]
    val mutable roleParents : DbSet<RoleParent>
    [<Entity("Id", ForbidExternalReferences=true)>]
    [<UniqueConstraint("Role", [|"RoleId"; "ParentId"|])>]
    member this.RoleParents
        with get () = this.roleParents
        and set value = this.roleParents <- value

    [<DefaultValue>]
    val mutable roleEntities : DbSet<RoleEntity>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"RoleId\"=>\"__main\" || '.' || \"EntityId\"=>\"__main\"")>]
    [<UniqueConstraint("Entry", [|"RoleId"; "EntityId"|])>]
    member this.RoleEntities
        with get () = this.roleEntities
        and set value = this.roleEntities <- value

    [<DefaultValue>]
    val mutable roleColumnFields : DbSet<RoleColumnField>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"RoleEntityId\"=>\"__main\" || '.' || \"ColumnName\"")>]
    [<UniqueConstraint("Entry", [|"RoleEntityId"; "ColumnName"|])>]
    member this.RoleColumnFields
        with get () = this.roleColumnFields
        and set value = this.roleColumnFields <- value

    [<DefaultValue>]
    val mutable fieldsAttributes : DbSet<FieldAttributes>
    [<Entity("FullName", ForbidExternalReferences=true)>]
    [<ComputedField("FullName", "\"SchemaId\"=>\"__main\" || '.' || \"FieldEntityId\"=>\"__main\" || '.' || \"FieldName\"")>]
    [<UniqueConstraint("Entry", [|"SchemaId"; "FieldEntityId"; "FieldName"|])>]
    member this.FieldsAttributes
        with get () = this.fieldsAttributes
        and set value = this.fieldsAttributes <- value

    [<DefaultValue>]
    val mutable events : DbSet<EventEntry>
    [<Entity("Id")>]
    member this.Events
        with get () = this.events
        and set value = this.events <- value

    override this.OnModelCreating (modelBuilder : ModelBuilder) =
        ignore <| modelBuilder.Entity<RoleParent>()
            .HasOne(fun roleParent -> roleParent.Role)
            .WithMany(fun (role : Role) -> role.Parents :> RoleParent seq)
        ignore <| modelBuilder.Entity<RoleParent>()
            .HasOne(fun roleParent -> roleParent.Parent)
            .WithMany(fun (role : Role) -> role.Children :> RoleParent seq)

and
    [<AllowNullLiteral>]
    StateValue () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Value = "" with get, set

and
    [<AllowNullLiteral>]
    Schema () =
        member val Id = 0 with get, set
        [<ColumnField("string", Immutable=true)>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val ForbidExternalInheritance = false with get, set

        member val Entities = ResizeArray<Entity>() with get, set
        member val Roles = ResizeArray<Role>() with get, set
        member val FieldsAttributes = ResizeArray<FieldAttributes>() with get, set
        member val UserViews = ResizeArray<UserView>() with get, set

and
    [<AllowNullLiteral>]
    Entity () =
        member val Id = 0 with get, set
        [<ColumnField("string", Immutable=true)>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Schemas\")", Immutable=true)>]
        member val SchemaId = 0 with get, set
        member val Schema = null : Schema with get, set
        // FIXME: Make this ColumnField relation when we implement reference constraints.
        [<ColumnField("string", Nullable=true)>]
        member val MainField = null : string with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val ForbidExternalReferences = false with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val Hidden = false with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val IsAbstract = false with get, set
        [<ColumnField("reference(\"public\".\"Entities\")", Nullable=true, Immutable=true)>]
        member val ParentId = Nullable<int>() with get, set
        member val Parent = null : Entity with get, set

        member val ColumnFields = ResizeArray<ColumnField>() with get, set
        member val ComputedFields = ResizeArray<ComputedField>() with get, set
        member val UniqueConstraints = ResizeArray<UniqueConstraint>() with get, set
        member val CheckConstraints = ResizeArray<CheckConstraint>() with get, set

and
    [<AllowNullLiteral>]
    ColumnField () =
        member val Id = 0 with get, set
        [<ColumnField("string", Immutable=true)>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")", Immutable=true)>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Type = "" with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Default = null : string with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val Nullable = false with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val Immutable = false with get, set

and
    [<AllowNullLiteral>]
    ComputedField () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Expression = "" with get, set

and
    [<AllowNullLiteral>]
    UniqueConstraint () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        // Order is important here
        // Change this if/when we implement "ordered 1-N references".
        [<ColumnField("array(string)")>]
        [<Required>]
        member val Columns = [||] : string[] with get, set

and
    [<AllowNullLiteral>]
    CheckConstraint () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Expression = "" with get, set

and
    [<AllowNullLiteral>]
    UserView () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Schemas\")")>]
        member val SchemaId = 0 with get, set
        member val Schema = null : Schema with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val AllowBroken = false with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Query = "" with get, set

and
    [<AllowNullLiteral>]
    User () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val IsRoot = false with get, set
        [<ColumnField("reference(\"public\".\"Roles\")", Nullable=true)>]
        member val RoleId = Nullable<int>() with get, set
        member val Role = null : Role with get, set

and
    [<AllowNullLiteral>]
    Role () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Schemas\")")>]
        member val SchemaId = 0 with get, set
        member val Schema = null : Schema with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Name = "" with get, set

        member val Parents = ResizeArray<RoleParent>() with get, set
        member val Children = ResizeArray<RoleParent>() with get, set
        member val Entities = ResizeArray<RoleEntity>() with get, set

and
    [<AllowNullLiteral>]
    RoleParent () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Roles\")")>]
        member val RoleId = 0 with get, set
        member val Role = null : Role with get, set
        [<ColumnField("reference(\"public\".\"Roles\")")>]
        member val ParentId = 0 with get, set
        member val Parent = null : Role with get, set

and
    [<AllowNullLiteral>]
    RoleEntity () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Roles\")")>]
        member val RoleId = 0 with get, set
        member val Role = null : Role with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val AllowBroken = false with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val Insert = false with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Check = null : string with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Select = null : string with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Update = null : string with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Delete = null : string with get, set

        member val ColumnFields = ResizeArray<RoleColumnField>() with get, set

and
    [<AllowNullLiteral>]
    RoleColumnField () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"RoleEntities\")")>]
        member val RoleEntityId = 0 with get, set
        member val RoleEntity = null : RoleEntity with get, set
        // FIXME: Make this ColumnField relation when we implement reference constraints.
        [<ColumnField("string")>]
        [<Required>]
        member val ColumnName = "" with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val Change = false with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Select = null : string with get, set

and
    [<AllowNullLiteral>]
    FieldAttributes () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Schemas\")")>]
        member val SchemaId = 0 with get, set
        member val Schema = null : Schema with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val FieldEntityId = 0 with get, set
        member val FieldEntity = null : Entity with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val FieldName = "" with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val AllowBroken = false with get, set
        [<ColumnField("int", Default="0")>]
        member val Priority = 0 with get, set
        [<ColumnField("string")>]
        [<Required>]
        member val Attributes = "" with get, set

and
    [<AllowNullLiteral>]
    EventEntry () =
        member val Id = 0 with get, set
        [<ColumnField("datetime", Immutable=true)>]
        member val TransactionTimestamp = DateTimeOffset.MinValue with get, set
        [<ColumnField("datetime", Immutable=true)>]
        member val Timestamp = DateTimeOffset.MinValue with get, set
        [<ColumnField("string", Immutable=true)>]
        [<Required>]
        member val Type = "" with get, set
        [<ColumnField("string", Nullable=true, Immutable=true)>]
        member val UserName = null : string with get, set
        [<ColumnField("string", Nullable=true, Immutable=true)>]
        member val SchemaName = null : string with get, set
        [<ColumnField("string", Nullable=true, Immutable=true)>]
        member val EntityName = null : string with get, set
        [<ColumnField("int", Nullable=true, Immutable=true)>]
        member val EntityId = Nullable<int>() with get, set
        [<ColumnField("string", Immutable=true)>]
        [<Required>]
        member val Details = "" with get, set

let getFieldsObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("Entities")
        .Include("Entities.ColumnFields")

let getLayoutObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    (getFieldsObjects schemas)
        .Include("Entities.ComputedFields")
        .Include("Entities.UniqueConstraints")
        .Include("Entities.CheckConstraints")
        .Include("Entities.Parent")
        .Include("Entities.Parent.Schema")

let getRolesObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("Roles")
        .Include("Roles.Parents")
        .Include("Roles.Parents.Parent")
        .Include("Roles.Parents.Parent.Schema")
        .Include("Roles.Entities")
        .Include("Roles.Entities.Entity")
        .Include("Roles.Entities.Entity.Schema")
        .Include("Roles.Entities.ColumnFields")

let getAttributesObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("FieldsAttributes")
        .Include("FieldsAttributes.FieldEntity")
        .Include("FieldsAttributes.FieldEntity.Schema")

let getUserViewsObjects (schemas : IQueryable<Schema>) : IQueryable<Schema> =
    schemas
        .Include("UserViews")

let updateDifference (db : SystemContext) (updateFunc : 'k -> 'nobj -> 'eobj -> unit) (createFunc : 'k -> 'eobj) (newObjects : Map<'k, 'nobj>) (existingObjects : Map<'k, 'eobj>) =
    for KeyValue (name, newObject) in newObjects do
        match Map.tryFind name existingObjects with
        | Some existingObject -> updateFunc name newObject existingObject
        | None ->
            let newExistingObject = createFunc name
            updateFunc name newObject newExistingObject
    for KeyValue (name, existingObject) in existingObjects do
        if not <| Map.containsKey name newObjects then
            ignore <| db.Remove(existingObject)