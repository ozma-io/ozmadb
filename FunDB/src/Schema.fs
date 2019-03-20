module FunWithFlags.FunDB.Schema

open System
open System.Linq
open Microsoft.EntityFrameworkCore

open FunWithFlags.FunDB.Layout.System

type SystemContext (options : DbContextOptions<SystemContext>) =
    inherit DbContext (options)

    // All of this shit is because of how EF Core works.
    [<DefaultValue>]
    val mutable state : DbSet<StateValue>
    [<Entity("Name")>]
    [<UniqueConstraint("Name", [|"Name"|])>]
    [<CheckConstraint("NotEmpty", "\"Name\" <> ''")>]
    member this.State
        with get () = this.state
        and set value = this.state <- value

    [<DefaultValue>]
    val mutable schemas : DbSet<Schema>
    [<Entity("Name")>]
    [<UniqueConstraint("Name", [|"Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%__%' AND \"Name\" <> ''")>]
    member this.Schemas
        with get () = this.schemas
        and set value = this.schemas <- value

    [<DefaultValue>]
    val mutable entities : DbSet<Entity>
    [<Entity("FullName")>]
    [<ComputedField("FullName", "\"SchemaId\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"SchemaId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%__%' AND \"Name\" <> ''")>]
    [<CheckConstraint("CorrectMainField", "\"MainField\" <> '' AND \"MainField\" <> 'Id'")>]
    member this.Entities
        with get () = this.entities
        and set value = this.entities <- value

    [<DefaultValue>]
    val mutable columnFields : DbSet<ColumnField>
    [<Entity("FullName")>]
    [<ComputedField("FullName", "\"EntityId\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> '' AND \"Name\" <> 'Id' AND \"Name\" <> 'SubEntity'")>]
    member this.ColumnFields
        with get () = this.columnFields
        and set value = this.columnFields <- value

    [<DefaultValue>]
    val mutable computedFields : DbSet<ComputedField>
    [<Entity("FullName")>]
    [<ComputedField("FullName", "\"EntityId\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> '' AND \"Name\" <> 'Id' AND \"Name\" <> 'SubEntity'")>]
    member this.ComputedFields
        with get () = this.computedFields
        and set value = this.computedFields <- value

    [<DefaultValue>]
    val mutable uniqueConstraints : DbSet<UniqueConstraint>
    [<Entity("FullName")>]
    [<ComputedField("FullName", "\"EntityId\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> ''")>]
    [<CheckConstraint("NotEmpty", "\"Columns\" <> ([] :: array(string))")>]
    member this.UniqueConstraints
        with get () = this.uniqueConstraints
        and set value = this.uniqueConstraints <- value

    [<DefaultValue>]
    val mutable checkConstraints : DbSet<CheckConstraint>
    [<Entity("FullName")>]
    [<ComputedField("FullName", "\"EntityId\" || '.' || \"Name\"")>]
    [<UniqueConstraint("Name", [|"EntityId"; "Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\\\_\\\\_%' AND \"Name\" <> ''")>]
    member this.CheckConstraints
        with get () = this.checkConstraints
        and set value = this.checkConstraints <- value

    [<DefaultValue>]
    val mutable userViews : DbSet<UserView>
    [<Entity("Name")>]
    [<UniqueConstraint("Name", [|"Name"|])>]
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
    [<Entity("Name")>]
    [<UniqueConstraint("Name", [|"Name"|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member this.Roles
        with get () = this.roles
        and set value = this.roles <- value

    [<DefaultValue>]
    val mutable roleParents : DbSet<RoleParent>
    [<Entity("Id")>]
    [<CheckConstraint("NotRecursive", "\"ParentId\" <> \"Id\"")>]
    member this.RoleParents
        with get () = this.roleParents
        and set value = this.roleParents <- value

    [<DefaultValue>]
    val mutable roleEntities : DbSet<RoleEntity>
    [<Entity("Id")>]
    [<UniqueConstraint("Entry", [|"RoleId"; "EntityId"|])>]
    member this.RoleEntities
        with get () = this.roleEntities
        and set value = this.roleEntities <- value

    [<DefaultValue>]
    val mutable roleColumnFields : DbSet<RoleColumnField>
    [<Entity("Id")>]
    [<UniqueConstraint("Entry", [|"RoleId"; "ColumnFieldId"|])>]
    member this.RoleColumnFields
        with get () = this.roleColumnFields
        and set value = this.roleColumnFields <- value

    [<DefaultValue>]
    val mutable userRoles : DbSet<UserRole>
    [<Entity("Id")>]
    [<UniqueConstraint("Entry", [|"UserId"; "RoleId"|])>]
    member this.UserRoles
        with get () = this.userRoles
        and set value = this.userRoles <- value

    [<DefaultValue>]
    val mutable events : DbSet<EventEntry>
    [<Entity("Id")>]
    member this.Events
        with get () = this.events
        and set value = this.events <- value

and
    [<AllowNullLiteral>]
    StateValue () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("string")>]
        member val Value = "" with get, set

and
    [<AllowNullLiteral>]
    Schema () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set

        member val Entities = ResizeArray<Entity>() with get, set

and
    [<AllowNullLiteral>]
    Entity () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Schemas\")")>]
        member val SchemaId = 0 with get, set
        member val Schema = null : Schema with get, set
        [<ColumnField("string", Nullable=true)>]
        member val MainField = "" with get, set

        member val ColumnFields = ResizeArray<ColumnField>() with get, set
        member val ComputedFields = ResizeArray<ComputedField>() with get, set
        member val UniqueConstraints = ResizeArray<UniqueConstraint>() with get, set
        member val CheckConstraints = ResizeArray<CheckConstraint>() with get, set

and
    [<AllowNullLiteral>]
    ColumnField () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("string")>]
        member val Type = "" with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Default = null : string with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val Nullable = false with get, set

and
    [<AllowNullLiteral>]
    ComputedField () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("string")>]
        member val Expression = "" with get, set

and
    [<AllowNullLiteral>]
    UniqueConstraint () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("array(string)")>]
        member val Columns = [||] : string[] with get, set

and
    [<AllowNullLiteral>]
    CheckConstraint () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("reference(\"public\".\"Entities\")")>]
        member val EntityId = 0 with get, set
        member val Entity = null : Entity with get, set
        [<ColumnField("string")>]
        member val Expression = "" with get, set

and
    [<AllowNullLiteral>]
    UserView () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("string")>]
        member val Query = "" with get, set

and
    [<AllowNullLiteral>]
    User () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set
        [<ColumnField("bool", Default="FALSE")>]
        member val IsRoot = false with get, set

and
    [<AllowNullLiteral>]
    Role () =
        member val Id = 0 with get, set
        [<ColumnField("string")>]
        member val Name = "" with get, set

        member val Parents = ResizeArray<Role>() with get, set
        member val Entities = ResizeArray<RoleEntity>() with get, set

and
    [<AllowNullLiteral>]
    RoleParent () =
        member val Id = 0 with get, set
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
        [<ColumnField("bool", Default="TRUE")>]
        member val ReadOnly = true with get, set
        [<ColumnField("string", Nullable=true)>]
        member val Where = null : string with get, set

and
    [<AllowNullLiteral>]
    RoleColumnField () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Roles\")")>]
        member val RoleId = 0 with get, set
        member val Role = null : Role with get, set
        [<ColumnField("reference(\"public\".\"ColumnFields\")")>]
        member val ColumnFieldId = 0 with get, set
        member val ColumnField = null : ColumnField with get, set

and
    [<AllowNullLiteral>]
    UserRole () =
        member val Id = 0 with get, set
        [<ColumnField("reference(\"public\".\"Users\")")>]
        member val UserId = 0 with get, set
        member val User = null : User with get, set
        [<ColumnField("reference(\"public\".\"Roles\")")>]
        member val RoleId = 0 with get, set
        member val Role = null : Role with get, set

and
    [<AllowNullLiteral>]
    EventEntry () =
        member val Id = 0 with get, set
        [<ColumnField("datetime")>]
        member val TransactionTimestamp = DateTimeOffset.MinValue with get, set
        [<ColumnField("datetime")>]
        member val Timestamp = DateTimeOffset.MinValue with get, set
        [<ColumnField("string")>]
        member val Type = "" with get, set
        [<ColumnField("string", Nullable=true)>]
        member val UserName = null : string with get, set
        [<ColumnField("string")>]
        member val SchemaName = "" with get, set
        [<ColumnField("string")>]
        member val EntityName = "" with get, set
        [<ColumnField("int", Nullable=true)>]
        member val EntityId = Nullable<int>() with get, set
        [<ColumnField("string")>]
        member val Details = "" with get, set

let getLayoutObjects (db : SystemContext) : IQueryable<Schema> =
    db.Schemas
        .Include(fun sch -> sch.Entities)
        .Include("Entities.ColumnFields")
        .Include("Entities.ComputedFields")
        .Include("Entities.UniqueConstraints")
        .Include("Entities.CheckConstraints")