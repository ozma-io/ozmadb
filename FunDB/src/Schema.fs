module FunWithFlags.FunDB.Schema

open System
open System.Collections.Generic
open System.ComponentModel.DataAnnotations.Schema
open BCrypt.Net

open FunWithFlags.FunDB.SystemLayout

type DatabaseContext (options : DbContextOptions<DatabaseContext>) =
    inherit DbContext (options)

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\_\\_%' AND \"Name\" <> ''")>]
    member val Schemas = null : DbSet<Schema> with get, set

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"SchemaId\"", "\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\_\\_%' AND \"Name\" <> ''")>]
    [<CheckConstraint("NotRecursive", "\"AncestorId\" <> \"Id\"")>]
    member val Entities = null : DbSet<Entity> with get, set

    [<Entity(Abstract=True)>]
    [<UniqueConstraint("Name", [|"\"EntityId\"", "\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\_\\_%' AND \"Name\" <> '' AND \"Name\" <> 'Id' AND \"Name\" <> 'SubEntity'")>]
    member val Fields = null : DbSet<Field> with get, set
    [<Entity(Ancestor=Some("Fields"))>]
    member val ColumnFields = null : DbSet<ColumnField> with get, set
    [<Entity(Ancestor=Some("Fields"))>]
    member val ComputedFields = null : DbSet<ComputedField> with get, set

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"EntityId\"", "\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\_\\_%' AND \"Name\" <> ''")>]
    [<CheckConstraint("NotEmpty", "\"Expressions\" <> '{}'")>]
    member val UniqueConstraints = null : DbSet<UniqueConstraint> with get, set

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"EntityId\"", "\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" NOT LIKE '%\\_\\_%' AND \"Name\" <> ''")>]
    member val CheckConstraints = null : DbSet<CheckConstraint> with get, set

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member val UserViews = null : DbSet<UserView> with get, set

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member val Users = null : DbSet<User> with get, set

    [<Entity>]
    [<UniqueConstraint("Name", [|"\"Name\""|])>]
    [<CheckConstraint("NotReserved", "\"Name\" <> ''")>]
    member val Roles = null : DbSet<Role> with get, set

    [<Entity>]
    [<CheckConstraint("NotRecursive", "\"ParentId\" <> \"Id\"")>]
    member val RoleParents = null : DbSet<RoleParent> with get, set

    [<Entity>]
    [<UniqueConstraint("Entry", [|"\"RoleId\"", "\"EntityId\""|])>]
    member val RoleEntities = null : DbSet<RoleEntity> with get, set

    [<Entity>]
    [<UniqueConstraint("Entry", [|"\"RoleId\"", "\"FieldId\""|])>]
    member val RoleFields = null : DbSet<RoleField> with get, set

    [<Entity>]
    [<UniqueConstraint("Entry", [|"\"UserId\"", "\"RoleId\""|])>]
    member val UserRoles = null : DbSet<UserRole> with get, set

    override this.OnModelCreating (modelBuilder : ModelBuilder) =
        modelBuilder.Entity<Field>()
            .HasDiscriminator<string>("SubEntity")
            .HasValue<ColumnField>("ColumnField")
            .HasValue<ComputedField>("ComputedField")

and Schema () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set

    member val Entities = null : List<Entity> with get, set

and Entity () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set
    [<ColumnField("reference(\"Schemas\")", Nullable=true)>]
    member val SchemaId = null : Nullable<int> with get, set
    member val Schema = null : Schema with get, set
    [<ColumnField("bool", Default=Some("FALSE"))>]
    member val Abstract = false with get, set
    [<ColumnField("reference(\"Entities\", ref.\"SchemaId\" = this.\"SchemaId\")", Nullable=true)>]
    member val AncestorId = null : Nullable<int> with get, set
    member val Ancestor = null : Entity with get, set
    
    member val Fields = null : List<Field> with get, set
    member val UniqueConstraints = null : List<UniqueConstraint> with get, set
    member val CheckConstraints = null : List<CheckConstraint> with get, set

    member this.ColumnFields =
        seqMapMaybe tryCast<ColumnField> this.Fields

    member this.ComputedFields =
        seqMapMaybe tryCast<ComputedField> this.Fields

and Field () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set
    [<ColumnField("reference(\"Entities\")")>]
    member val EntityId = 0 with get, set
    member val Entity = null : Entity with get, set

and ColumnField () =
    inherit Field ()

    [<ColumnField("string")>]
    member val Type = "" with get, set
    [<ColumnField("string", Nullable=true)>]
    member val Default = null : string with get, set
    [<ColumnField("bool", Default=Some("FALSE"))>]
    member val Nullable = false with get, set

and ComputedField () =
    inherit Field ()

    [<ColumnField("string")>]
    member val Expression = "" with get, set

and UniqueConstraint () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set
    [<ColumnField("reference(\"Entities\")")>]
    member val EntityId = 0 with get, set
    member val Entity = null : Entity with get, set
    [<ColumnField("array(string)")>]
    member val Columns = [||] : string array with get, set

and CheckConstraint () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set
    [<ColumnField("reference(\"Entities\")")>]
    member val EntityId = 0 with get, set
    member val Entity = null : Entity with get, set
    [<ColumnField("string")>]
    member val Expression = "" with get, set

and UserView () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set
    [<ColumnField("string")>]
    member val Query = "" with get, set

and User () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set
    [<ColumnField("string")>]
    member val PasswordHash = "" with get, set
    
    member this.CheckPassword (password) =
        BCrypt.Verify(password, this.PasswordHash)

    member this.Password with set (value) =
        this.PasswordHash <- BCrypt.HashPassword(value)

and Role () =
    member val Id = 0 with get, set
    [<ColumnField("string")>]
    member val Name = "" with get, set

    member val Parents = null : List<Role> with get, set
    member val Entities = null : List<RoleEntity> with get, set

and RoleParent () =
    member val Id = 0 with get, set
    [<ColumnField("reference(\"Roles\")")>]
    member val ParentId = 0 with get, set
    member val Parent = null : Role with get, set

and RoleEntity () =
    member val Id = 0 with get, set
    [<ColumnField("reference(\"Roles\")")>]
    member val RoleId = 0 with get, set
    member val Role = null : Role with get, set
    [<ColumnField("reference(\"Entities\")")>]
    member val EntityId = 0 with get, set
    member val Entity = null : Entity with get, set
    [<ColumnField("bool", Default=Some("TRUE"))>]
    member val ReadOnly = true with get, set
    [<ColumnField("string", Nullable=true)>]
    member val Where = null : string with get, set

and RoleField () =
    member val Id = 0 with get, set
    [<ColumnField("reference(\"Roles\")")>]
    member val RoleId = 0 with get, set
    member val Role = null : Role with get, set
    [<ColumnField("reference(\"Fields\")")>]
    member val FieldId = 0 with get, set
    member val Field = null : Field with get, set

and UserRole () =
    member val Id = 0 with get, set
    [<ColumnField("reference(\"Users\")")>]
    member val UserId = 0 with get, set
    member val User = null : User with get, set
    [<ColumnField("reference(\"Role\")")>]
    member val RoleId = 0 with get, set
    member val Role = null : Role with get, set
