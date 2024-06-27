from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Literal
import isodate
from pydantic import BaseModel, Field, PlainSerializer, model_validator
from pydantic.alias_generators import to_camel

from .render import ozmaql_name


DomainId = int
RowId = int
ArgumentName = str
InstanceName = str
InstanceName = str
SchemaName = str
EntityName = str
FieldName = str
ColumnName = str
IndexName = str
RoleName = str
UserViewName = str
ModuleName = str
ActionName = str
TriggerName = str
CustomEntityName = str
AttributeName = str


OzmaQLJson = dict[str, Any]
ScalarOzmaQLValue = str | int | float | bool | date | datetime | timedelta | OzmaQLJson | None
OzmaQLValue = ScalarOzmaQLValue | list[ScalarOzmaQLValue]
ScalarRawOzmaQLValue = str | int | float | bool | OzmaQLJson | None
RawOzmaQLValue = ScalarRawOzmaQLValue | list[ScalarRawOzmaQLValue]


def _ozmaql_scalar_value_to_raw(v: ScalarOzmaQLValue | ScalarRawOzmaQLValue) -> ScalarRawOzmaQLValue:
    match v:
        case date():
            return v.isoformat()
        case datetime():
            return v.isoformat()
        case timedelta():
            return isodate.duration_isoformat(v)
        case _:
            return v


def ozmaql_value_to_raw(v: OzmaQLValue | RawOzmaQLValue) -> RawOzmaQLValue:
    if isinstance(v, list):
        return [_ozmaql_scalar_value_to_raw(x) for x in v]
    else:
        return _ozmaql_scalar_value_to_raw(v)


def _ozmaql_scalar_value_from_raw(
    t: "OzmaDBScalarValueType", v: ScalarOzmaQLValue | ScalarRawOzmaQLValue
) -> ScalarOzmaQLValue:
    if v is None:
        return None
    match t:
        case OzmaDBDateValueType():
            match v:
                case date():
                    return v
                case str():
                    return date.fromisoformat(v)
                case _:
                    raise ValueError(f"Expected date string, got {v}")
        case OzmaDBDateTimeValueType() | OzmaDBLocalDateTimeValueType():
            match v:
                case datetime():
                    return v
                case str():
                    return datetime.fromisoformat(v)
                case _:
                    raise ValueError(f"Expected datetime string, got {v}")
        case OzmaDBIntervalValueType():
            match v:
                case timedelta():
                    return v
                case str():
                    return isodate.parse_duration(v)
                case _:
                    raise ValueError(f"Expected interval string, got {v}")
        case _:
            return v


def ozmaql_value_from_raw(t: "OzmaDBValueType", v: OzmaQLValue | RawOzmaQLValue) -> OzmaQLValue:
    if isinstance(t, OzmaDBArrayValueType):
        if not isinstance(v, list):
            raise ValueError(f"Expected list, got {v}")
        return [_ozmaql_scalar_value_from_raw(t.subtype, x) for x in v]
    else:
        if isinstance(v, list):
            raise ValueError(f"Expected scalar value, got {v}")
        return _ozmaql_scalar_value_from_raw(t, v)


OzmaQLValueArg = Annotated[
    OzmaQLValue | RawOzmaQLValue, PlainSerializer(ozmaql_value_to_raw, return_type=RawOzmaQLValue)
]


class OzmaDBModel(BaseModel, alias_generator=to_camel, populate_by_name=True, extra="ignore"):
    pass


class OzmaDBAbstractRef(BaseModel, alias_generator=to_camel, populate_by_name=True, frozen=True):
    # `schema` conflicts with `schema` in `BaseModel`.
    schema_name: SchemaName = Field(alias="schema")
    name: str

    # Allow positional args.
    def __init__(
        self, schema_name: SchemaName | None = None, name: str | None = None, schema: SchemaName | None = None
    ):
        if schema_name and schema:
            raise ValueError("Use schema_name or schema")
        super().__init__(schema_name=schema_name or schema, name=name)

    def __str__(self) -> str:
        return f"{ozmaql_name(self.schema_name)}.{ozmaql_name(self.name)}"


class OzmaDBEntityRef(OzmaDBAbstractRef, frozen=True):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class OzmaDBUserViewRef(OzmaDBAbstractRef, frozen=True):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self) -> str:
        return f"&{ozmaql_name(self.schema_name)}.{ozmaql_name(self.name)}"


class OzmaDBFieldRef(BaseModel, alias_generator=to_camel, populate_by_name=True, frozen=True):
    entity: OzmaDBEntityRef
    name: str

    # Allow positional args.
    def __init__(self, entity: OzmaDBEntityRef, name: str):
        super().__init__(entity=entity, name=name)

    def __str__(self) -> str:
        return f"{self.entity}.{ozmaql_name(self.name)}"


class OzmaDBColumnField(OzmaDBModel):
    type: str = Field(description="The type of the column.")
    default_value: str | None = Field(default=None, description="Constant OzmaQL expression for the default value.")
    is_nullable: bool = False
    is_immutable: bool = False
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class OzmaDBComputedField(OzmaDBModel):
    expression: str = Field(description="OzmaQL expression.")
    allow_broken: bool = False
    is_virtual: bool = False
    is_materialized: bool = False
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class OzmaDBUniqueConstraint(OzmaDBModel):
    columns: list[str]
    is_alternate_key: bool = False
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class OzmaDBCheckConstraint(OzmaDBModel):
    expression: str


class IndexType(StrEnum):
    BTREE = "btree"
    GIST = "gist"
    GIN = "gin"


class OzmaDBIndex(OzmaDBModel):
    expressions: list[str] = Field(description="OzmaQL expressions list to index.")
    included_expressions: list[str] = Field(default_factory=list)
    is_unique: bool = False
    type: IndexType = IndexType.BTREE
    predicate: str | None = None
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class OzmaDBEntity(OzmaDBModel):
    column_fields: dict[FieldName, OzmaDBColumnField] = Field(default_factory=dict)
    computed_fields: dict[FieldName, OzmaDBComputedField] = Field(default_factory=dict)
    unique_constraints: dict[FieldName, OzmaDBUniqueConstraint] = Field(default_factory=dict)
    check_constraints: dict[FieldName, OzmaDBCheckConstraint] = Field(default_factory=dict)
    indexes: dict[IndexName, OzmaDBIndex] = Field(default_factory=dict)
    main_field: str | None = None
    save_restore_key: str | None = None
    is_abstract: bool = False
    is_frozen: bool = False
    parent: OzmaDBEntityRef | None = None
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class OzmaDBAttributesField(OzmaDBModel):
    allow_broken: bool = False
    priority: int = 0
    attributes: str


class OzmaDBAttributesEntity(OzmaDBModel):
    fields: dict[FieldName, OzmaDBAttributesField] = Field(default_factory=dict)


class OzmaDBAttributesSchema(OzmaDBModel):
    entities: dict[EntityName, OzmaDBAttributesEntity] = Field(default_factory=dict)


class OzmaDBSavedSchemaCustomEntities(OzmaDBModel):
    type: Literal["customEntities"] = "customEntities"
    custom_entities: dict[SchemaName, dict[EntityName, list[Any]]] = Field(default_factory=dict)


class OzmaDBUserView(OzmaDBModel):
    query: str
    allow_broken: bool = False


class OzmaDBFullSavedSchema(OzmaDBModel):
    type: Literal["full"] = "full"
    custom_entities: dict[SchemaName, dict[EntityName, list[Any]]] = Field(default_factory=dict)
    entities: dict[EntityName, OzmaDBEntity] = Field(default_factory=dict)
    # TODO: Type others.
    roles: dict[RoleName, Any] = Field(default_factory=dict)
    user_views: dict[UserViewName, OzmaDBUserView] = Field(default_factory=dict)
    default_attributes: dict[SchemaName, OzmaDBAttributesSchema] = Field(default_factory=dict)
    modules: dict[ModuleName, Any] = Field(default_factory=dict)
    actions: dict[ActionName, Any] = Field(default_factory=dict)
    triggers: dict[TriggerName, Any] = Field(default_factory=dict)
    user_views_geneerator_script: str | None = None
    description: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


OzmaDBSavedSchema = Annotated[OzmaDBFullSavedSchema | OzmaDBSavedSchemaCustomEntities, Field(discriminator="type")]


class OzmaDBSavedSchemas(OzmaDBModel):
    schemas: dict[SchemaName, OzmaDBSavedSchema] = Field(default_factory=dict)


class OzmaDBActionResponse(OzmaDBModel):
    result: RawOzmaQLValue = None


class OzmaDBInsertEntityOp(OzmaDBModel):
    type: Literal["insert"] = "insert"
    entity: OzmaDBEntityRef
    fields: dict[FieldName, OzmaQLValueArg]


class OzmaDBUpdateEntityOp(OzmaDBModel):
    type: Literal["update"] = "update"
    entity: OzmaDBEntityRef
    id: RowId
    fields: dict[FieldName, OzmaQLValueArg]


class OzmaDBDeleteEntityOp(OzmaDBModel):
    type: Literal["delete"] = "delete"
    entity: OzmaDBEntityRef
    id: RowId


OzmaDBTransactionOp = Annotated[
    OzmaDBInsertEntityOp | OzmaDBUpdateEntityOp | OzmaDBDeleteEntityOp, Field(discriminator="type")
]


class OzmaDBInsertEntityResult(OzmaDBModel):
    type: Literal["insert"] = "insert"
    id: RowId | None


class OzmaDBUpdateEntityResult(OzmaDBModel):
    type: Literal["update"] = "update"
    id: RowId


class OzmaDBDeleteEntityResult(OzmaDBModel):
    type: Literal["delete"] = "delete"


OzmaDBTransactionResultOp = Annotated[
    OzmaDBInsertEntityResult | OzmaDBUpdateEntityResult | OzmaDBDeleteEntityResult, Field(discriminator="type")
]


class OzmaDBTransactionResult(OzmaDBModel):
    results: list[OzmaDBTransactionResultOp]


class OzmaDBIntValueType(OzmaDBModel):
    type: Literal["int"] = "int"


class OzmaDBDecimalValueType(OzmaDBModel):
    type: Literal["decimal"] = "decimal"


class OzmaDBStringValueType(OzmaDBModel):
    type: Literal["string"] = "string"


class OzmaDBBoolValueType(OzmaDBModel):
    type: Literal["bool"] = "bool"


class OzmaDBDateTimeValueType(OzmaDBModel):
    type: Literal["datetime"] = "datetime"


class OzmaDBLocalDateTimeValueType(OzmaDBModel):
    type: Literal["localdatetime"] = "localdatetime"


class OzmaDBDateValueType(OzmaDBModel):
    type: Literal["date"] = "date"


class OzmaDBIntervalValueType(OzmaDBModel):
    type: Literal["interval"] = "interval"


class OzmaDBJsonValueType(OzmaDBModel):
    type: Literal["json"] = "json"


class OzmaDBUuidValueType(OzmaDBModel):
    type: Literal["uuid"] = "uuid"


OzmaDBScalarValueType = Annotated[
    OzmaDBIntValueType
    | OzmaDBDecimalValueType
    | OzmaDBStringValueType
    | OzmaDBBoolValueType
    | OzmaDBDateTimeValueType
    | OzmaDBLocalDateTimeValueType
    | OzmaDBDateValueType
    | OzmaDBIntervalValueType
    | OzmaDBJsonValueType
    | OzmaDBUuidValueType,
    Field(discriminator="type"),
]


class OzmaDBArrayValueType(OzmaDBModel):
    type: Literal["array"] = "array"
    subtype: OzmaDBScalarValueType


# OzmaDBValueType conversion
OzmaDBValueType = Annotated[OzmaDBScalarValueType | OzmaDBArrayValueType, Field(discriminator="type")]


class OzmaDBIntFieldType(OzmaDBModel):
    type: Literal["int"] = "int"


class OzmaDBDecimalFieldType(OzmaDBModel):
    type: Literal["decimal"] = "decimal"


class OzmaDBStringFieldType(OzmaDBModel):
    type: Literal["string"] = "string"


class OzmaDBBoolFieldType(OzmaDBModel):
    type: Literal["bool"] = "bool"


class OzmaDBDateTimeFieldType(OzmaDBModel):
    type: Literal["datetime"] = "datetime"


class OzmaDBDateFieldType(OzmaDBModel):
    type: Literal["date"] = "date"


class OzmaDBIntervalFieldType(OzmaDBModel):
    type: Literal["interval"] = "interval"


class OzmaDBJsonFieldType(OzmaDBModel):
    type: Literal["json"] = "json"


class OzmaDBUuidFieldType(OzmaDBModel):
    type: Literal["uuid"] = "uuid"


class OzmaDBReferenceFieldType(OzmaDBModel):
    type: Literal["reference"] = "reference"
    entity: OzmaDBEntityRef


class OzmaDBEnumFieldType(OzmaDBModel):
    type: Literal["enum"] = "enum"
    values: list[str]


OzmaDBScalarFieldType = Annotated[
    OzmaDBIntFieldType
    | OzmaDBDecimalFieldType
    | OzmaDBStringFieldType
    | OzmaDBBoolFieldType
    | OzmaDBDateTimeFieldType
    | OzmaDBDateFieldType
    | OzmaDBIntervalFieldType
    | OzmaDBJsonFieldType
    | OzmaDBUuidFieldType
    | OzmaDBReferenceFieldType
    | OzmaDBEnumFieldType,
    Field(discriminator="type"),
]


# ArrayFieldType
class OzmaDBArrayFieldType(OzmaDBModel):
    type: Literal["array"] = "array"
    subtype: OzmaDBScalarFieldType


# Union for FieldType
OzmaDBFieldType = Annotated[OzmaDBScalarFieldType | OzmaDBArrayFieldType, Field(discriminator="type")]


def ozmadb_scalar_field_type_to_type(type: OzmaDBScalarFieldType) -> OzmaDBScalarValueType:
    # Ugly!
    match type:
        case OzmaDBIntFieldType():
            return OzmaDBIntValueType()
        case OzmaDBDecimalFieldType():
            return OzmaDBDecimalValueType()
        case OzmaDBStringFieldType():
            return OzmaDBStringValueType()
        case OzmaDBBoolFieldType():
            return OzmaDBBoolValueType()
        case OzmaDBDateTimeFieldType():
            return OzmaDBDateTimeValueType()
        case OzmaDBDateFieldType():
            return OzmaDBDateValueType()
        case OzmaDBIntervalFieldType():
            return OzmaDBIntervalValueType()
        case OzmaDBJsonFieldType():
            return OzmaDBJsonValueType()
        case OzmaDBUuidFieldType():
            return OzmaDBUuidValueType()
        case OzmaDBReferenceFieldType():
            return OzmaDBIntValueType()
        case OzmaDBEnumFieldType():
            return OzmaDBStringValueType()
        case _:
            raise ValueError(f"Invalid scalar field type {type}")


def ozmadb_field_type_to_value(type: OzmaDBFieldType) -> OzmaDBValueType:
    if isinstance(type, OzmaDBArrayFieldType):
        return OzmaDBArrayValueType(subtype=ozmadb_scalar_field_type_to_type(type.subtype))
    else:
        return ozmadb_scalar_field_type_to_type(type)


class OzmaDBFieldAccess(OzmaDBModel):
    select: bool
    update: bool
    insert: bool


class OzmaDBViewColumnField(OzmaDBModel):
    field_type: OzmaDBFieldType
    value_type: OzmaDBValueType
    default_value: OzmaQLValue | None = None
    is_nullable: bool
    is_immutable: bool
    inherited_from: OzmaDBEntityRef | None = None
    access: OzmaDBFieldAccess
    hasUpdateTriggers: bool

    @model_validator(mode="after")
    def _validate(self: "OzmaDBViewColumnField") -> "OzmaDBViewColumnField":
        if self.default_value is not None:
            self.default_value = ozmaql_value_from_raw(self.value_type, self.default_value)
        return self


class OzmaDBMainFieldInfo(OzmaDBModel):
    name: FieldName
    field: OzmaDBViewColumnField


class OzmaDBBoundMappingEntry(OzmaDBModel):
    when: OzmaQLValue
    value: OzmaQLValue

    def _convert_values(self, type: OzmaDBValueType):
        self.when = ozmaql_value_from_raw(type, self.when)
        self.value = ozmaql_value_from_raw(type, self.value)


class OzmaDBBoundMapping(OzmaDBModel):
    entries: list[OzmaDBBoundMappingEntry]
    default: OzmaQLValue | None = None

    def _convert_values(self, type: OzmaDBValueType):
        for entry in self.entries:
            entry._convert_values(type)
        if self.default is not None:
            self.default = ozmaql_value_from_raw(type, self.default)


class OzmaDBBasicAttributeInfo(OzmaDBModel):
    type: OzmaDBValueType


class OzmaDBMappedAttributeInfo(OzmaDBBasicAttributeInfo):
    mapping: OzmaDBBoundMapping | None = None

    @model_validator(mode="after")
    def _validate(self: "OzmaDBMappedAttributeInfo") -> "OzmaDBMappedAttributeInfo":
        if self.mapping is not None:
            self.mapping._convert_values(self.type)
        return self


class OzmaDBBoundAttributeInfo(OzmaDBMappedAttributeInfo):
    const: bool = False


class OzmaDBCellAttributeInfo(OzmaDBMappedAttributeInfo):
    pass


class OzmaDBViewAttributeInfo(OzmaDBBasicAttributeInfo):
    const: bool = False


class OzmaDBRowAttributeInfo(OzmaDBBasicAttributeInfo):
    pass


class OzmaDBResultColumnInfo(OzmaDBModel):
    name: str
    attribute_types: dict[str, OzmaDBBoundAttributeInfo] = Field(default_factory=dict)
    cell_attribute_types: dict[str, OzmaDBCellAttributeInfo] = Field(default_factory=dict)
    value_type: OzmaDBValueType
    pun_type: OzmaDBValueType | None = None
    main_field: OzmaDBMainFieldInfo | None = None


class OzmaDBDomainField(OzmaDBModel):
    ref: OzmaDBFieldRef
    field: OzmaDBViewColumnField | None = None
    id_column: int


class OzmaDBArgument(OzmaDBModel):
    name: str
    arg_type: OzmaDBFieldType
    optional: bool
    default_value: OzmaQLValue | None = None
    attribute_types: dict[str, OzmaDBBoundAttributeInfo] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate(self: "OzmaDBArgument") -> "OzmaDBArgument":
        arg_value_type = ozmadb_field_type_to_value(self.arg_type)
        if self.default_value is not None:
            self.default_value = ozmaql_value_from_raw(arg_value_type, self.default_value)
        return self


class OzmaDBMainEntity(OzmaDBModel):
    entity: OzmaDBEntityRef
    for_insert: bool


class OzmaDBResultViewInfo(OzmaDBModel):
    attribute_types: dict[str, OzmaDBViewAttributeInfo] = Field(default_factory=dict)
    row_attribute_types: dict[str, OzmaDBRowAttributeInfo] = Field(default_factory=dict)
    arguments: list[OzmaDBArgument]
    domains: dict[str, dict[str, OzmaDBDomainField]] = Field(default_factory=dict)
    main_entity: OzmaDBMainEntity | None = None
    columns: list[OzmaDBResultColumnInfo]
    hash: str


AttributesMap = dict[AttributeName, OzmaQLValue]


class OzmaDBExecutedValue(OzmaDBModel):
    value: OzmaQLValue
    attributes: AttributesMap | None = None
    pun: OzmaQLValue | None = None

    def _convert_values(self, column: OzmaDBResultColumnInfo):
        self.value = ozmaql_value_from_raw(column.value_type, self.value)
        if self.pun is not None:
            assert column.pun_type is not None
            self.pun = ozmaql_value_from_raw(column.pun_type, self.pun)
        if self.attributes is not None:
            for name, value in self.attributes.items():
                self.attributes[name] = ozmaql_value_from_raw(column.attribute_types[name].type, value)


class OzmaDBEntityId(OzmaDBModel):
    id: RowId
    sub_entity: OzmaDBEntityRef | None = None


class OzmaDBExecutedRow(OzmaDBModel):
    values: list[OzmaDBExecutedValue]
    domain_id: DomainId | None = None
    attributes: AttributesMap | None = None
    entity_ids: dict[ColumnName, OzmaDBEntityId] | None = None
    main_id: RowId | None = None
    main_sub_entity: OzmaDBEntityRef | None = None

    def _convert_values(self, info: OzmaDBResultViewInfo):
        for column, value in zip(info.columns, self.values):
            value._convert_values(column)
        if self.attributes is not None:
            for name, value in self.attributes.items():
                self.attributes[name] = ozmaql_value_from_raw(info.row_attribute_types[name].type, value)


class OzmaDBExecutedViewExpr(OzmaDBModel):
    attributes: AttributesMap
    column_attributes: list[AttributesMap]
    argument_attributes: dict[ArgumentName, AttributesMap]
    rows: list[OzmaDBExecutedRow]

    def _convert_values(self, info: OzmaDBResultViewInfo):
        for row in self.rows:
            row._convert_values(info)
        for name, value in self.attributes.items():
            self.attributes[name] = ozmaql_value_from_raw(info.attribute_types[name].type, value)
        for column, attributes in zip(info.columns, self.column_attributes):
            for name, value in attributes.items():
                attributes[name] = ozmaql_value_from_raw(column.attribute_types[name].type, value)
        for argument in info.arguments:
            attributes = self.argument_attributes.get(argument.name)
            if attributes is not None:
                for name, value in attributes.items():
                    attributes[name] = ozmaql_value_from_raw(argument.attribute_types[name].type, value)


class OzmaDBViewExprResult(OzmaDBModel):
    info: OzmaDBResultViewInfo
    result: OzmaDBExecutedViewExpr

    @model_validator(mode="after")
    def _validate(self: "OzmaDBViewExprResult") -> "OzmaDBViewExprResult":
        self.result._convert_values(self.info)
        return self


class OzmaDBViewInfoResult(OzmaDBModel):
    info: OzmaDBResultViewInfo
    const_attributes: AttributesMap
    const_column_attributes: list[AttributesMap]
    const_argument_attributes: dict[ArgumentName, AttributesMap]

    @model_validator(mode="after")
    def _validate(self: "OzmaDBViewInfoResult") -> "OzmaDBViewInfoResult":
        for name, value in self.const_attributes.items():
            self.const_attributes[name] = ozmaql_value_from_raw(self.info.attribute_types[name].type, value)
        for column, attributes in zip(self.info.columns, self.const_column_attributes):
            for name, value in attributes.items():
                attributes[name] = ozmaql_value_from_raw(column.attribute_types[name].type, value)
        for argument in self.info.arguments:
            attributes = self.const_argument_attributes.get(argument.name)
            if attributes is not None:
                for name, value in attributes.items():
                    attributes[name] = ozmaql_value_from_raw(argument.attribute_types[name].type, value)
        return self


class OzmaDBChunkArgument(OzmaDBModel):
    type: str
    value: OzmaQLValueArg


class OzmaDBChunkWhere(OzmaDBModel):
    arguments: dict[ArgumentName, OzmaDBChunkArgument] | None = None
    expression: str


class OzmaDBQueryChunk(OzmaDBModel):
    limit: int | None = None
    offset: int | None = None
    where: OzmaDBChunkWhere | None = None
    search: str | None = None
