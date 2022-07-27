/*
 * Basic type definitions.
 */

export type DomainId = number;
export type RowId = number;
export type FieldName = string;
export type EntityName = string;
export type SchemaName = string;
export type ColumnName = string;
export type ArgumentName = string;
export type AttributeName = string;
export type UserViewName = string;
export type ActionName = string;
export type ConstraintName = string;

/*
 * References to various database objects.
 */

export interface IEntityRef {
  schema: SchemaName;
  name: EntityName;
}

export type IRoleRef = IEntityRef;

export interface IFieldRef {
  entity: IEntityRef;
  name: FieldName;
}

export interface IUserViewRef {
  schema: SchemaName;
  name: UserViewName;
}

export interface IActionRef {
  schema: SchemaName;
  name: ActionName;
}

export interface IAnonymousUserView {
  type: "anonymous";
  query: string;
}

export interface INamedUserView {
  type: "named";
  ref: IUserViewRef;
}

export type UserViewSource = IAnonymousUserView | INamedUserView;

/*
 * Database result types.
 */

export type SimpleType = "int" | "decimal" | "string" | "bool" | "datetime" | "localdatetime" | "date" | "interval" | "json" | "uuid";

export interface IScalarSimpleType {
  type: SimpleType;
}

export interface IArraySimpleType {
  type: "array";
  subtype: IScalarSimpleType;
}

export type ValueType = IScalarSimpleType | IArraySimpleType;

/*
 * Column field types. More high-level, include references and enumerations.
 */

export type FieldSimpleValueType = "int" | "decimal" | "string" | "bool" | "datetime" | "date" | "interval" | "json" | "uuid";

export interface IScalarSimpleFieldType {
  type: FieldSimpleValueType;
}

export interface IReferenceScalarFieldType {
  type: "reference";
  entity: IEntityRef;
}

export interface IEnumScalarFieldType {
  type: "enum";
  values: string[];
}

export type ScalarFieldType = IScalarSimpleFieldType | IReferenceScalarFieldType | IEnumScalarFieldType;

export interface IArrayFieldType {
  type: "array";
  subtype: ScalarFieldType;
}

export type FieldType = ScalarFieldType | IArrayFieldType;

/*
 * Database entity definition.
 */

export interface IFieldAccess {
  select: boolean;
  update: boolean;
  insert: boolean;
}

export interface IColumnField {
  fieldType: FieldType;
  valueType: ValueType;
  defaultValue: unknown;
  isNullable: boolean;
  isImmutable: boolean;
  inheritedFrom?: IEntityRef;
  access: IFieldAccess;
  hasUpdateTriggers: boolean;
}

export type UsedFields = FieldName[];
export type UsedEntities = Record<EntityName, UsedFields>;
export type UsedSchemas = Record<SchemaName, UsedEntities>;

export interface IComputedField {
  expression: string;
  isLocal: boolean;
  hasId: boolean;
  usedSchemas: UsedSchemas;
  inheritedFrom?: IEntityRef;
  isVirtual: boolean;
}

export interface IUniqueConstraint {
  columns: FieldName[];
}

export interface ICheckConstraint {
  expression: string;
}

export interface IChildEntity {
  ref: IEntityRef;
  direct: boolean;
}

export interface IEntityAccess {
  select: boolean;
  delete: boolean;
  insert: boolean;
}

export interface IEntity {
  columnFields: Record<FieldName, IColumnField>;
  computedFields: Record<FieldName, IComputedField>;
  mainField: FieldName;
  forbidExternalReferences: boolean;
  parents: IEntityRef[];
  children: IChildEntity[];
  isAbstract: boolean;
  isFrozen: boolean;
  root: IEntityRef;
  access: IEntityAccess;
  hasInsertTriggers: boolean;
  hasDeleteTriggers: boolean;
}

export interface ISchema {
  entities: Record<EntityName, IEntity>;
}

export interface ILayout {
  schemas: Record<SchemaName, ISchema>;
}

/*
 * User view metadata. Includes columns, types etc.
 */

export interface IMainFieldInfo {
  name: FieldName;
  field: IColumnField;
}

export type AttributesMap = Record<AttributeName, unknown>;

export interface IBoundMappingEntry {
  when: unknown;
  value: unknown;
}

export interface IBoundMapping {
  entries: IBoundMappingEntry[];
  default?: unknown;
}

export interface IBasicAttributeInfo {
  type: ValueType;
}

export interface IMappedAttributeInfo extends IBasicAttributeInfo {
  mapping?: IBoundMapping;
}

export interface IBoundAttributeInfo extends IMappedAttributeInfo {
  const: boolean;
}
export type BoundAttributesInfoMap = Record<AttributeName, IBoundAttributeInfo>;

export interface ICellAttributeInfo extends IMappedAttributeInfo { }
export type CellAttributesInfoMap = Record<AttributeName, ICellAttributeInfo>;

export interface IViewAttributeInfo extends IBasicAttributeInfo {
  const: boolean;
}
export type ViewAttributesInfoMap = Record<AttributeName, IViewAttributeInfo>;

export interface IRowAttributeInfo extends IBasicAttributeInfo { }
export type RowAttributesInfoMap = Record<AttributeName, IRowAttributeInfo>;

export interface IResultColumnInfo {
  name: string;
  attributeTypes: BoundAttributesInfoMap;
  cellAttributeTypes: CellAttributesInfoMap;
  valueType: ValueType;
  punType?: ValueType;
  mainField?: IMainFieldInfo;
}

export interface IDomainField {
  ref: IFieldRef;
  field?: IColumnField;
  idColumn: ColumnName;
}

export interface IArgument {
  name: ArgumentName;
  argType: FieldType;
  optional: boolean;
  defaultValue?: any;
  attributeTypes: BoundAttributesInfoMap;
}

export interface IMainEntity {
  entity: IEntityRef;
  forInsert: boolean;
}

export interface IResultViewInfo {
  attributeTypes: ViewAttributesInfoMap;
  rowAttributeTypes: RowAttributesInfoMap;
  arguments: IArgument[];
  domains: Record<DomainId, Record<ColumnName, IDomainField>>;
  mainEntity?: IMainEntity;
  columns: IResultColumnInfo[];
  hash: string;
}

/*
 * User view data.
 */

export interface IExecutedValue {
  value: unknown;
  attributes?: AttributesMap;
  pun?: unknown;
}

export interface IEntityId {
  id: RowId;
  subEntity?: IEntityRef;
}

export interface IExecutedRow {
  values: IExecutedValue[];
  domainId: DomainId | null;
  attributes?: AttributesMap;
  entityIds?: Record<ColumnName, IEntityId>;
  mainId?: RowId;
  mainSubEntity?: IEntityRef;
}

export interface IExecutedViewExpr {
  attributes: AttributesMap;
  columnAttributes: AttributesMap[];
  argumentAttributes: Record<ArgumentName, AttributesMap>;
  rows: IExecutedRow[];
}

/*
 * User view API responses.
 */

export interface IViewExprResult {
  info: IResultViewInfo;
  result: IExecutedViewExpr;
}

export interface IViewInfoResult {
  info: IResultViewInfo;
  constAttributes: AttributesMap;
  constColumnAttributes: AttributesMap[];
  constArgumentAttributes: Record<ArgumentName, AttributesMap>;
}

/*
 * Transaction operations.
 */

export interface IAltRowKey {
  alt: ConstraintName;
  keys: Record<ArgumentName, any>;
}

export type RowKey = RowId | IAltRowKey;

export type ReferencesRowIndex = number;

export interface IReferencesChild {
  field: FieldName;
  row: ReferencesRowIndex;
}

export interface IReferencesNode {
  entity: IEntityRef;
  id: RowId;
  references: IReferencesChild[];
}

export interface IReferencesTree {
  nodes: IReferencesNode[];
  root: ReferencesRowIndex;
}

/*
 * Action API responses.
 */

export interface IActionResult {
  result: unknown;
}

/*
 * Domain API responses.
 */

export interface IDomainValue {
  value: any;
  pun?: any;
}

export interface IDomainValuesResult {
  values: IDomainValue[];
  punType: ValueType;
  hash: string;
}

/*
 * Error types.
 */

export type RequestErrorType = "internal" | "request" | "noEndpoint" | "noInstance" | "accessDenied" | "concurrentUpdate";

export interface IApiError {
  error: string;
  message: string;
}

export type UserViewErrorType = RequestErrorType | "notFound" | "compilation" | "execution" | "arguments";

/*
 * Extra types.
 */

export interface IChunkArgument {
  type: string;
  value: any;
}

export interface IChunkWhere {
  arguments?: Record<ArgumentName, IChunkArgument>;
  expression: string;
}

export interface IQueryChunk {
  limit?: number;
  offset?: number;
  where?: IChunkWhere;
}

export interface IPermissionsInfo {
  isRoot: boolean;
}
