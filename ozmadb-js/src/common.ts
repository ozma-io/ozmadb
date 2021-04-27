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

export type SimpleType = "int" | "decimal" | "string" | "bool" | "datetime" | "date" | "interval" | "json" | "uuid";

export interface IScalarSimpleType {
  type: SimpleType;
}

export interface IArraySimpleType {
  type: "array";
  subtype: SimpleType;
}

export type ValueType = IScalarSimpleType | IArraySimpleType;

/*
 * Column field types. More high-level, include references and enumerations.
 */

export type FieldValueType = "int" | "decimal" | "string" | "bool" | "datetime" | "date" | "interval" | "json" | "uuid";

export interface IScalarFieldType {
  type: FieldValueType;
}

export interface IArrayFieldType {
  type: "array";
  subtype: FieldValueType;
}

export interface IReferenceFieldType {
  type: "reference";
  entity: IEntityRef;
}

export interface IEnumFieldType {
  type: "enum";
  values: string[];
}

export type FieldType = IScalarFieldType | IArrayFieldType | IReferenceFieldType | IEnumFieldType;

/*
 * Database entity definition.
 */

export interface IColumnField {
  fieldType: FieldType;
  valueType: ValueType;
  defaultValue: unknown;
  isNullable: boolean;
  isImmutable: boolean;
  inheritedFrom?: IEntityRef;
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

export interface IEntity {
  columnFields: Record<FieldName, IColumnField>;
  computedFields: Record<FieldName, IComputedField>;
  uniqueConstraints: Record<ConstraintName, IUniqueConstraint>;
  checkConstraints: Record<ConstraintName, ICheckConstraint>;
  mainField: FieldName;
  forbidExternalReferences: boolean;
  parents: IEntityRef[];
  children: IChildEntity[];
  isAbstract: boolean;
  isFrozen: boolean;
  root: IEntityRef;
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
export type AttributeTypesMap = Record<AttributeName, ValueType>;

export interface IResultColumnInfo {
  name: string;
  attributeTypes: AttributeTypesMap;
  cellAttributeTypes: AttributeTypesMap;
  valueType: ValueType;
  punType?: ValueType;
  mainField?: IMainFieldInfo;
}

export interface IDomainField {
  ref: IFieldRef;
  field?: IColumnField;
  idColumn: ColumnName;
}

export interface IResultViewInfo {
  attributeTypes: AttributeTypesMap;
  rowAttributeTypes: AttributeTypesMap;
  domains: Record<DomainId, Record<ColumnName, IDomainField>>;
  mainEntity?: IEntityRef;
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
  rows: IExecutedRow[];
}

export interface IExplainedQuery {
  query: string;
  explanation: string;
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
  pureAttributes: AttributesMap;
  pureColumnAttributes: AttributesMap[];
}

export interface IViewExplainResult {
  rows: IExplainedQuery;
  attributes?: IExplainedQuery;
}

/*
 * Transaction operations.
 */

export interface IInsertEntityOp {
  type: "insert";
  entity: IEntityRef;
  entries: Record<FieldName, unknown>;
}

export interface IUpdateEntityOp {
  type: "update";
  entity: IEntityRef;
  id: number;
  entries: Record<FieldName, unknown>;
}

export interface IDeleteEntityOp {
  type: "delete";
  entity: IEntityRef;
  id: number;
}

export type TransactionOp = IInsertEntityOp | IUpdateEntityOp | IDeleteEntityOp;

export interface ITransaction {
  operations: TransactionOp[];
  deferConstraints?: boolean;
}

/*
 * Transaction API responses.
 */

export interface IInsertEntityResult {
  type: "insert";
  id: number | null;
}

export interface IUpdateEntityResult {
  type: "update";
}

export interface IDeleteEntityResult {
  type: "delete";
}

export type TransactionOpResult = IInsertEntityResult | IUpdateEntityResult | IDeleteEntityResult;

export interface ITransactionResult {
  results: TransactionOpResult[];
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

export type RequestErrorType = "internal" | "request" | "no_endpoint" | "no_instance" | "access_denied" | "concurrent_update";

export interface IApiError {
  error: string;
  message: string;
}

export type UserViewErrorType = RequestErrorType | "not_found" | "compilation" | "execution" | "arguments";

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

/*
 * Helpful functions.
 */

export const goodName = (name: string): boolean => {
  return !(name === "" || name.includes(' ') || name.includes('/') || name.includes("__"));
}