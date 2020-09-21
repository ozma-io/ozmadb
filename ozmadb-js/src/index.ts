/*
 * Basic type definitions.
 */

export type DomainId = number;
export type RowId = number;
export type FieldName = string;
export type EntityName = string;
export type SchemaName = string;
export type ColumnName = string;
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

export type UserViewSource = IAnonymousUserView | INamedUserView

/*
 * Database result types.
 */

export type SimpleType = "int" | "decimal" | "string" | "bool" | "datetime" | "date" | "interval" | "json";

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

export type FieldValueType = "int" | "decimal" | "string" | "bool" | "datetime" | "date" | "interval" | "json";

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
  where?: string;
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
  defaultValue: any;
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
  parent?: IEntityRef;
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

export type AttributesMap = Record<AttributeName, any>;
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
}

/*
 * User view data.
 */

export interface IExecutedValue {
  value: any;
  attributes?: AttributesMap;
  pun?: any;
}

export interface IEntityId {
  id: RowId;
  subEntity?: IEntityRef;
}

export interface IExecutedRow {
  values: IExecutedValue[];
  domainId: DomainId;
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

/*
 * Transaction operations.
 */

export interface IInsertEntityOp {
  type: "insert";
  entity: IEntityRef;
  entries: Record<FieldName, any>;
}

export interface IUpdateEntityOp {
  type: "update";
  entity: IEntityRef;
  id: number;
  entries: Record<FieldName, any>;
}

export interface IDeleteEntityOp {
  type: "delete";
  entity: IEntityRef;
  id: number;
}

export type TransactionOp = IInsertEntityOp | IUpdateEntityOp | IDeleteEntityOp;

export interface ITransaction {
  operations: TransactionOp[];
}

/*
 * Transaction results.
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
 * Action results.
 */

export interface IActionResult {
  result: any;
}

/*
 * Error types.
 */

export interface IApiError {
  error: string;
  message: string;
}

export type UserViewErrorType = "access_denied" | "not_found" | "resolution" | "execution" | "arguments";

/*
 * Low-level API client.
 */

const convertArgs = (args: Record<string, any>): URLSearchParams => {
  const params = new URLSearchParams();
  Object.entries(args).forEach(([name, arg]) => {
    params.append(name, JSON.stringify(arg));
  });
  return params;
};

export class FunDBError extends Error {
  body: IApiError;

  constructor(body: IApiError) {
    super(body.message);
    this.body = body;
  }
}

const fetchFunDB = async (input: RequestInfo, init?: RequestInit): Promise<Response> => {
  const response = await fetch(input, init);
  if (!response.ok) {
    const body = await response.json();
    throw new FunDBError(body);
  }
  return response;
};

const fetchJson = async (input: RequestInfo, init?: RequestInit): Promise<any> => {
  const response = await fetchFunDB(input, init);
  return await response.json();
};

export default class FunDBAPI {
  private apiUrl: string;

  constructor(opts: { apiUrl: string }) {
    this.apiUrl = opts.apiUrl;
  }

  private async fetchGetFileApi(subUrl: string, token: string | null, accept: string): Promise<Blob> {
    const headers: Record<string, string> = {};
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    headers["Accept"] = accept;
    const response = await fetchFunDB(`${this.apiUrl}/${subUrl}`, {
      method: "GET",
      headers,
    });
    return await response.blob();
  }

  private async fetchJsonApi(subUrl: string, token: string | null, method: string, body?: any): Promise<any> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    return await fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers,
      body: JSON.stringify(body),
    });
  }

  private async fetchSendFileApi(subUrl: string, token: string | null, method: string, contentType: string, body: Blob): Promise<any> {
    const headers: Record<string, string> = {
      "Content-Type": contentType,
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    return await fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers,
      body,
    });
  }

  getUserView = async (path: string, token: string | null, args: URLSearchParams): Promise<IViewExprResult> => {
    return await this.fetchJsonApi(`views/${path}/entries?${args}`, token, "GET");
  }

  getUserViewInfo = async (path: string, token: string | null, args: URLSearchParams): Promise<IViewInfoResult> => {
    return await this.fetchJsonApi(`views/${path}/info?${args}`, token, "GET");
  }

  getAnonymousUserView = async (token: string | null, query: string, args: Record<string, any>): Promise<IViewExprResult> => {
    const search = convertArgs(args);
    search.set("__query", query);
    return await this.getUserView("anonymous", token, search);
  }

  getNamedUserView = async (token: string | null, ref: IUserViewRef, args: Record<string, any>): Promise<IViewExprResult> => {
    return await this.getUserView(`by_name/${ref.schema}/${ref.name}`, token, convertArgs(args));
  }

  getNamedUserViewInfo = async (token: string | null, ref: IUserViewRef): Promise<IViewInfoResult> => {
    return await this.getUserViewInfo(`by_name/${ref.schema}/${ref.name}`, token, new URLSearchParams());
  }

  getEntityInfo = async (token: string | null, ref: IEntityRef): Promise<IEntity> => {
    return await this.fetchJsonApi(`entities/${ref.schema}/${ref.name}`, token, "GET");
  }

  runTransaction = async (token: string | null, action: ITransaction): Promise<ITransactionResult> => {
    return await this.fetchJsonApi("transaction", token, "POST", action);
  };

  runAction = async (token: string | null, ref: IActionRef, args: Record<string, any>): Promise<IActionResult> => {
    return await this.fetchJsonApi(`actions/${ref.schema}/${ref.name}`, token, "POST", args);
  };

  saveSchemas = async (token: string | null, schemas: string[] | null): Promise<Blob> => {
    let url = "layouts";
    if (schemas !== null) {
      const params = new URLSearchParams();
      schemas.forEach(name => {
        params.append("schema", name);
      });
      url += `?${params}`;
    }
    return await this.fetchGetFileApi(url, token, "application/zip");
  };

  restoreSchemas = async (token: string | null, data: Blob): Promise<void> => {
    await this.fetchSendFileApi(`layouts`, token, "PUT", "application/zip", data);
  };
}
