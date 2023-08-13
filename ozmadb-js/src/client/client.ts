import {
  IEntityRef, IEntity, IUserViewRef, IQueryChunk, ApiError, IViewExprResult, IViewInfoResult,
  IActionRef, IActionResult, ArgumentName, IFieldRef, IAccessDeniedError, IRequestError,
  IDomainValuesResult, FieldName, IPermissionsInfo, RowKey, RowId, IBasicError,
} from "../types";

/*
 * Low-level API client.
 */

export interface INetworkFailureError extends IBasicError {
  error: "networkFailure";
}

export interface IUnsupportedMediaTypeError extends IBasicError {
  error: "unsupportedMediaType";
}

export interface IUnacceptableError extends IBasicError {
  error: "unacceptable";
}

export interface ICanceledError extends IBasicError {
  error: "canceled";
}

export interface IJobNotFoundError extends IBasicError {
  error: "jobNotFound";
}

export interface INotFinishedError extends IBasicError {
  error: "notFinished";
  id: string;
}

type CommonClientApiError = ApiError | IUnsupportedMediaTypeError | IUnacceptableError | ICanceledError | IJobNotFoundError;

export type ClientApiError = CommonClientApiError | INetworkFailureError;

type ClientRawApiError = CommonClientApiError | INotFinishedError;

export class FunDBError extends Error {
  body: ClientApiError;

  constructor(body: ClientApiError) {
    super(body.message);
    this.body = body;
  }
}

const fetchRawFunDB = async (input: RequestInfo, init?: RequestInit): Promise<Response> => {
  try {
    return await fetch(input, init);
  } catch (e) {
    if (e instanceof TypeError) {
      throw new FunDBError({ error: "networkFailure", message: e.message });
    } else {
      throw e;
    }
  }
};

interface IExplainFlags {
  verbose?: boolean;
  analyze?: boolean;
  costs?: boolean;
}

export interface IExplainedQuery {
  query: string;
  parameters: Record<number, any>;
  explanation: object;
}

export interface IViewExplainResult {
  rows: IExplainedQuery;
  attributes?: IExplainedQuery;
}

interface IAnonymousUserViewRequest {
  query: string;
}

interface IUserViewCommonRequest extends IQueryChunk {
  args?: Record<ArgumentName, any>;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

interface IUserViewEntriesRequest extends IUserViewCommonRequest { }

interface IAnonymousUserViewEntriesRequest extends IUserViewEntriesRequest, IAnonymousUserViewRequest { }

interface IUserViewExplainRequest extends IUserViewCommonRequest, IExplainFlags { }

interface IAnonymousUserViewExplainRequest extends IUserViewExplainRequest, IAnonymousUserViewRequest { }

interface IDomainsCommonRequest extends IQueryChunk {
  rowId?: number;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

interface IDomainsRequest extends IDomainsCommonRequest { }

interface IDomainsExplainRequest extends IDomainsCommonRequest, IExplainFlags { }

export interface IInfoRequestOpts { }

export interface IEntriesRequestOpts {
  chunk?: IQueryChunk;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

export interface IEntriesExplainOpts extends IEntriesRequestOpts, IExplainFlags { }

export interface IInsertEntityOp {
  type: "insert";
  entity: IEntityRef;
  fields: Record<FieldName, unknown>;
}

export interface IUpdateEntityOp {
  type: "update";
  entity: IEntityRef;
  id: RowKey;
  fields: Record<FieldName, unknown>;
}

export interface IDeleteEntityOp {
  type: "delete";
  entity: IEntityRef;
  id: RowKey;
}

export interface ICommandOp {
  type: "command";
  command: string;
  arguments?: Record<ArgumentName, any>;
}

export type TransactionOp = IInsertEntityOp | IUpdateEntityOp | IDeleteEntityOp | ICommandOp;

export interface ITransaction {
  operations: TransactionOp[];
  deferConstraints?: boolean;
  forceAllowBroken?: boolean;
}

export interface IInsertEntityResult {
  type: "insert";
  id: RowId | null;
}

export interface IUpdateEntityResult {
  type: "update";
  id: RowId;
}

export interface IDeleteEntityResult {
  type: "delete";
}

export type TransactionOpResult = IInsertEntityResult | IUpdateEntityResult | IDeleteEntityResult;

export interface ITransactionResult {
  results: TransactionOpResult[];
}

export interface ISaveSchemasOptions {
  skipPreloaded?: boolean;
}

export interface IRestoreSchemasOptions {
  dropOthers?: boolean;
  forceAllowBroken?: boolean;
}

const commonHeaders = {
  "X-FunDB-LongRunning": "hybrid",
};

export default class FunDBClient {
  private apiUrl: string;

  constructor(opts: { apiUrl: string }) {
    this.apiUrl = opts.apiUrl;
  }

  private async fetchFunDB(input: RequestInfo, init?: RequestInit): Promise<Response> {
    const response = await fetchRawFunDB(input, init);
    if (!response.ok) {
      const body = await response.json() as ClientRawApiError;
      if (body.error === "notFinished") {
        let jobId = body.id;
        while (true) {
          // eslint-disable-next-line no-await-in-loop
          const newResponse = await this.fetchFunDB(`${this.apiUrl}/jobs/${jobId}/result`, {
            headers: init?.headers ?? {},
          });
          if (!newResponse.ok) {
            // eslint-disable-next-line no-await-in-loop
            const newBody = await response.json() as ClientRawApiError;
            if (newBody.error === "notFinished") {
              jobId = newBody.id;
            } else {
              throw new FunDBError(newBody);
            }
          } else {
            return newResponse;
          }
        }
      } else {
        throw new FunDBError(body);
      }
    } else {
      return response;
    }
  }

  private async fetchJson(input: RequestInfo, init?: RequestInit): Promise<unknown> {
    const response = await this.fetchFunDB(input, init);
    return response.json();
  }

  private async fetchGetFileApi(subUrl: string, token: string | null, accept: string): Promise<Blob> {
    const headers: Record<string, string> = {
      ...commonHeaders,
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    headers["Accept"] = accept;
    const response = await this.fetchFunDB(`${this.apiUrl}/${subUrl}`, {
      method: "GET",
      headers,
    });
    return response.blob();
  }

  private async fetchJsonApi(subUrl: string, token: string | null, method: string, body?: unknown): Promise<unknown> {
    const headers: Record<string, string> = {
      ...commonHeaders,
      "Content-Type": "application/json",
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    return this.fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers,
      body: JSON.stringify(body),
    });
  }

  private async fetchSendFileApi(subUrl: string, token: string | null, method: string, contentType: string, body: Blob): Promise<unknown> {
    const headers: Record<string, string> = {
      ...commonHeaders,
      "Content-Type": contentType,
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    return this.fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers,
      body,
    });
  }

  // We use `POST` method to ensure we won't bloat URLs and to have full access to Chunk APIs.
  private getUserView = async (path: string, token: string | null, body: any): Promise<IViewExprResult> => {
    return this.fetchJsonApi(`views/${path}/entries`, token, "POST", body) as Promise<IViewExprResult>;
  };

  private getUserViewInfo = async (path: string, token: string | null, body: any): Promise<IViewInfoResult> => {
    return this.fetchJsonApi(`views/${path}/info`, token, "POST", body) as Promise<IViewInfoResult>;
  };

  private getUserViewExplain = async (path: string, token: string | null, body: any): Promise<IViewExplainResult> => {
    return this.fetchJsonApi(`views/${path}/explain`, token, "POST", body) as Promise<IViewExplainResult>;
  };

  getAnonymousUserView = async (token: string | null, query: string, args?: Record<string, unknown>, opts?: IEntriesRequestOpts): Promise<IViewExprResult> => {
    const req: IAnonymousUserViewEntriesRequest = {
      ...opts,
      query,
      args,
    };
    return this.getUserView("anonymous", token, req);
  };

  getNamedUserView = async (token: string | null, ref: IUserViewRef, args?: Record<string, unknown>, opts?: IEntriesRequestOpts): Promise<IViewExprResult> => {
    const req: IUserViewEntriesRequest = {
      ...opts,
      args,
    };
    return this.getUserView(`by_name/${ref.schema}/${ref.name}`, token, req);
  };

  getNamedUserViewInfo = async (token: string | null, ref: IUserViewRef, opts?: IInfoRequestOpts): Promise<IViewInfoResult> => {
    return this.getUserViewInfo(`by_name/${ref.schema}/${ref.name}`, token, opts);
  };

  getAnonymousUserViewExplain = async (token: string | null, query: string, args?: Record<string, unknown>, opts?: IEntriesExplainOpts): Promise<IViewExplainResult> => {
    const req: IAnonymousUserViewExplainRequest = {
      ...opts,
      query,
      args,
    };
    return this.getUserViewExplain("anonymous", token, req);
  };

  getNamedUserViewExplain = async (token: string | null, ref: IUserViewRef, args?: Record<string, unknown>, opts?: IEntriesExplainOpts): Promise<IViewExplainResult> => {
    const req: IUserViewExplainRequest = {
      ...opts,
      args,
    };
    return this.getUserViewExplain(`by_name/${ref.schema}/${ref.name}`, token, req);
  };

  getEntityInfo = async (token: string | null, ref: IEntityRef): Promise<IEntity> => {
    return this.fetchJsonApi(`entities/${ref.schema}/${ref.name}/info`, token, "GET") as Promise<IEntity>;
  };

  runTransaction = async (token: string | null, action: ITransaction): Promise<ITransactionResult> => {
    return this.fetchJsonApi("transaction", token, "POST", action) as Promise<ITransactionResult>;
  };

  runAction = async (token: string | null, ref: IActionRef, args?: Record<string, unknown>): Promise<IActionResult> => {
    return this.fetchJsonApi(`actions/${ref.schema}/${ref.name}/run`, token, "POST", args ?? {}) as Promise<IActionResult>;
  };

  getDomainValues = async (token: string | null, ref: IFieldRef, rowId?: number, opts?: IEntriesRequestOpts): Promise<IDomainValuesResult> => {
    const req: IDomainsRequest = {
      ...opts,
      rowId,
    };
    return this.fetchJsonApi(`domains/${ref.entity.schema}/${ref.entity.name}/${ref.name}/entries`, token, "POST", req) as Promise<IDomainValuesResult>;
  };

  getDomainExplain = async (token: string | null, ref: IFieldRef, rowId?: number, opts?: IEntriesExplainOpts): Promise<IExplainedQuery> => {
    const req: IDomainsExplainRequest = {
      ...opts,
      rowId,
    };
    return this.fetchJsonApi(`domains/${ref.entity.schema}/${ref.entity.name}/${ref.name}/explain`, token, "POST", req) as Promise<IExplainedQuery>;
  };

  saveSchemas = async (token: string | null, schemas: string[] | "all", options?: ISaveSchemasOptions): Promise<Blob> => {
    const params = new URLSearchParams();
    if (schemas !== "all") {
      schemas.forEach(name => {
        params.append("schema", name);
      });
    }
    if (options?.skipPreloaded) {
      params.append("skip_preloaded", "true");
    }
    return this.fetchGetFileApi(`layouts?${params}`, token, "application/zip");
  };

  restoreSchemas = async (token: string | null, data: Blob, options?: IRestoreSchemasOptions): Promise<void> => {
    const params = new URLSearchParams();
    if (options?.dropOthers) {
      params.append("drop_others", "true");
    }
    if (options?.forceAllowBroken) {
      params.append("force_allow_broken", "true");
    }
    await this.fetchSendFileApi(`layouts?${params}`, token, "PUT", "application/zip", data);
  };

  getPermissions = async (token: string | null): Promise<IPermissionsInfo> => {
    return this.fetchJsonApi("permissions", token, "GET") as Promise<IPermissionsInfo>;
  };
}
