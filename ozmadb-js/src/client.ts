import {
  IEntityRef, IEntity, IUserViewRef, IQueryChunk, IApiError, IViewExprResult, IViewInfoResult,
  ITransaction, ITransactionResult, IActionRef, IActionResult, ArgumentName, IFieldRef,
  IDomainValuesResult, IViewExplainResult,
} from "./common";

/*
 * Low-level API client.
 */

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

const fetchJson = async (input: RequestInfo, init?: RequestInit): Promise<unknown> => {
  const response = await fetchFunDB(input, init);
  return response.json();
};

export interface ISaveSchemasOptions {
  skipPreloaded?: boolean;
}

export interface IRestoreSchemasOptions {
  dropOthers?: boolean;
}

interface IAnonymousUserViewRequest {
  query: string;
}

interface IUserViewEntriesRequest extends IQueryChunk {
  args?: Record<ArgumentName, any>;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

interface IAnonymousUserViewEntriesRequest extends IUserViewEntriesRequest, IAnonymousUserViewRequest { }

interface IUserViewExplainFlags {
  verbose?: boolean;
  analyze?: boolean;
  costs?: boolean;
}

interface IUserViewExplainRequest extends IQueryChunk, IUserViewExplainFlags {
  args?: Record<ArgumentName, any>;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

interface IAnonymousUserViewExplainRequest extends IUserViewExplainRequest, IAnonymousUserViewRequest { }

interface IDomainsRequest extends IQueryChunk {
  rowId?: number;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

export interface IUserViewOpts {
  chunk?: IQueryChunk;
  pretendUser?: string;
  pretendRole?: IEntityRef;
}

export interface IUserViewExplainOpts extends IUserViewOpts, IUserViewExplainFlags {
}

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
    return response.blob();
  }

  private async fetchJsonApi(subUrl: string, token: string | null, method: string, body?: unknown): Promise<unknown> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    return fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers,
      body: JSON.stringify(body),
    });
  }

  private async fetchSendFileApi(subUrl: string, token: string | null, method: string, contentType: string, body: Blob): Promise<unknown> {
    const headers: Record<string, string> = {
      "Content-Type": contentType,
    };
    if (token !== null) {
      headers["Authorization"] = `Bearer ${token}`;
    }
    return fetchJson(`${this.apiUrl}/${subUrl}`, {
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

  getAnonymousUserView = async (token: string | null, query: string, args?: Record<string, unknown>, opts?: IUserViewOpts): Promise<IViewExprResult> => {
    const req: IAnonymousUserViewEntriesRequest = {
        query,
        args,
        ...opts?.chunk,
        pretendRole: opts?.pretendRole,
    };
    return this.getUserView("anonymous", token, req);
  };

  getNamedUserView = async (token: string | null, ref: IUserViewRef, args?: Record<string, unknown>, opts?: IUserViewOpts): Promise<IViewExprResult> => {
    const req: IUserViewEntriesRequest = {
        args,
        ...opts?.chunk,
        pretendRole: opts?.pretendRole,
    };
    return this.getUserView(`by_name/${ref.schema}/${ref.name}`, token, req);
  };

  getNamedUserViewInfo = async (token: string | null, ref: IUserViewRef): Promise<IViewInfoResult> => {
    return this.getUserViewInfo(`by_name/${ref.schema}/${ref.name}`, token, {});
  };

  getAnonymousUserViewExplain = async (token: string | null, query: string, args?: Record<string, unknown>, opts?: IUserViewExplainOpts): Promise<IViewExplainResult> => {
    const req: IAnonymousUserViewExplainRequest = {
      query,
      args,
      ...opts?.chunk,
      verbose: opts?.verbose,
      analyze: opts?.analyze,
      costs: opts?.costs,
      pretendUser: opts?.pretendUser,
      pretendRole: opts?.pretendRole,
    };
    return this.getUserViewExplain("anonymous", token, req);
  };

  getNamedUserViewExplain = async (token: string | null, ref: IUserViewRef, args?: Record<string, unknown>, opts?: IUserViewExplainOpts): Promise<IViewExplainResult> => {
    const req: IUserViewExplainRequest = {
      args,
      ...opts?.chunk,
      verbose: opts?.verbose,
      analyze: opts?.analyze,
      costs: opts?.costs,
      pretendUser: opts?.pretendUser,
      pretendRole: opts?.pretendRole,
    };
    return this.getUserViewExplain(`by_name/${ref.schema}/${ref.name}`, token, req);
  };

  getEntityInfo = async (token: string | null, ref: IEntityRef): Promise<IEntity> => {
    return this.fetchJsonApi(`entities/${ref.schema}/${ref.name}`, token, "GET") as Promise<IEntity>;
  };

  runTransaction = async (token: string | null, action: ITransaction): Promise<ITransactionResult> => {
    return this.fetchJsonApi("transaction", token, "POST", action) as Promise<ITransactionResult>;
  };

  runAction = async (token: string | null, ref: IActionRef, args?: Record<string, unknown>): Promise<IActionResult> => {
    return this.fetchJsonApi(`actions/${ref.schema}/${ref.name}`, token, "POST", args ?? {}) as Promise<IActionResult>;
  };

  getDomainValues = async (token: string | null, ref: IFieldRef, rowId?: number, opts?: IUserViewOpts): Promise<IDomainValuesResult> => {
    const req: IDomainsRequest = {
      rowId,
      ...opts?.chunk,
      pretendRole: opts?.pretendRole,
    };
    return this.fetchJsonApi(`domains/${ref.entity.schema}/${ref.entity.name}/${ref.name}`, token, "POST", req) as Promise<IDomainValuesResult>;
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
    await this.fetchSendFileApi(`layouts?${params}`, token, "PUT", "application/zip", data);
  };
}
