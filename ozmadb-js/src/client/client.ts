import {
  IEntityRef,
  IEntity,
  IUserViewRef,
  IQueryChunk,
  ApiError,
  IViewExprResult,
  IViewInfoResult,
  IActionRef,
  IActionResult,
  ArgumentName,
  IFieldRef,
  IDomainValuesResult,
  FieldName,
  IPermissionsInfo,
  RowKey,
  RowId,
  IBasicError,
} from '../types'

/*
 * Low-level API client.
 */

export interface INetworkFailureError extends IBasicError {
  error: 'networkFailure'
}

export interface IRateExceededError extends IBasicError {
  error: 'rateExceeded'
}

export interface INoEndpointError extends IBasicError {
  error: 'noEndpoint'
}

export interface INoInstanceError extends IBasicError {
  error: 'noInstance'
}

export interface IUnauthorizedError extends IBasicError {
  error: 'unauthorized'
}

export interface IUnsupportedMediaTypeError extends IBasicError {
  error: 'unsupportedMediaType'
}

export interface IUnacceptableError extends IBasicError {
  error: 'unacceptable'
}

export interface ICanceledError extends IBasicError {
  error: 'canceled'
}

export interface IJobNotFoundError extends IBasicError {
  error: 'jobNotFound'
}

export interface INotFinishedError extends IBasicError {
  error: 'notFinished'
  id: string
}

type CommonClientHttpError =
  | IRateExceededError
  | INoEndpointError
  | INoInstanceError
  | IUnauthorizedError
  | IUnsupportedMediaTypeError
  | IUnacceptableError
  | ICanceledError
  | IJobNotFoundError

type ClientRawApiError = ApiError | CommonClientHttpError | INotFinishedError

export type ClientHttpError = CommonClientHttpError | INetworkFailureError

export type ClientApiError = ApiError | ClientHttpError

export class OzmaDBError extends Error {
  body: ClientApiError

  constructor(body: ClientApiError) {
    super(body.message)
    this.body = body
  }
}

const fetchRawOzmaDB = async (
  input: RequestInfo,
  init?: RequestInit,
): Promise<Response> => {
  try {
    return await fetch(input, init)
  } catch (e) {
    if (e instanceof TypeError) {
      throw new OzmaDBError({ error: 'networkFailure', message: e.message })
    } else {
      throw e
    }
  }
}

interface IExplainFlags {
  verbose?: boolean
  analyze?: boolean
  costs?: boolean
}

export interface IExplainedQuery {
  query: string
  parameters: Record<number, any>
  explanation: object
}

export interface IViewExplainResult {
  rows: IExplainedQuery
  attributes?: IExplainedQuery
}

interface IAnonymousUserViewRequest {
  query: string
}

interface IUserViewCommonRequest extends IQueryChunk {
  args?: Record<ArgumentName, any>
  pretendUser?: string
  pretendRole?: IEntityRef
}

interface IUserViewEntriesRequest extends IUserViewCommonRequest {}

interface IAnonymousUserViewEntriesRequest
  extends IUserViewEntriesRequest,
    IAnonymousUserViewRequest {}

interface IUserViewExplainRequest
  extends IUserViewCommonRequest,
    IExplainFlags {}

interface IAnonymousUserViewExplainRequest
  extends IUserViewExplainRequest,
    IAnonymousUserViewRequest {}

interface IDomainsCommonRequest extends IQueryChunk {
  rowId?: number
  pretendUser?: string
  pretendRole?: IEntityRef
}

interface IDomainsRequest extends IDomainsCommonRequest {}

interface IDomainsExplainRequest extends IDomainsCommonRequest, IExplainFlags {}

export interface IInfoRequestOpts {}

export interface IEntriesRequestOpts {
  chunk?: IQueryChunk
  pretendUser?: string
  pretendRole?: IEntityRef
}

export interface IEntriesExplainOpts
  extends IEntriesRequestOpts,
    IExplainFlags {}

export interface IInsertEntityOp {
  type: 'insert'
  entity: IEntityRef
  fields: Record<FieldName, unknown>
}

export interface IUpdateEntityOp {
  type: 'update'
  entity: IEntityRef
  id: RowKey
  fields: Record<FieldName, unknown>
}

export interface IDeleteEntityOp {
  type: 'delete'
  entity: IEntityRef
  id: RowKey
}

export interface ICommandOp {
  type: 'command'
  command: string
  arguments?: Record<ArgumentName, any>
}

export type TransactionOp =
  | IInsertEntityOp
  | IUpdateEntityOp
  | IDeleteEntityOp
  | ICommandOp

export interface ITransaction {
  operations: TransactionOp[]
  deferConstraints?: boolean
  forceAllowBroken?: boolean
}

export interface IInsertEntityResult {
  type: 'insert'
  id: RowId | null
}

export interface IUpdateEntityResult {
  type: 'update'
  id: RowId
}

export interface IDeleteEntityResult {
  type: 'delete'
}

export type TransactionOpResult =
  | IInsertEntityResult
  | IUpdateEntityResult
  | IDeleteEntityResult

export interface ITransactionResult {
  results: TransactionOpResult[]
}

export interface ISaveSchemasOptions {
  skipPreloaded?: boolean
}

export interface IRestoreSchemasOptions {
  dropOthers?: boolean
  forceAllowBroken?: boolean
}

const commonHeaders = {
  'X-OzmaDB-LongRunning': 'hybrid',
}

interface IOzmaDBRequestInit {
  method?: string
  headers?: Record<string, string>
  body?: Blob | string
}

export default class OzmaDBClient {
  apiUrl: string
  token: null | string

  constructor(opts: { apiUrl: string; token?: string }) {
    this.apiUrl = opts.apiUrl
    this.token = opts.token ?? null
  }

  private async fetchOzmaDB(
    url: string,
    opts?: IOzmaDBRequestInit,
  ): Promise<Response> {
    const headers: Record<string, string> = {
      ...commonHeaders,
      ...opts?.headers,
    }
    if (this.token !== null) {
      headers['Authorization'] = `Bearer ${this.token}`
    }
    const response = await fetchRawOzmaDB(url, { ...opts, headers })
    if (!response.ok) {
      const body = (await response.json()) as ClientRawApiError
      if (body.error === 'notFinished') {
        let jobId = body.id
        while (true) {
          // eslint-disable-next-line no-await-in-loop
          const newResponse = await this.fetchOzmaDB(
            `${this.apiUrl}/jobs/${jobId}/result`,
            {
              headers:
                this.token === null
                  ? undefined
                  : {
                      Authorization: `Bearer ${this.token}`,
                    },
            },
          )
          if (!newResponse.ok) {
            // eslint-disable-next-line no-await-in-loop
            const newBody = (await response.json()) as ClientRawApiError
            if (newBody.error === 'notFinished') {
              jobId = newBody.id
            } else {
              throw new OzmaDBError(newBody)
            }
          } else {
            return newResponse
          }
        }
      } else {
        throw new OzmaDBError(body)
      }
    } else {
      return response
    }
  }

  private async fetchJson(
    url: string,
    opts?: IOzmaDBRequestInit,
  ): Promise<unknown> {
    const response = await this.fetchOzmaDB(url, opts)
    return response.json()
  }

  private async fetchGetFileApi(subUrl: string, accept: string): Promise<Blob> {
    const response = await this.fetchOzmaDB(`${this.apiUrl}/${subUrl}`, {
      method: 'GET',
      headers: {
        Accept: accept,
      },
    })
    return response.blob()
  }

  private fetchJsonApi(
    subUrl: string,
    method: string,
    body?: unknown,
  ): Promise<unknown> {
    return this.fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    })
  }

  private fetchSendFileApi(
    subUrl: string,
    method: string,
    contentType: string,
    body: Blob,
  ): Promise<unknown> {
    return this.fetchJson(`${this.apiUrl}/${subUrl}`, {
      method,
      headers: {
        'Content-Type': contentType,
      },
      body,
    })
  }

  // We use `POST` method to ensure we won't bloat URLs and to have full access to Chunk APIs.
  private getUserView = (path: string, body: any): Promise<IViewExprResult> => {
    return this.fetchJsonApi(
      `views/${path}/entries`,
      'POST',
      body,
    ) as Promise<IViewExprResult>
  }

  private getUserViewInfo = (
    path: string,
    body: any,
  ): Promise<IViewInfoResult> => {
    return this.fetchJsonApi(
      `views/${path}/info`,
      'POST',
      body,
    ) as Promise<IViewInfoResult>
  }

  private getUserViewExplain = (
    path: string,
    body: any,
  ): Promise<IViewExplainResult> => {
    return this.fetchJsonApi(
      `views/${path}/explain`,
      'POST',
      body,
    ) as Promise<IViewExplainResult>
  }

  getAnonymousUserView = (
    query: string,
    args?: Record<string, unknown>,
    opts?: IEntriesRequestOpts,
  ): Promise<IViewExprResult> => {
    const req: IAnonymousUserViewEntriesRequest = {
      ...opts,
      query,
      args,
    }
    return this.getUserView('anonymous', req)
  }

  getNamedUserView = (
    ref: IUserViewRef,
    args?: Record<string, unknown>,
    opts?: IEntriesRequestOpts,
  ): Promise<IViewExprResult> => {
    const req: IUserViewEntriesRequest = {
      ...opts,
      args,
    }
    return this.getUserView(`by_name/${ref.schema}/${ref.name}`, req)
  }

  getNamedUserViewInfo = (
    ref: IUserViewRef,
    opts?: IInfoRequestOpts,
  ): Promise<IViewInfoResult> => {
    return this.getUserViewInfo(`by_name/${ref.schema}/${ref.name}`, opts)
  }

  getAnonymousUserViewExplain = (
    query: string,
    args?: Record<string, unknown>,
    opts?: IEntriesExplainOpts,
  ): Promise<IViewExplainResult> => {
    const req: IAnonymousUserViewExplainRequest = {
      ...opts,
      query,
      args,
    }
    return this.getUserViewExplain('anonymous', req)
  }

  getNamedUserViewExplain = (
    ref: IUserViewRef,
    args?: Record<string, unknown>,
    opts?: IEntriesExplainOpts,
  ): Promise<IViewExplainResult> => {
    const req: IUserViewExplainRequest = {
      ...opts,
      args,
    }
    return this.getUserViewExplain(`by_name/${ref.schema}/${ref.name}`, req)
  }

  getEntityInfo = (ref: IEntityRef): Promise<IEntity> => {
    return this.fetchJsonApi(
      `entities/${ref.schema}/${ref.name}/info`,
      'GET',
    ) as Promise<IEntity>
  }

  runTransaction = (action: ITransaction): Promise<ITransactionResult> => {
    return this.fetchJsonApi(
      'transaction',
      'POST',
      action,
    ) as Promise<ITransactionResult>
  }

  runAction = (
    ref: IActionRef,
    args?: Record<string, unknown>,
  ): Promise<IActionResult> => {
    return this.fetchJsonApi(
      `actions/${ref.schema}/${ref.name}/run`,
      'POST',
      args ?? {},
    ) as Promise<IActionResult>
  }

  getDomainValues = (
    ref: IFieldRef,
    rowId?: number,
    opts?: IEntriesRequestOpts,
  ): Promise<IDomainValuesResult> => {
    const req: IDomainsRequest = {
      ...opts,
      rowId,
    }
    return this.fetchJsonApi(
      `domains/${ref.entity.schema}/${ref.entity.name}/${ref.name}/entries`,
      'POST',
      req,
    ) as Promise<IDomainValuesResult>
  }

  getDomainExplain = (
    ref: IFieldRef,
    rowId?: number,
    opts?: IEntriesExplainOpts,
  ): Promise<IExplainedQuery> => {
    const req: IDomainsExplainRequest = {
      ...opts,
      rowId,
    }
    return this.fetchJsonApi(
      `domains/${ref.entity.schema}/${ref.entity.name}/${ref.name}/explain`,
      'POST',
      req,
    ) as Promise<IExplainedQuery>
  }

  saveSchemas = (
    schemas: string[] | 'all',
    options?: ISaveSchemasOptions,
  ): Promise<Blob> => {
    const params = new URLSearchParams()
    if (schemas !== 'all') {
      schemas.forEach((name) => {
        params.append('schema', name)
      })
    }
    if (options?.skipPreloaded) {
      params.append('skip_preloaded', 'true')
    }
    return this.fetchGetFileApi(`layouts?${params}`, 'application/zip')
  }

  restoreSchemas = async (
    data: Blob,
    options?: IRestoreSchemasOptions,
  ): Promise<void> => {
    const params = new URLSearchParams()
    if (options?.dropOthers) {
      params.append('drop_others', 'true')
    }
    if (options?.forceAllowBroken) {
      params.append('force_allow_broken', 'true')
    }
    await this.fetchSendFileApi(
      `layouts?${params}`,
      'PUT',
      'application/zip',
      data,
    )
  }

  getPermissions = (): Promise<IPermissionsInfo> => {
    return this.fetchJsonApi('permissions', 'GET') as Promise<IPermissionsInfo>
  }
}
