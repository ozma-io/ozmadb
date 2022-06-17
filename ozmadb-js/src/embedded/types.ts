import { AttributesMap } from "../types";

export const apiVersion = 1;

export interface IRequest<Request> {
  type: "request";
  id?: unknown;
  request: Request;
}

export interface IResponseCommon {
  type: "response";
  id: unknown;
}

export interface IResponseSuccess<Result> extends IResponseCommon {
  status: "ok";
  result: Result;
}

export interface IResponseError<Error> extends IResponseCommon {
  status: "error";
  error: Error;
  message: string;
}

export type Response<Result, Error> = IResponseSuccess<Result> | IResponseError<Error>;

export type CommonError = "internal" | "badRequest";

export interface IReadyRequestData {
  type: "ready";
  version: number;
}

export type ReadyRequest = IRequest<IReadyRequestData>;
export type ReadyResponse = Response<undefined, CommonError>;

export interface IChangeHeightRequestData {
  type: "changeHeight";
  height: number;
}

export type ChangeHeightRequest = IRequest<IChangeHeightRequestData>;
export type ChangeHeightResponse = Response<undefined, CommonError>;

export interface IUpdateValueRequestData {
  type: "updateValue";
  rawValue: unknown;
}

export type UpdateValueRequest = IRequest<IUpdateValueRequestData>;
export type UpdateValueResponse = Response<undefined, CommonError>;

export interface ICurrentValue {
  rawValue: unknown;
  value: unknown;
  pun?: string;
  attributes: AttributesMap;
}

export interface IUpdateValuePush {
  type: "updateValue";
  update: ICurrentValue;
}

export type PageClientMessage = ReadyRequest;
export type PageServerMessage = ReadyResponse;

export type ControlClientMessage = PageClientMessage | ChangeHeightRequest | UpdateValueRequest;
export type ControlServerMessage = PageServerMessage | ChangeHeightResponse | UpdateValueResponse | IUpdateValuePush;

export type AnyClientMessage = PageClientMessage | ControlClientMessage;
export type AnyServerMessage = PageServerMessage | ControlServerMessage;
