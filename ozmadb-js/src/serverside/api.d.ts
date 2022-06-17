import {
  IEntityRef, IRoleRef, IEntity, UserViewSource, IViewExprResult, IViewInfoResult, IQueryChunk, ILayout, UserViewName,
  IFieldRef, IDomainValuesResult, IApiError, RowId, RowKey, IReferencesTree,
} from "../types";

export declare const renderDate: (date : Date) => string;
export declare const renderFunQLName: (name: string) => string;
export declare const renderFunQLValue: (value: unknown) => string;

export interface IFunDBAPI {
  getUserView: (source: UserViewSource, args?: Record<string, unknown>, chunk?: IQueryChunk) => Promise<IViewExprResult>;
  getUserViewInfo: (source: UserViewSource) => Promise<IViewInfoResult>;
  getEntityInfo: (ref: IEntityRef) => Promise<IEntity>;
  insertEntity: (ref: IEntityRef, args: Record<string, unknown>) => Promise<RowId | null>;
  insertEntities: (ref: IEntityRef, args: Record<string, unknown>[]) => Promise<(RowId | null)[]>;
  updateEntity: (ref: IEntityRef, id: RowKey, args: Record<string, unknown>) => Promise<RowId>;
  deleteEntity: (ref: IEntityRef, id: RowKey) => Promise<void>;
  getRelatedEntities: (ref: IEntityRef, id: RowKey) => Promise<IReferencesTree>;
  recursiveDeleteEntity: (ref: IEntityRef, id: RowKey) => Promise<IReferencesTree>;
  runCommand: (commandString: string, args?: Record<string, unknown>) => Promise<void>;
  deferConstraints: <T>(inner: () => Promise<T>) => Promise<T>;
  pretendRole: <T>(ref: "root" | IRoleRef, inner: () => Promise<T>) => Promise<T>;
  getDomainValues: (ref: IFieldRef, chunk?: IQueryChunk) => Promise<IDomainValuesResult>;
  writeEvent: (message: string) => void;
  writeEventSync: (message: string) => Promise<void>;
}

export declare const FunDB: IFunDBAPI;

export class FunDBError extends Error {
  constructor(body: IApiError);

  body: IApiError;
}

export interface ITriggerInsertSource {
  type: "insert";
  newId: number | null;
}

export interface ITriggerUpdateSource {
  type: "update";
  id: number;
}

export interface ITriggerDeleteSource {
  type: "delete";
  id: number | null;
}

export type TriggerSource = ITriggerInsertSource | ITriggerUpdateSource | ITriggerDeleteSource;

export type TriggerTime = "before" | "after";

export interface ITriggerEvent {
  entity: IEntityRef;
  time: TriggerTime;
  source: TriggerSource;
}

export type BeforeInsertTrigger = (event: ITriggerEvent, args: Record<string, unknown>) => Promise<Record<string, unknown> | boolean>;
export type BeforeUpdateTrigger = (event: ITriggerEvent, args: Record<string, unknown>) => Promise<Record<string, unknown> | boolean>;
export type BeforeDeleteTrigger = (event: ITriggerEvent) => Promise<boolean>;
export type AfterInsertTrigger = (event: ITriggerEvent, args: Record<string, unknown>) => Promise<void>;
export type AfterUpdateTrigger = (event: ITriggerEvent, args: Record<string, unknown>) => Promise<void>;
export type AfterDeleteTrigger = (event: ITriggerEvent) => Promise<void>;

export type Action = (args: Record<string, unknown>) => Promise<unknown>;

export type UserViewGenerator = (layout: ILayout) => Promise<Record<UserViewName, string>>;
