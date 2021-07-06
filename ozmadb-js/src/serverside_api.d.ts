import {
  IEntityRef, IRoleRef, IEntity, UserViewSource, IViewExprResult, IViewInfoResult, IQueryChunk, ILayout, UserViewName,
  IFieldRef, IDomainValuesResult,
} from "./common";

export declare const renderFunQLName: (name: string) => string;
export declare const renderFunQLValue: (value: unknown) => string;

export interface IFunDBAPI {
  getUserView: (source: UserViewSource, args?: Record<string, unknown>, chunk?: IQueryChunk) => Promise<IViewExprResult>;
  getUserViewInfo: (source: UserViewSource) => Promise<IViewInfoResult>;
  getEntityInfo: (ref: IEntityRef) => Promise<IEntity>;
  insertEntity: (ref: IEntityRef, args: Record<string, unknown>) => Promise<number | undefined>;
  updateEntity: (ref: IEntityRef, id: number, args: Record<string, unknown>) => Promise<void>;
  deleteEntity: (ref: IEntityRef, id: number) => Promise<void>;
  deferConstraints: <T>(inner: () => Promise<T>) => Promise<T>;
  pretendRole: <T>(ref: "root" | IRoleRef, inner: () => Promise<T>) => Promise<T>;
  getDomainValues: (ref: IFieldRef, chunk?: IQueryChunk) => Promise<IDomainValuesResult>;
  writeEvent: (message: string) => void;
  writeEventSync: (message: string) => void;
}

export declare const FunDB: IFunDBAPI;

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

export type UserViewGenerator = (layout: ILayout) => Promise<Record<UserViewName, string>>;
