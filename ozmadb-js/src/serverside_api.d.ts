import * as Api from "./index";

export declare const renderFunQLName: (name: string) => string;
export declare const renderFunQLValue: (value: any) => string;

export interface IFunDBAPI {
  getUserView: (source: Api.UserViewSource, args?: Record<string, any>) => Promise<Api.IViewExprResult>;
  getUserViewInfo: (source: Api.UserViewSource) => Promise<Api.IViewInfoResult>;
  getEntityInfo: (ref: Api.IEntityRef) => Promise<Api.IEntity>;
  insertEntity: (ref: Api.IEntityRef, args: Record<string, any>) => Promise<Api.IInsertEntityResult>;
  updateEntity: (ref: Api.IEntityRef, id: number, args: Record<string, any>) => Promise<Api.IUpdateEntityResult>;
  deleteEntity: (ref: Api.IEntityRef, id: number) => Promise<Api.IDeleteEntityResult>;
  writeEvent: (message: string) => void;
  writeEventSync: (message: string) => void;
}

export declare const FunDB: IFunDBAPI

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
  entity: Api.IEntityRef;
  time: TriggerTime;
  source: TriggerSource;
}

export type BeforeInsertTrigger = (event: ITriggerEvent, args: Record<string, any>) => Promise<Record<string, any> | boolean>;
export type BeforeUpdateTrigger = (event: ITriggerEvent, args: Record<string, any>) => Promise<Record<string, any> | boolean>;
export type BeforeDeleteTrigger = (event: ITriggerEvent) => Promise<boolean>;
export type AfterInsertTrigger = (event: ITriggerEvent, args: Record<string, any>) => Promise<void>;
export type AfterUpdateTrigger = (event: ITriggerEvent, args: Record<string, any>) => Promise<void>;
export type AfterDeleteTrigger = (event: ITriggerEvent) => Promise<void>;

export type UserViewGenerator = (layout: Api.ILayout) => Promise<Record<Api.UserViewName, string>>;