import * as Api from "./index";

export declare const renderSqlName: (name: string) => string;

export interface IFunDBAPI {
  getUserView: (source: Api.UserViewSource, args: Record<string, any>) => Promise<Api.IViewExprResult>;
  getUserViewInfo: (source: Api.UserViewSource) => Promise<Api.IViewInfoResult>;
  getEntityInfo: (ref: Api.IEntityRef) => Promise<Api.IEntity>;
  insertEntity: (ref: Api.IEntityRef, args: Record<string, any>) => Promise<Api.IInsertEntityResult>;
  updateEntity: (ref: Api.IEntityRef, id: number, args: Record<string, any>) => Promise<Api.IUpdateEntityResult>;
  deleteEntity: (ref: Api.IEntityRef, id: number) => Promise<Api.IDeleteEntityResult>;
  writeEvent: (message: string) => void;
}

export declare const FunDB: IFunDBAPI