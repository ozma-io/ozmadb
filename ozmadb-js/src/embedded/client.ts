import {
  AnyServerMessage, CommonError, IChangeHeightRequestData, ICurrentValue, IReadyRequestData,
  IRequest, IUpdateValueRequestData, Response, apiVersion,
} from "./types";

/*
 * Embedded API client.
 */

export class FunAppEmbeddedError extends Error {
  error: CommonError;

  constructor(error: CommonError, message: string) {
    super(message);
    this.error = error;
  }
}

export type ValueHandler = (newValue: ICurrentValue) => void;

export default class FunAppEmbeddedClient {
  private parent: Window;
  private lastId = 0;
  private initialized: Promise<void>;
  private valueHandlers: ValueHandler[] = [];
  private _currentValue: ICurrentValue | undefined;
  private requests: Record<number, (response: Response<any, CommonError>) => void> = {};
  private needUpdateHeight = true;

  constructor() {
    if (window.parent === null) {
      throw new Error("Client is expected to be run in <iframe>");
    }

    this.parent = window.parent;
    this.initialized = (async () => {
      /* eslint-disable @typescript-eslint/unbound-method */
      window.addEventListener("message", this.eventHandler);
      /* eslint-enable @typescript-eslint/unbound-method */

      const readyMessage: IReadyRequestData = {
        type: "ready",
        version: apiVersion,
      };
      await this.sendRequest(readyMessage);
    })();
    void this.initialized.then(() => {
      if (this.needUpdateHeight) {
        return this.setHeight();
      }
      return undefined;
    });
  }

  private async sendRequest<R extends { type: string }>(request: R): Promise<any> {
    return new Promise((resolve, reject) => {
      const id = this.lastId++;
      const msg: IRequest<R> = {
        type: "request",
        id,
        request,
      };
      this.requests[id] = (response: Response<any, CommonError>) => {
        if (response.status === "ok") {
          resolve(response.result);
        } else {
          reject(new FunAppEmbeddedError(response.error, response.message));
        }
      };
      this.parent.postMessage(msg, "*");
    });
  }

  private eventHandler(event: MessageEvent<AnyServerMessage>) {
    if (event.source !== this.parent) return;
    const msg = event.data;

    if (msg.type === "response") {
      const request = this.requests[msg.id as number];
      if (request === undefined) {
        throw new Error(`Response received for an unknown request, id ${msg.id}`);
      }
      delete this.requests[msg.id as number];
      request(msg);
    } else if (msg.type === "updateValue") {
      this._currentValue = msg.update;
      for (const handler of this.valueHandlers) {
        try {
          handler(msg.update);
        } catch (e) {
          console.error(`Value handler failed with error: ${e}`);
        }
      }
    } else {
      throw new Error(`Unknown incoming message type: ${(msg as any).type}`);
    }
  }

  get currentValue() {
    return this._currentValue;
  }

  addValueHandler(handler: ValueHandler) {
    this.valueHandlers.push(handler);
  }

  removeValueHandler(handler: ValueHandler) {
    const idx = this.valueHandlers.lastIndexOf(handler);
    if (idx >= 0) {
      this.valueHandlers.splice(idx, 1);
    }
  }

  async setHeight(height?: number) {
    this.needUpdateHeight = false;
    await this.initialized;
    const newHeight = height ?? document.body.clientHeight;

    const heightMessage: IChangeHeightRequestData = {
      type: "changeHeight",
      height: newHeight,
    };
    await this.sendRequest(heightMessage);
  }

  async updateValue(rawValue: unknown) {
    await this.initialized;
    const updateMessage: IUpdateValueRequestData = {
      type: "updateValue",
      rawValue,
    };
    await this.sendRequest(updateMessage);
  }
}
