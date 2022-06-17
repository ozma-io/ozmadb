import {
  AnyServerMessage, CommonError, IChangeHeightRequestData, ICurrentValue, IReadyRequestData,
  IRequest, IUpdateValueRequestData, Response, apiVersion, Link, IGotoRequestData, IHrefLinkOpts,
} from "./types";
import { redirectClick } from "./utils";

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
  private handler = (msg: MessageEvent<AnyServerMessage>) => this.eventHandler(msg);

  constructor() {
    if (window.parent === null) {
      throw new Error("Client is expected to be run in <iframe>");
    }

    this.parent = window.parent;
    window.addEventListener("message", this.handler);
    this.initialized = (async () => {
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

  async goto(link: Link) {
    await this.initialized;
    const gotoMessage: IGotoRequestData = {
      type: "goto",
      link,
    };
    await this.sendRequest(gotoMessage);
  }

  hrefClick(opts?: IHrefLinkOpts | Link): (event: MouseEvent) => void;
  hrefClick(event: MouseEvent): void;

  hrefClick(arg?: IHrefLinkOpts | Link | MouseEvent): ((event: MouseEvent) => void) | void {
    let opts: IHrefLinkOpts | Link | undefined;
    const handler = (event: MouseEvent) => {
      // If a link is already given, follow it.
      let link: Link | undefined;
      if (typeof opts === "string" || (typeof opts === "object" && ("href" in opts || "name" in opts || "ref" in opts))) {
        link = opts;
      } else if (event.currentTarget) {
        // Try to autodetect the link.
        const el = event.currentTarget as Element;
        const href = el.getAttribute?.("href");
        if (href) {
          link = href;
        }
      }
      if (!link) {
        console.error("Couldn't autodetect link to follow");
        return;
      }

      event.stopPropagation();
      if (!redirectClick(event, true)) {
        return;
      }
      void this.goto(link);
    };

    if (arg instanceof MouseEvent) {
      return handler(arg);
    } else {
      opts = arg;
      return handler;
    }
  }
}
