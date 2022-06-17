import * as Index from "./index";
import { IHrefLinkOpts, Link } from "./types";

/* eslint-disable vars-on-top, no-var */

declare global {
  var FunApp: typeof Index.FunApp;
  var FunAppEmbeddedError: typeof Index.FunAppEmbeddedError;

  function hrefClick(opts?: IHrefLinkOpts | Link): (event: MouseEvent) => void;
  function hrefClick(event: MouseEvent): void;
}

window.FunApp = Index.FunApp;
window.FunAppEmbeddedError = Index.FunAppEmbeddedError;
// eslint-disable-next-line @typescript-eslint/no-unsafe-argument
window.hrefClick = arg => Index.FunApp.hrefClick(arg as any);
