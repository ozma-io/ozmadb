import * as Index from "./index";

/* eslint-disable vars-on-top, no-var */

declare global {
  var FunApp: typeof Index.FunApp;
  var FunAppEmbeddedError: typeof Index.FunAppEmbeddedError;
}

window.FunApp = Index.FunApp;
window.FunAppEmbeddedError = Index.FunAppEmbeddedError;
