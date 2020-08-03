module FunWithFlags.FunDB.JavaScript.Runtime

open NetJs

open FunWithFlags.FunUtils.Utils

type CachingIsolate =
    { isolate : Isolate
      cache : ObjectMap
    }