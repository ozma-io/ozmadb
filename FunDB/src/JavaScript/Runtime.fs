module FunWithFlags.FunDB.JavaScript.Runtime

open NetJs

open FunWithFlags.FunDB.Utils

type CachingIsolate =
    { isolate : Isolate
      cache : ObjectMap
    }