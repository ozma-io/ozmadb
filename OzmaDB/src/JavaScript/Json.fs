module OzmaDB.JavaScript.Json

open System
open System.Globalization
open System.Collections.Generic
open Microsoft.ClearScript
open Microsoft.ClearScript.JavaScript
open Microsoft.ClearScript.V8
open Newtonsoft.Json

open OzmaDB.OzmaUtils

type private PendingObject =
    { Object : IDictionary<string, obj>
      mutable PropertyName : string option
    }

type private PendingArray =
    { Array : IList<obj>
    }

let private unixEpochStartTicks = DateTimeOffset.FromUnixTimeSeconds(0).Ticks

type V8JsonEngine (engine : V8ScriptEngine) =
    let arrayConstructor = engine.Global.["Array"] :?> IJavaScriptObject
    let objectConstructor = engine.Global.["Object"] :?> IJavaScriptObject
    let dateConstructor = engine.Global.["Date"] :?> IJavaScriptObject

    member this.CreateArray () =
        arrayConstructor.Invoke(true) :?> IList<obj>

    member this.CreateObject () =
        objectConstructor.Invoke(true) :?> IDictionary<string, obj>

    member this.CreateDate (date: DateTimeOffset) =
        let epochTicks = date.Ticks - unixEpochStartTicks
        dateConstructor.Invoke(true, epochTicks / TimeSpan.TicksPerMillisecond)

    member this.Serialize (value : obj, ?serializer: JsonSerializer) =
        V8JsonWriter.Serialize(this, value, ?serializer=serializer)

and V8JsonWriter (engine : V8JsonEngine) =
    inherit JsonWriter()

    let stack = Stack<obj>()

    let pushValue (value : obj) =
        if stack.Count = 0 then
            stack.Push value
        else
            match stack.Peek() with
            | :? PendingArray as array ->
                array.Array.Add(value)
            | :? PendingObject as obj ->
                match obj.PropertyName with
                | None -> raisef JsonWriterException "Property name expected before a value"
                | Some propName ->
                    obj.Object.[propName] <- value
                    obj.PropertyName <- None
            | top -> raisef JsonWriterException "Invalid state: %O" top

    do
        base.AutoCompleteOnClose <- false
        base.CloseOutput <- false

    static member Serialize (engine : V8JsonEngine, value : obj, ?serializer: JsonSerializer) =
        use writer = new V8JsonWriter(engine)
        let serializer = serializer |> Option.defaultValue (JsonSerializer.CreateDefault())
        serializer.Serialize(writer, value)
        Option.get writer.Result

    member this.Result =
        if stack.Count = 1 then
            match stack.Peek() with
            | :? PendingArray -> None
            | :? PendingObject -> None
            | v -> Some v
        else
            None

    override this.Flush () = ()

    override this.WriteEndArray () =
        base.WriteEndArray()
        match stack.TryPeek() with
        | (true, (:? PendingArray as array)) ->
            stack.Pop() |> ignore
            pushValue array.Array
        | _ -> raisef JsonWriterException "Expected array start before array end"

    override this.WriteEndObject () =
        base.WriteEndObject()
        match stack.TryPeek() with
        | (true, (:? PendingObject as obj)) ->
            match obj.PropertyName with
            | Some propName -> raisef JsonWriterException "Expected value for property \"%s\" before object end" propName
            | None ->
                stack.Pop() |> ignore
                pushValue obj.Object
        | _ -> raisef JsonWriterException "Expected object start before object end"

    override this.WriteComment (text) = raisef JsonWriterException "Cannot write comment"
    override this.WriteStartConstructor (name) = raisef JsonWriterException "Cannot write constructor"
    override this.WriteEndConstructor () = raisef JsonWriterException "Cannot write constructor"
    override this.WriteRaw (json) = raisef JsonWriterException "Cannot write raw JSON"
    override this.WriteRawValue (json) = raisef JsonWriterException "Cannot write raw JSON"

    override this.WriteStartArray () =
        base.WriteStartArray()
        let arr =
            { Array = engine.CreateArray()
            } : PendingArray
        stack.Push(arr)

    override this.WriteStartObject () =
        base.WriteStartObject()
        let obj =
            { Object = engine.CreateObject()
              PropertyName = None
            } : PendingObject
        stack.Push(obj)

    override this.WritePropertyName (name: string) =
        base.WritePropertyName(name)
        match stack.TryPeek() with
        | (true, (:? PendingObject as obj)) ->
            match obj.PropertyName with
            | Some propName -> raisef JsonWriterException "Expected value for property \"%s\" before property name" propName
            | None -> obj.PropertyName <- Some name
        | (true, _) -> raisef JsonWriterException "Expected object start before property name"
        | (false, _) ->  raisef JsonWriterException "Expected object start before property name"

    override this.WriteNull () =
        base.WriteNull()
        pushValue null

    override this.WriteUndefined () =
        base.WriteUndefined()
        pushValue Undefined.Value

    override this.WriteValue (value : string) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : int) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : uint32) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : int64) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : uint64) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : float32) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : double) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : bool) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : int16) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : uint16) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : char) =
        base.WriteValue (value)
        pushValue (value.ToString(CultureInfo.InvariantCulture))

    override this.WriteValue (value : byte) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : sbyte) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (value : decimal) =
        base.WriteValue (value)
        pushValue value

    override this.WriteValue (date : DateTime) =
        base.WriteValue (date)
        pushValue (engine.CreateDate date)

    override this.WriteValue (date : DateTimeOffset) =
        base.WriteValue (date)
        pushValue (engine.CreateDate date)

    override this.WriteValue (value : byte[]) : unit = raisef JsonWriterException "Cannot write byte array"

    override this.WriteValue (value : Guid) =
        base.WriteValue (value)
        pushValue <| string value

    override this.WriteValue (value : TimeSpan) : unit = raisef JsonWriterException "Cannot write time span"

    override this.WriteValue (value : Uri) =
        base.WriteValue (value)
        pushValue (if isNull value then null else string value)

type V8JsonReader (rootValue : obj) =
    inherit JsonReader()

    let stack = Stack<obj>()

    do
        stack.Push(rootValue)

    static member Deserialize<'T>(value : obj, ?serializer: JsonSerializer) =
        use reader = new V8JsonReader(value)
        let serializer = defaultArg serializer (JsonSerializer.CreateDefault())
        serializer.Deserialize<'T>(reader)

    member private this.Unpack(value : obj) =
        match value with
        | :? char as c ->
            this.SetToken(JsonToken.String, string c)
        | :? byte as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? int16 as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? uint16 as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? int32 as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? uint32 as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? int64 as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? uint64 as i ->
            this.SetToken(JsonToken.Integer, box i)
        | :? single as number ->
            let integer = int number
            if number = single integer then
                this.SetToken(JsonToken.Integer, box integer)
            else
                this.SetToken(JsonToken.Float, box number)
        | :? double as number ->
            let integer = int64 number
            if number = double integer then
                this.SetToken(JsonToken.Integer, box integer)
            else
                this.SetToken(JsonToken.Float, box number)
        | :? decimal as number ->
            let integer = Int128.op_Explicit number
            if number = Int128.op_Explicit integer then
                this.SetToken(JsonToken.Integer, box integer)
            else
                this.SetToken(JsonToken.Float, box number)
        | :? bool as boolVal ->
            this.SetToken(JsonToken.Boolean, box boolVal)
        | :? string as str ->
            this.SetToken(JsonToken.String, str)
        | null ->
            this.SetToken(JsonToken.Null)
        | :? Undefined ->
            this.SetToken(JsonToken.Undefined)
        | :? IList<obj> as array ->
            stack.Push(array.GetEnumerator())
            this.SetToken(JsonToken.StartArray)
        | :? DateTime as date ->
            this.SetToken(JsonToken.Date, box date)
        | :? IDictionary<string, obj> as obj ->
            stack.Push(obj.GetEnumerator())
            this.SetToken(JsonToken.StartObject)
        | _ ->
            raisef JsonReaderException "Unsupported value data type: %O" (value.GetType())

    override this.Read() =
        match stack.TryPeek() with
        | (true, (:? IEnumerator<obj> as arrayI)) when arrayI.MoveNext() ->
            let curValue =
                // Replace `undefined` with nulls.
                match arrayI.Current with
                | :? Undefined -> null
                | v -> v
            this.Unpack(curValue)
            true
        | (true, (:? IEnumerator<obj> as arrayI)) ->
            ignore <| stack.Pop()
            this.SetToken(JsonToken.EndArray)
            true
        | (true, (:? IEnumerator<KeyValuePair<string, obj>> as objectI)) when objectI.MoveNext() ->
            match objectI.Current.Value with
            | :? Undefined ->
                // Skip undefined values in objects.
                this.Read()
            | obj ->
                this.SetToken(JsonToken.PropertyName, box objectI.Current.Key)
                stack.Push(obj)
                true
        | (true, (:? IEnumerator<KeyValuePair<string, obj>> as objectI)) ->
            ignore <| stack.Pop()
            this.SetToken(JsonToken.EndObject)
            true
        | (true, value) ->
            ignore <| stack.Pop()
            this.Unpack(value)
            true
        | _ -> false