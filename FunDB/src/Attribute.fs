namespace FunWithFlags.FunDB.Attribute

// Basically JSON without nulls.
type AttributeMap = Map<string, Attribute>

and Attribute =
    | ABool of bool
    | AFloat of float
    | AInt of int
    | AString of string
    | AList of Attribute array
    | AAssoc of AttributeMap
with
    member this.GetBool () =
        match this with
            | ABool(b) -> b
            | _ -> invalidOp "GetBool"
    member this.GetFloat () =
        match this with
            | AFloat(f) -> f
            | _ -> invalidOp "GetFloat"
    member this.GetInt () =
        match this with
            | AInt(i) -> i
            | _ -> invalidOp "GetInt"
    member this.GetString () =
        match this with
            | AString(s) -> s
            | _ -> invalidOp "GetString"
    member this.GetList () =
        match this with
            | AList(l) -> l
            | _ -> invalidOp "GetList"
    member this.GetAssoc () =
        match this with
            | AAssoc(a) -> a
            | _ -> invalidOp "GetAssoc"
            
    member this.Item
        with get index = this.GetList().[index]

    member this.Item
        with get key = Map.find key (this.GetAssoc ())

    static member MergeMap (a : AttributeMap) (b : AttributeMap) : AttributeMap =
        let insertOne oldMap k v =
            match Map.tryFind k oldMap with
                | None -> Map.add k v oldMap
                | Some(oldv) -> Map.add k (Attribute.Merge oldv v) oldMap
        Map.fold insertOne a b

    static member Merge (a : Attribute) (b : Attribute) : Attribute =
        match (a, b) with
            | (AList(a), AList(b)) -> AList(Array.append a b)
            | (AAssoc(a), AAssoc(b)) -> AAssoc(Attribute.MergeMap a b)
            | _ -> b
