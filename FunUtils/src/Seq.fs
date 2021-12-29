[<RequireQualifiedAccess>]
module FunWithFlags.FunUtils.Seq

open System
open System.Linq
open System.Collections.Generic
open System.Threading.Tasks
open FSharp.Control.Tasks.NonAffine

let mapMaybe (f : 'a -> 'b option) (s : seq<'a>) : seq<'b> =
    seq {
        for i in s do
            match f i with
            | Some r -> yield r
            | None -> ()
    }

let map2Maybe (f : 'a -> 'b -> 'c option) (s1 : seq<'a>) (s2 : seq<'b>) : seq<'c> =
    seq {
        for (i1, i2) in Seq.zip s1 s2 do
            match f i1 i2 with
            | Some r -> yield r
            | None -> ()
    }

let map3Maybe (f : 'a -> 'b -> 'c -> 'd option) (s1 : seq<'a>) (s2 : seq<'b>) (s3 : seq<'c>) : seq<'d> =
    seq {
        for (i1, i2, i3) in Seq.zip3 s1 s2 s3 do
            match f i1 i2 i3 with
            | Some r -> yield r
            | None -> ()
    }

let catMaybes (s : seq<'a option>) : seq<'a> =
    seq {
        for i in s do
            match i with
            | Some r -> yield r
            | None -> ()
    }

let mapiMaybe (f : int -> 'a -> 'b option) (s : seq<'a>) : seq<'b> =
    seq {
        let mutable n = 0
        for i in s do
            match f n i with
            | Some r -> yield r
            | None -> ()
            n <- n + 1
    }

let mapi2Maybe (f : int -> 'a -> 'b -> 'c option) (s1 : seq<'a>) (s2 : seq<'b>) : seq<'c> =
    seq {
        let mutable n = 0
        for (i1, i2) in Seq.zip s1 s2 do
            match f n i1 i2 with
            | Some r -> yield r
            | None -> ()
            n <- n + 1
    }

let mapi3Maybe (f : int -> 'a -> 'b -> 'c -> 'd option) (s1 : seq<'a>) (s2 : seq<'b>) (s3 : seq<'c>) : seq<'d> =
    seq {
        let mutable n = 0
        for (i1, i2, i3) in Seq.zip3 s1 s2 s3 do
            match f n i1 i2 i3 with
            | Some r -> yield r
            | None -> ()
            n <- n + 1
    }

let filteri (f : int -> 'a -> bool) (s : seq<'a>) : seq<'a> =
    seq {
        let mutable n = 0
        for i in s do
            if f n i then
                yield i
            n <- n + 1
    }

let first (s : seq<'a>) : 'a option = Seq.tryFind (fun x -> true) s

let fold1 (func : 'a -> 'a -> 'a) (s : seq<'a>) : 'a =
    Seq.fold func (Seq.head s) (Seq.tail s)

let areEqual (a : seq<'a>) (b : seq<'a>) : bool =
    Seq.compareWith (fun e1 e2 -> if e1 = e2 then 0 else -1) a b = 0

let iterTask (f : 'a -> Task) (s : seq<'a>) : Task =
    unitTask {
        for a in s do
            do! f a
    }

let mapTask (f : 'a -> Task<'b>) (s : seq<'a>) : Task<seq<'b>> =
    task {
        let list = List<'b>()
        for a in s do
            let! b = f a
            list.Add(b)
        return list :> seq<'b>
    }

let collectTask (f : 'a -> Task<seq<'b>>) (s : seq<'a>) : Task<seq<'b>> =
    task {
        let list = List<'b>()
        for a in s do
            let! bs = f a
            list.AddRange(bs)
        return list :> seq<'b>
    }

let mapTake (f : 'a -> 'b option) (s : seq<'a>) : seq<'b> =
    let i = s.GetEnumerator()
    let rec iter () =
        seq {
            if i.MoveNext() then
                match f i.Current with
                | None -> ()
                | Some r ->
                    yield r
                    yield! iter ()
        }
    iter ()

let iterStop (f : 'a -> bool) (s : seq<'a>) : unit =
    let i = s.GetEnumerator()
    let mutable cont = true
    while cont && i.MoveNext() do
        cont <- f i.Current

let iterTaskStop (f : 'a -> Task<bool>) (s : seq<'a>) : Task<unit> =
    task {
        let i = s.GetEnumerator()
        let mutable cont = true
        while cont && i.MoveNext() do
            let! c = f i.Current
            cont <- c
    }

let foldOption (func : 'acc -> 'a -> 'acc option) (init : 'acc) (vals : seq<'a>) : 'acc option =
    let mutable acc = init
    let mutable failed = false
    let tryOne a =
        match func acc a with
        | Some newAcc ->
            acc <- newAcc
            true
        | None ->
            failed <- true
            false
    iterStop tryOne vals
    if failed then
        None
    else
        Some acc

let foldResult (func : 'acc -> 'a -> Result<'acc, 'e>) (init : 'acc) (vals : seq<'a>) : Result<'acc, 'e> =
    let mutable acc = init
    let mutable error = None
    let tryOne a =
        match func acc a with
        | Ok newAcc ->
            acc <- newAcc
            true
        | Error e ->
            error <- Some e
            false
    iterStop tryOne vals
    match error with
    | None -> Ok acc
    | Some e -> Error e

let foldTask (func : 'acc -> 'a -> Task<'acc>) (init : 'acc) (vals : seq<'a>) : Task<'acc> =
    task {
        let mutable acc = init
        for a in vals do
            let! newAcc = func acc a
            acc <- newAcc
        return acc
    }

let foldOptionTask (func : 'acc -> 'a -> Task<'acc option>) (init : 'acc) (vals : seq<'a>) : Task<'acc option> =
    task {
        let mutable acc = init
        let mutable failed = false
        let tryOne a =
            task {
                match! func acc a with
                | Some newAcc ->
                    acc <- newAcc
                    return true
                | None ->
                    failed <- true
                    return false
            }
        do! iterTaskStop tryOne vals
        if failed then
            return None
        else
            return Some acc
    }

let foldResultTask (func : 'acc -> 'a -> Task<Result<'acc, 'e>>) (init : 'acc) (vals : seq<'a>) : Task<Result<'acc, 'e>> =
    task {
        let mutable acc = init
        let mutable error = None
        let tryOne a =
            task {
                match! func acc a with
                | Ok newAcc ->
                    acc <- newAcc
                    return true
                | Error e ->
                    error <- Some e
                    return false
            }
        do! iterTaskStop tryOne vals
        match error with
        | None -> return Ok acc
        | Some e -> return Error e
    }

let traverseOption (func : 'a -> 'b option) (vals : seq<'a>) : seq<'b> option =
    let list = List<'b>()
    let mutable failed = false
    let tryOne a =
        match func a with
        | Some b ->
            list.Add(b)
            true
        | None ->
            failed <- true
            false
    iterStop tryOne vals
    if failed then
        None
    else
        Some (list :> seq<'b>)

let traverseResult (func : 'a -> Result<'b, 'e>) (vals : seq<'a>) : Result<seq<'b>, 'e> =
    let list = List<'b>()
    let mutable error = None
    let tryOne a =
        match func a with
        | Ok b ->
            list.Add(b)
            true
        | Error e ->
            error <- Some e
            false
    iterStop tryOne vals
    match error with
    | None -> Ok (list :> seq<'b>)
    | Some e -> Error e

let traverseOptionTask (func : 'a -> Task<'b option>) (vals : seq<'a>) : Task<seq<'b> option> =
    task {
        let list = List<'b>()
        let mutable failed = false
        let tryOne a = task {
            match! func a with
            | Some b ->
                list.Add(b)
                return true
            | None ->
                failed <- true
                return false
        }
        do! iterTaskStop tryOne vals
        if failed then
            return None
        else
            return Some (list :> seq<'b>)
    }

let traverseResultTask (func : 'a -> Task<Result<'b, 'e>>) (vals : seq<'a>) : Task<Result<seq<'b>, 'e>> =
    task {
        let list = List<'b>()
        let mutable error = None
        let tryOne a = task {
            match! func a with
            | Ok b ->
                list.Add(b)
                return true
            | Error e ->
                error <- Some e
                return false
        }
        do! iterTaskStop tryOne vals
        match error with
        | None -> return Ok (list :> seq<'b>)
        | Some e -> return Error e
    }

let hash (s : seq<'a>) : int = Seq.fold (fun a b -> HashCode.Combine(a, b)) 0 s

let ofEnumerator (i : IEnumerator<'a>) : seq<'a> =
    seq {
        while i.MoveNext() do
            yield i.Current
    }

let mergeSortedBy (keyFunc : 'a -> 'k) (seq1 : seq<'a>) (seq2 : seq<'a>) : seq<'a> =
    seq {
        let i1 = seq1.GetEnumerator()
        if not (i1.MoveNext()) then
            yield! seq2
        else
            let i2 = seq2.GetEnumerator()
            if not (i2.MoveNext()) then
                yield i1.Current
                yield! ofEnumerator i1
            else
                let mutable key1 = keyFunc i1.Current
                let mutable key2 = keyFunc i2.Current
                let mutable next = true
                while next do
                    if key1 < key2 then
                        yield i1.Current
                        if not (i1.MoveNext()) then
                            yield i2.Current
                            yield! ofEnumerator i2
                            next <- false
                        else
                            key1 <- keyFunc i1.Current
                    else
                        yield i2.Current
                        if not (i2.MoveNext()) then
                            yield i1.Current
                            yield! ofEnumerator i1
                            next <- false
                        else
                            key2 <- keyFunc i2.Current
    }

let mergeSorted (seq1 : seq<'a>) (seq2 : seq<'a>) : seq<'a> = mergeSortedBy id seq1 seq2

let ofObj (a : 'a seq) =
    match a with
    | null -> Seq.empty
    | ra -> ra

let partition (f : 'a -> bool) (vals : seq<'a>) : seq<'a> * seq<'a> =
    let trues = List()
    let falses = List()
    for v in vals do
        if f v then
            trues.Add(v)
        else
            falses.Add(v)
    (trues :> seq<'a>, falses :> seq<'a>)

let skipLast (n : int) (a : 'a seq) = a.SkipLast(n)

let snoc (s : 'a seq) : 'a * 'a seq =
    let i = s.GetEnumerator()
    if i.MoveNext() then
        (i.Current, ofEnumerator i)
    else
        failwith "snoc: empty sequence"

let trySnoc (s : 'a seq) : ('a * 'a seq) option =
    let i = s.GetEnumerator()
    if i.MoveNext() then
        Some (i.Current, ofEnumerator i)
    else
        None

let (|Snoc|_|) (s : 'a seq) : ('a * 'a seq) option = trySnoc s