module OzmaDB.Objects.Types

[<NoEquality; NoComparison>]
type BrokenInfo = { Error: exn; AllowBroken: bool }

type PossiblyBroken<'a> = Result<'a, BrokenInfo>
