module FunWithFlags.FunUtils.IO

open System
open System.IO

// https://www.meziantou.net/prevent-zip-bombs-in-dotnet.htm
type MaxLengthStream (stream : Stream, maxLength : int64) =
    inherit Stream()

    let mutable length = 0L

    member this.BytesRead = length

    override this.CanRead = stream.CanRead
    override this.CanSeek = false
    override this.CanWrite = false
    override this.Length = stream.Length
    override this.Position
        with get () = stream.Position
        and set _ = raise <| NotSupportedException()

    override this.Read (buffer, offset, count) =
        let result = stream.Read(buffer, offset, count)
        length <- length + int64 result
        if length > maxLength then
            raise <| IOException("File is too large")
        result

    override this.Flush () = raise <| NotSupportedException()
    override this.Seek (offset, origin) = raise <| NotSupportedException()
    override this.SetLength (value) = raise <| NotSupportedException()
    override this.Write (buffer, offset, count) = raise <| NotSupportedException()
    override this.Dispose (disposing) =
        stream.Dispose()
        base.Dispose(disposing)
