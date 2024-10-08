module OzmaDB.OzmaUtils.IO

open System
open System.IO
open System.Threading.Tasks

let getMemoryStreamMemory (stream: MemoryStream) =
    let length = stream.Seek(0L, SeekOrigin.Current)
    let buffer = stream.GetBuffer()
    ReadOnlyMemory(buffer, 0, int length)

// https://www.meziantou.net/prevent-zip-bombs-in-dotnet.htm
type MaxLengthStream(stream: Stream, maxLength: int64) =
    inherit Stream()

    let mutable length = 0L

    let checkLength () =
        if length > maxLength then
            raise <| IOException("File is too large")

    member this.BytesRead = length

    override this.CanRead = stream.CanRead
    override this.CanSeek = false
    override this.CanWrite = false
    override this.Length = stream.Length

    override this.Position
        with get () = stream.Position
        and set _ = raise <| NotSupportedException()

    override this.Read(buffer, offset, count) =
        let result = stream.Read(buffer, offset, count)
        length <- length + int64 result
        checkLength ()
        result

    override this.Read span =
        let result = stream.Read(span)
        length <- length + int64 result
        checkLength ()
        result

    override this.ReadByte() =
        let result = stream.ReadByte()

        if result >= 0 then
            length <- length + 1L
            checkLength ()

        result

    override this.ReadAsync(buffer, cancellationToken) =
        task {
            let! result = stream.ReadAsync(buffer, cancellationToken)
            length <- length + int64 result
            checkLength ()
            return result
        }
        |> ValueTask<int>

    override this.ReadAsync(buffer, offset, count, cancellationToken) =
        task {
            let! result = stream.ReadAsync(buffer, offset, count, cancellationToken)
            length <- length + int64 result
            checkLength ()
            return result
        }

    override this.Flush() = raise <| NotSupportedException()
    override this.Seek(offset, origin) = raise <| NotSupportedException()
    override this.SetLength(value) = raise <| NotSupportedException()
    override this.Write(buffer, offset, count) = raise <| NotSupportedException()

    override this.Dispose(disposing) =
        stream.Dispose()
        base.Dispose(disposing)
