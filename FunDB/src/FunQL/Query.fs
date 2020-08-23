module FunWithFlags.FunDB.FunQL.Query

open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open FSharp.Control.Tasks.Affine

open FunWithFlags.FunUtils
open FunWithFlags.FunDB.FunQL.AST
open FunWithFlags.FunDB.FunQL.Compile
open FunWithFlags.FunDB.FunQL.Arguments
open FunWithFlags.FunDB.Layout.Types
open FunWithFlags.FunDB.SQL.Query
module SQL = FunWithFlags.FunDB.SQL.AST

type ExecutedAttributeMap = Map<AttributeName, SQL.Value>
type ExecutedAttributeTypes = Map<AttributeName, SQL.SimpleValueType>

type [<NoEquality; NoComparison>] ExecutedValue =
    { Value : SQL.Value
      [<DataMember(EmitDefaultValue = false, Order = 42)>]
      Attributes : ExecutedAttributeMap
      Pun : SQL.Value option
    }

type [<NoEquality; NoComparison>] ExecutedEntityId =
    { Id : int
      [<DataMember(EmitDefaultValue = false)>]
      SubEntity : ResolvedEntityRef option
    }

type [<NoEquality; NoComparison>] ExecutedRow =
    { Values : ExecutedValue array
      DomainId : GlobalDomainId
      [<DataMember(EmitDefaultValue = false)>]
      Attributes : ExecutedAttributeMap
      [<DataMember(EmitDefaultValue = false)>]
      EntityIds : Map<EntityName, ExecutedEntityId>
      MainId : int option
      MainSubEntity : ResolvedEntityRef option
    }

[<NoEquality; NoComparison>]
type ExecutedColumnInfo =
    { Name : FunQLName
      AttributeTypes : ExecutedAttributeTypes
      CellAttributeTypes : ExecutedAttributeTypes
      ValueType : SQL.SimpleValueType
      PunType : SQL.SimpleValueType option
    }

[<NoEquality; NoComparison>]
type ExecutedViewInfo =
    { AttributeTypes : ExecutedAttributeTypes
      RowAttributeTypes : ExecutedAttributeTypes
      Columns : ExecutedColumnInfo array
    }

[<NoEquality; NoComparison>]
type ExecutedViewExpr =
    { Attributes : ExecutedAttributeMap
      ColumnAttributes : ExecutedAttributeMap array
      Rows : ExecutedRow seq
    }

[<NoEquality; NoComparison>]
type private ExecutedAttributes =
    { Attributes : ExecutedAttributeMap
      AttributeTypes : ExecutedAttributeTypes
      ColumnAttributes : Map<FieldName, ExecutedAttributeTypes * ExecutedAttributeMap>
    }

let private parseAttributesResult (columns : ColumnType[]) (result : QueryResult) : ExecutedAttributes =
    let values = Seq.exactlyOne result.Rows

    let takeViewAttribute colType (_, valType) v =
        match colType with
        | CTMeta (CMRowAttribute name) -> Some (name, (valType, v))
        | _ -> None
    let viewAttributes = Seq.map3Maybe takeViewAttribute columns result.Columns values |> Map.ofSeq

    let takeColumnAttribute colType (_, valType) v =
        match colType with
        | CTColumnMeta (fieldName, CCCellAttribute name) -> Some (fieldName, (name, (valType, v)))
        | _ -> None
    let makeColumnAttributes (fieldName : FieldName, values : (FieldName * (AttributeName * (SQL.SimpleValueType * SQL.Value))) seq) =
        let valuesMap = values |> Seq.map snd |> Map.ofSeq
        let attrs = Map.map (fun name -> snd) valuesMap
        let attrTypes = Map.map (fun name -> fst) valuesMap
        (fieldName, (attrTypes, attrs))
    let colAttributes =
        Seq.map3Maybe takeColumnAttribute columns result.Columns values
        |> Seq.groupBy fst
        |> Seq.map makeColumnAttributes
        |> Map.ofSeq

    { Attributes = Map.map (fun name -> snd) viewAttributes
      AttributeTypes = Map.map (fun name -> fst) viewAttributes
      ColumnAttributes = colAttributes
    }

let private parseResult (mainEntity : ResolvedEntityRef option) (domains : Domains) (columns : ColumnType[]) (result : QueryResult) : ExecutedViewInfo * ExecutedRow seq =
    let takeRowAttribute i colType (_, valType) =
        match colType with
        | CTMeta (CMRowAttribute name) -> Some (name, (valType, i))
        | _ -> None
    let rowAttributes = Seq.mapi2Maybe takeRowAttribute columns result.Columns |> Map.ofSeq

    let takeCellAttribute i colType (_, valType) =
        match colType with
            | CTColumnMeta (fieldName, CCCellAttribute name) -> Some (fieldName, (name, (valType, i)))
            | _ -> None
    let allCellAttributes =
        Seq.mapi2Maybe takeCellAttribute columns result.Columns
        |> Seq.groupBy fst
        |> Seq.map (fun (fieldName, attrs) -> (fieldName, attrs |> Seq.map snd |> Map.ofSeq))
        |> Map.ofSeq

    let takePunAttribute i colType (_, valType) =
        match colType with
        | CTColumnMeta (name, CCPun) -> Some (name, (valType, i))
        | _ -> None
    let punAttributes = Seq.mapi2Maybe takePunAttribute columns result.Columns |> Map.ofSeq

    let takeDomainColumn i colType =
        match colType with
        | CTMeta (CMDomain ns) -> Some (ns, i)
        | _ -> None
    let domainColumns = Seq.mapiMaybe takeDomainColumn columns |> Map.ofSeq

    let takeIdColumn i colType =
        match colType with
        | CTMeta (CMId name) -> Some (name, i)
        | _ -> None
    let idColumns = Seq.mapiMaybe takeIdColumn columns |> Map.ofSeq

    let takeSubEntityColumn i colType =
        match colType with
        | CTMeta (CMSubEntity name) -> Some (name, i)
        | _ -> None
    let subEntityColumns = Seq.mapiMaybe takeSubEntityColumn columns |> Map.ofSeq

    let takeMainIdColumn i colType =
        match colType with
        | CTMeta CMMainId -> Some i
        | _ -> None
    let mainIdColumn = Seq.mapiMaybe takeMainIdColumn columns |> Seq.first

    let takeMainSubEntityColumn i colType =
        match colType with
        | CTMeta CMMainSubEntity -> Some i
        | _ -> None
    let mainSubEntityColumn = Seq.mapiMaybe takeMainSubEntityColumn columns |> Seq.first

    let takeColumn i colType (_, valType) =
        match colType with
            | CTColumn name ->
                let cellAttributes =
                    match Map.tryFind name allCellAttributes with
                        | None -> Map.empty
                        | Some a -> a
                let punType = Option.map (fun (valType, i) -> valType) <| Map.tryFind name punAttributes
                let columnInfo =
                    { Name = name
                      AttributeTypes = Map.empty
                      CellAttributeTypes = Map.map (fun name (valType, i) -> valType) cellAttributes
                      ValueType = valType
                      PunType = punType
                    }
                Some (cellAttributes, i, columnInfo)
            | _ -> None
    let columnsMeta = Seq.mapi2Maybe takeColumn columns result.Columns |> Seq.toArray

    let parseRow (values : SQL.Value array) =
        let getCell (cellAttributes, i, column) =
            let attrs = Map.map (fun name (valType, i) -> values.[i]) cellAttributes
            let pun = Map.tryFind column.Name punAttributes |> Option.map (fun (valType, i) -> values.[i])
            let value = values.[i]
            { Attributes = attrs
              Value = value
              Pun = pun
            }

        let getDomainId i =
            match values.[i] with
                | SQL.VInt id -> Some id
                | SQL.VNull -> None
                | _ -> failwith "Domain id is not an integer"

        let getId i =
            match values.[i] with
                | SQL.VInt id -> Some id
                | SQL.VNull -> None
                | _ -> failwith "Entity id is not an integer"

        let getMainId i =
            match values.[i] with
            | SQL.VInt id -> id
            | _ -> failwith "Entity main id is not an integer"

        let getMainSubEntity i =
            match values.[i] with
            | SQL.VString subEntityString ->
                parseTypeName (Option.get mainEntity) subEntityString
            | _ -> failwith "Main subentity is not a string"

        let domainIds = Map.mapMaybe (fun ns i -> getDomainId i) domainColumns

        let rec getGlobalDomainId = function
            | DSingle (id, info) -> (id, info)
            | DMulti (ns, subdoms) ->
                let localId = Map.findOrFailWith (fun () -> sprintf "No domain namespace value: %i" ns) ns domainIds
                let doms = Map.findOrFailWith (fun () -> sprintf "Unknown local domain id for namespace %i: %i" ns localId) localId subdoms
                getGlobalDomainId doms

        let (domainId, domainInfo) = getGlobalDomainId domains

        let getSubEntity name i =
            match values.[i] with
                | SQL.VString subEntityString ->
                    let field = Map.find name domainInfo
                    Some <| parseTypeName field.ref.entity subEntityString
                | SQL.VNull -> None
                | _ -> failwith "Entity subtype is not a string"

        let getEntityId name i =
            match getId i with
            | None -> None
            | Some id ->
                let subEntity = Option.bind (getSubEntity name) (Map.tryFind name subEntityColumns)
                Some
                    { Id = id
                      SubEntity = subEntity
                    }

        let rowAttrs = Map.map (fun name (valType, i) -> values.[i]) rowAttributes
        let values = Array.map getCell columnsMeta
        let entityIds = Map.mapMaybe getEntityId idColumns
        let mainId = Option.map getMainId mainIdColumn
        let mainSubEntity = Option.map getMainSubEntity mainSubEntityColumn

        { Attributes = rowAttrs
          Values = values
          DomainId = domainId
          EntityIds = entityIds
          MainId = mainId
          MainSubEntity = mainSubEntity
        }

    let columns = Array.map (fun (attributes, i, column) -> column) columnsMeta
    let rows = Seq.map parseRow result.Rows

    let viewInfo =
        { Columns = columns
          AttributeTypes = Map.empty
          RowAttributeTypes = Map.map (fun name (valType, i) -> valType) rowAttributes
        }
    (viewInfo, rows)

let runViewExpr (connection : QueryConnection) (viewExpr : CompiledViewExpr) (arguments : ArgumentValues) (cancellationToken : CancellationToken) (resultFunc : ExecutedViewInfo -> ExecutedViewExpr -> Task<'a>) : Task<'a> =
    task {
        let parameters = prepareArguments viewExpr.query.arguments arguments

        let! attrsResult = task {
            match viewExpr.attributesQuery with
            | Some attributesExpr ->
                return! connection.ExecuteQuery attributesExpr.query parameters cancellationToken (parseAttributesResult attributesExpr.columns >> Task.FromResult)
            | None ->
                return
                    { Attributes = Map.empty
                      AttributeTypes = Map.empty
                      ColumnAttributes = Map.empty
                    }
        }

        let getColumnInfo (col : ExecutedColumnInfo) =
            let attributeTypes =
                match Map.tryFind col.Name attrsResult.ColumnAttributes with
                | None -> Map.empty
                | Some (attrTypes, attrs) -> attrTypes
            { col with AttributeTypes = attributeTypes }

        let getColumnAttributes (col : ExecutedColumnInfo) =
            match Map.tryFind col.Name attrsResult.ColumnAttributes with
            | None -> Map.empty
            | Some (attrTypes, attrs) -> attrs

        return! connection.ExecuteQuery (viewExpr.query.expression.ToSQLString()) parameters cancellationToken <| fun rawResult ->
            let (info, rows) = parseResult viewExpr.mainEntity viewExpr.domains viewExpr.columns rawResult

            let mergedInfo =
                { info with
                      AttributeTypes = attrsResult.AttributeTypes
                      Columns = Array.map getColumnInfo info.Columns
                }
            let result =
                { Attributes = attrsResult.Attributes
                  ColumnAttributes = Array.map getColumnAttributes info.Columns
                  Rows = rows
                }
            resultFunc mergedInfo result
    }
