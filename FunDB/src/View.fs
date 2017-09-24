namespace FunWithFlags.FunDB.View

open System.Linq
open Microsoft.FSharp.Text.Lexing

open FunWithFlags.FunCore
open FunWithFlags.FunDB.FunQL

type ViewRow =
    { Cells : string[];
      Attributes : AttributeMap;
    }

type ViewColumn =
    { Name : string;
      Attributes : AttributeMap;
    }

type ViewResult =
    { View : UserView;
      Columns : ViewColumn[];
      Rows : ViewRow list;
    }

exception UserViewError of string

type ViewResolver() =
    let parseQuery dbQuery queryString =
        let qualifier = new Qualifier.Qualifier(dbQuery)
        let lexbuf = LexBuffer<char>.FromString queryString
        let parsedQuery =
            try
                Parser.start Lexer.tokenstream lexbuf
            with
                | Failure(msg) -> raise (UserViewError msg)
        try
            qualifier.Qualify parsedQuery
        with
            | Qualifier.QualifierError(msg) -> raise (UserViewError msg)

    let toViewColumn (res, attrs) =
        { Name = Qualifier.resultName res;
          Attributes = attrs;
        }

    let toViewRow row =
        { Cells = row;
          Attributes = Map.empty;
        }

    member this.QueryById (dbQuery : DatabaseHandle) (uvId: int) : ViewResult =
        let uv = dbQuery.Database.UserViews.FirstOrDefault(fun u -> u.Id = uvId)
        if uv = null then
            raise (UserViewError (sprintf "User view not found: %i" uvId))
        else
            this.Query dbQuery uv

    member this.Query (dbQuery : DatabaseHandle) (uv: UserView) : ViewResult =
        let queryTree = parseQuery dbQuery uv.Query
        let columns = queryTree.results |> List.map toViewColumn |> Seq.toArray
        let results = dbQuery.Query (Compiler.compileQuery queryTree)

        { View = uv;
          Columns = columns;
          Rows = List.map toViewRow results;
        }
