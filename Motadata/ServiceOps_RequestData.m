let
    // ===== PARAMETERS =====
    ServerURL = pServerURL,       // e.g., https://demo.motadataserviceops.com
    APIKey    = pApiKey,

    // ===== SETTINGS =====
    PageSize = 2000,
    MaxPages = null,                  // set to null to use totalCount

    // ===== AUTH =====
    AuthHeader = "Apikey " & APIKey,

    // ===== PAGE FETCHER (POST with empty body) =====
    GetRequestsPage = (offset as number, size as number) as record =>
        let
            Response =
                Web.Contents(
                    ServerURL,     // <-- ROOT ONLY (critical for Service + Template App)
                    [
                        RelativePath = "api/v1/request/search/byqual",
                        Query = [
                            offset  = Text.From(offset),
                            size    = Text.From(size),
                            sort_by = "createdTime"
                        ],
                        Headers = [
                            Authorization  = AuthHeader,
                            #"Content-Type" = "application/json",
                            Accept         = "application/json"
                        ],
                        Content = Text.ToBinary("{}")
                    ]
                ),

            JsonResponse = Json.Document(Response),

            // Handle possible shapes
            Data =
                if      Record.HasFields(JsonResponse, {"objectList"}) then JsonResponse[objectList]
                else if Record.HasFields(JsonResponse, {"data"})       then JsonResponse[data]
                else if Record.HasFields(JsonResponse, {"requests"})   then JsonResponse[requests]
                else if Type.Is(Value.Type(JsonResponse), List.Type)   then JsonResponse
                else {},

            TotalCount =
                if      Record.HasFields(JsonResponse, {"totalCount"}) then JsonResponse[totalCount]
                else if Record.HasFields(JsonResponse, {"total"})      then JsonResponse[total]
                else List.Count(Data)
        in
            [Data = Data, TotalCount = TotalCount],

    // ===== FIRST PAGE =====
    FirstPage     = GetRequestsPage(0, PageSize),
    FirstPageData = if Type.Is(Value.Type(FirstPage[Data]), List.Type) then FirstPage[Data] else {},
    TotalRecords  = FirstPage[TotalCount],

    // ===== PAGE COUNT =====
    TotalPagesComputed = if (TotalRecords <> null and TotalRecords > 0) then Number.RoundUp(TotalRecords / PageSize) else 1,
    TotalPages = if MaxPages <> null then List.Min({TotalPagesComputed, MaxPages}) else TotalPagesComputed,

    // ===== OTHER PAGES =====
    OtherPagesData =
        if TotalPages > 1 then
            List.Generate(
                () => [Page = 1],
                each [Page] < TotalPages,
                each [Page = [Page] + 1],
                each
                    let
                        Offset   = [Page] * PageSize,
                        PageRec  = GetRequestsPage(Offset, PageSize),
                        PageList = if Type.Is(Value.Type(PageRec[Data]), List.Type) then PageRec[Data] else {}
                    in
                        PageList
            )
        else
            {},

    // ===== COMBINE =====
    AllDataLists = { FirstPageData } & OtherPagesData,
    AllData      = List.Combine(AllDataLists),

    // ===== TABLE =====
    SourceTable = Table.FromList(AllData, Splitter.SplitByNothing(), {"Column1"}, null, ExtraValues.Error),

    // Expand with all discovered columns (or provide a small schema if empty)
    AllColumns =
        if List.Count(AllData) > 0 then
            List.Distinct(List.Combine(List.Transform(AllData, each Record.FieldNames(_))))
        else
            {"id", "name", "subject", "statusName", "requesterEmail", "createdTime"},

    ExpandedTable =
        if Table.RowCount(SourceTable) > 0 then
            Table.ExpandRecordColumn(SourceTable, "Column1", AllColumns)
        else
            #table(AllColumns, {}),

    // ===== TIMESTAMP CONVERSION =====
    ConvertUnixTimestamp = (ts as any) as nullable datetime =>
        let
            n = try Number.From(ts) otherwise null
        in
            if n = null then null
            else #datetime(1970, 1, 1, 0, 0, 0) + #duration(0, 0, 0, n / 1000.0),

    TimestampColumns = {
        "createdTime","updatedTime","resolvedTime","lastOpenedTime","lastResolvedTime",
        "firstResponseTime","lastClosedTime","dueBy","responseDue","lastViolationTime"
    },
    ExistingTimestampCols = List.Intersect({TimestampColumns, Table.ColumnNames(ExpandedTable)}),

    TimestampConverted =
        if List.Count(ExistingTimestampCols) > 0 then
            Table.TransformColumns(
                ExpandedTable,
                List.Transform(ExistingTimestampCols, each {_, ConvertUnixTimestamp, type datetime})
            )
        else
            ExpandedTable,

    // ===== SAFE TYPE CASTS (only where columns exist) =====
    DesiredTypeMap = {
        {"id", Int64.Type},
        {"name", type text},
        {"subject", type text},
        {"description", type text},
        {"statusName", type text},
        {"priorityName", type text},
        {"impactName", type text},
        {"urgencyName", type text},
        {"requesterName", type text},
        {"requesterEmail", type text},
        {"createdByName", type text},
        {"updatedByName", type text},
        {"source", type text},
        {"supportLevel", type text},
        {"spam", type logical},
        {"createdTime", type datetime},
        {"updatedTime", type datetime},
        {"categoryName", type text}
    },
    ExistingCols   = Table.ColumnNames(TimestampConverted),
    ExistingTypeMap = List.Select(DesiredTypeMap, each List.Contains(ExistingCols, _{0})),

    TypedTable =
        if List.Count(ExistingTypeMap) > 0
        then Table.TransformColumnTypes(TimestampConverted, ExistingTypeMap, "en-US")
        else TimestampConverted,

    // ===== METADATA =====
    WithExtractedAt = Table.AddColumn(TypedTable, "DataExtractedAt", each DateTime.LocalNow(), type datetime),
    FinalTable      = Table.AddColumn(WithExtractedAt, "APISource", each "Motadata ServiceOps v1 API (byqual)", type text)
in
    FinalTable