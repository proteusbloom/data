// ============================================================================
// POWER QUERY M - PSEUDO MODEL: CONCERT SALES SPLITTING
// ============================================================================
// Scenario: Split concert revenue 50/50 when certain venues have double-bill shows
// - Check if venue is in "DoubleBillVenues" lookup table (your 50 conditions)
// - Split revenue 50/50 between two artists
// - Update artist name and other columns from "ArtistSplitDetails" lookup table
// ============================================================================

let
    // ========================================================================
    // 1. SOURCE TABLES
    // ========================================================================
    
    // Main concert sales data
    Source_ConcertSales = Table.FromRecords({
        [ConcertID = "C001", VenueName = "Madison Square Garden", Date = #date(2024, 3, 15), Artist = "Taylor Swift & Ed Sheeran", Revenue = 500000, TicketsSold = 5000, Category = "Pop"],
        [ConcertID = "C002", VenueName = "Hollywood Bowl", Date = #date(2024, 3, 20), Artist = "Beyoncé", Revenue = 300000, TicketsSold = 3000, Category = "R&B"],
        [ConcertID = "C003", VenueName = "Red Rocks Amphitheatre", Date = #date(2024, 4, 10), Artist = "Coldplay & Imagine Dragons", Revenue = 450000, TicketsSold = 4500, Category = "Rock"],
        [ConcertID = "C004", VenueName = "Staples Center", Date = #date(2024, 4, 15), Artist = "Drake", Revenue = 400000, TicketsSold = 4000, Category = "Hip-Hop"],
        [ConcertID = "C005", VenueName = "Madison Square Garden", Date = #date(2024, 5, 1), Artist = "Adele & Sam Smith", Revenue = 600000, TicketsSold = 6000, Category = "Pop"]
    }),
    
    // Lookup table: Venues that require splitting (your 50 conditions)
    // This would be your existing merged table from previous steps
    Lookup_DoubleBillVenues = Table.FromRecords({
        [VenueName = "Madison Square Garden", RequiresSplit = true],
        [VenueName = "Red Rocks Amphitheatre", RequiresSplit = true],
        [VenueName = "Wembley Stadium", RequiresSplit = true]
        // ... (47 more venues in your actual scenario)
    }),
    
    // Lookup table: Artist split details (for updating column values in split rows)
    Lookup_ArtistSplitDetails = Table.FromRecords({
        [OriginalArtistPair = "Taylor Swift & Ed Sheeran", Artist1 = "Taylor Swift", Artist2 = "Ed Sheeran", Artist1Genre = "Pop", Artist2Genre = "Country-Pop", Artist1Label = "Republic Records", Artist2Label = "Atlantic Records"],
        [OriginalArtistPair = "Coldplay & Imagine Dragons", Artist1 = "Coldplay", Artist2 = "Imagine Dragons", Artist1Genre = "Alternative Rock", Artist2Genre = "Pop Rock", Artist1Label = "Parlophone", Artist2Label = "Interscope"],
        [OriginalArtistPair = "Adele & Sam Smith", Artist1 = "Adele", Artist2 = "Sam Smith", Artist1Genre = "Soul", Artist2Genre = "Pop", Artist1Label = "Columbia Records", Artist2Label = "Capitol Records"]
        // ... (more artist pairs)
    }),
    
    
    // ========================================================================
    // 2. MERGE WITH LOOKUP TABLE TO IDENTIFY ROWS REQUIRING SPLIT
    // ========================================================================
    
    // Merge with DoubleBillVenues lookup to add RequiresSplit flag
    MergedWithVenueLookup = Table.NestedJoin(
        Source_ConcertSales,
        {"VenueName"},
        Lookup_DoubleBillVenues,
        {"VenueName"},
        "VenueLookup",
        JoinKind.LeftOuter
    ),
    
    // Expand the RequiresSplit column
    ExpandedVenueLookup = Table.ExpandTableColumn(
        MergedWithVenueLookup,
        "VenueLookup",
        {"RequiresSplit"},
        {"RequiresSplit"}
    ),
    
    // Replace null with false for venues not in the lookup table
    AddedRequiresSplitFlag = Table.ReplaceValue(
        ExpandedVenueLookup,
        null,
        false,
        Replacer.ReplaceValue,
        {"RequiresSplit"}
    ),
    
    
    // ========================================================================
    // 3. MERGE WITH ARTIST SPLIT DETAILS LOOKUP
    // ========================================================================
    
    // Merge with ArtistSplitDetails to get split information
    MergedWithArtistLookup = Table.NestedJoin(
        AddedRequiresSplitFlag,
        {"Artist"},
        Lookup_ArtistSplitDetails,
        {"OriginalArtistPair"},
        "ArtistSplitDetails",
        JoinKind.LeftOuter
    ),
    
    // Expand artist split details
    ExpandedArtistLookup = Table.ExpandTableColumn(
        MergedWithArtistLookup,
        "ArtistSplitDetails",
        {"Artist1", "Artist2", "Artist1Genre", "Artist2Genre", "Artist1Label", "Artist2Label"},
        {"Artist1", "Artist2", "Artist1Genre", "Artist2Genre", "Artist1Label", "Artist2Label"}
    ),
    
    
    // ========================================================================
    // 4. CREATE SPLIT ROWS USING CUSTOM FUNCTION
    // ========================================================================
    
    // Custom function to split a row into two rows (Artist 1 and Artist 2)
    SplitRowFunction = (row as record) as table =>
        let
            // Calculate split revenue (50/50)
            SplitRevenue = row[Revenue] / 2,
            
            // Create Row for Artist 1
            Row1 = [
                ConcertID = row[ConcertID] & "-A1",  // Modified ID
                VenueName = row[VenueName],
                Date = row[Date],
                Artist = row[Artist1],                // Looked up value
                Revenue = SplitRevenue,               // Split 50/50
                TicketsSold = row[TicketsSold] / 2,  // Split tickets too
                Category = row[Artist1Genre],         // Looked up value
                RecordLabel = row[Artist1Label],      // Looked up value (new column)
                SplitType = "Artist 1"
            ],
            
            // Create Row for Artist 2
            Row2 = [
                ConcertID = row[ConcertID] & "-A2",  // Modified ID
                VenueName = row[VenueName],
                Date = row[Date],
                Artist = row[Artist2],                // Looked up value
                Revenue = SplitRevenue,               // Split 50/50
                TicketsSold = row[TicketsSold] / 2,  // Split tickets too
                Category = row[Artist2Genre],         // Looked up value
                RecordLabel = row[Artist2Label],      // Looked up value (new column)
                SplitType = "Artist 2"
            ],
            
            // Combine both rows into a table
            ResultTable = Table.FromRecords({Row1, Row2})
        in
            ResultTable,
    
    
    // ========================================================================
    // 5. CONDITIONAL LOGIC: SPLIT OR KEEP ORIGINAL
    // ========================================================================
    
    // Add custom column that contains either split rows or original row
    AddedConditionalSplit = Table.AddColumn(
        ExpandedArtistLookup,
        "ProcessedRows",
        each if [RequiresSplit] = true then
            // CONDITION IS TRUE: Split the row
            SplitRowFunction(_)
        else
            // CONDITION IS FALSE: Keep original row (as single-row table)
            Table.FromRecords({
                [
                    ConcertID = [ConcertID],
                    VenueName = [VenueName],
                    Date = [Date],
                    Artist = [Artist],
                    Revenue = [Revenue],
                    TicketsSold = [TicketsSold],
                    Category = [Category],
                    RecordLabel = null,  // No split, so no label lookup
                    SplitType = "Original"
                ]
            }),
        type table
    ),
    
    
    // ========================================================================
    // 6. EXPAND THE NESTED TABLES TO GET FINAL RESULT
    // ========================================================================
    
    // Expand the ProcessedRows column to flatten the table
    ExpandedProcessedRows = Table.ExpandTableColumn(
        AddedConditionalSplit,
        "ProcessedRows",
        {"ConcertID", "VenueName", "Date", "Artist", "Revenue", "TicketsSold", "Category", "RecordLabel", "SplitType"},
        {"Final_ConcertID", "Final_VenueName", "Final_Date", "Final_Artist", "Final_Revenue", "Final_TicketsSold", "Final_Category", "Final_RecordLabel", "Final_SplitType"}
    ),
    
    // Remove intermediate columns and keep only final columns
    RemovedIntermediateColumns = Table.RemoveColumns(
        ExpandedProcessedRows,
        {"ConcertID", "VenueName", "Date", "Artist", "Revenue", "TicketsSold", "Category", "RequiresSplit", "Artist1", "Artist2", "Artist1Genre", "Artist2Genre", "Artist1Label", "Artist2Label"}
    ),
    
    // Rename final columns to clean names
    RenamedFinalColumns = Table.RenameColumns(
        RemovedIntermediateColumns,
        {
            {"Final_ConcertID", "ConcertID"},
            {"Final_VenueName", "VenueName"},
            {"Final_Date", "Date"},
            {"Final_Artist", "Artist"},
            {"Final_Revenue", "Revenue"},
            {"Final_TicketsSold", "TicketsSold"},
            {"Final_Category", "Category"},
            {"Final_RecordLabel", "RecordLabel"},
            {"Final_SplitType", "SplitType"}
        }
    ),
    
    // Set proper data types
    SetDataTypes = Table.TransformColumnTypes(
        RenamedFinalColumns,
        {
            {"ConcertID", type text},
            {"VenueName", type text},
            {"Date", type date},
            {"Artist", type text},
            {"Revenue", Currency.Type},
            {"TicketsSold", Int64.Type},
            {"Category", type text},
            {"RecordLabel", type text},
            {"SplitType", type text}
        }
    )

in
    SetDataTypes


// ============================================================================
// EXPECTED OUTPUT:
// ============================================================================
// Original Row (C001): Madison Square Garden → SPLITS into 2 rows
//   - C001-A1: Taylor Swift, $250,000, Pop, Republic Records
//   - C001-A2: Ed Sheeran, $250,000, Country-Pop, Atlantic Records
//
// Original Row (C002): Hollywood Bowl → STAYS AS 1 row (not in lookup)
//   - C002: Beyoncé, $300,000, R&B, null
//
// Original Row (C003): Red Rocks → SPLITS into 2 rows
//   - C003-A1: Coldplay, $225,000, Alternative Rock, Parlophone
//   - C003-A2: Imagine Dragons, $225,000, Pop Rock, Interscope
//
// Original Row (C004): Staples Center → STAYS AS 1 row (not in lookup)
//   - C004: Drake, $400,000, Hip-Hop, null
//
// Original Row (C005): Madison Square Garden → SPLITS into 2 rows
//   - C005-A1: Adele, $300,000, Soul, Columbia Records
//   - C005-A2: Sam Smith, $300,000, Pop, Capitol Records
// ============================================================================
