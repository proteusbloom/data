Sub MergeSongIDsByArtist()
    Dim wsSource As Worksheet
    Dim wsOutput As Worksheet
    Dim lastRow As Long
    Dim lastCol As Long
    Dim i As Long, j As Long
    Dim artistDict As Object
    Dim artist As Variant
    Dim songIDs As String
    Dim songList As Object
    Dim cell As Variant
    Dim outputRow As Long
    
    ' Set source worksheet (change "Sheet1" to your sheet name)
    Set wsSource = ThisWorkbook.Sheets("Sheet1")
    
    ' Create or clear output worksheet
    On Error Resume Next
    Set wsOutput = ThisWorkbook.Sheets("MergedSongs")
    If wsOutput Is Nothing Then
        Set wsOutput = ThisWorkbook.Sheets.Add(After:=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count))
        wsOutput.Name = "MergedSongs"
    Else
        wsOutput.Cells.Clear
    End If
    On Error GoTo 0
    
    ' Find last row and column with data
    lastRow = wsSource.Cells(wsSource.Rows.Count, 1).End(xlUp).Row
    lastCol = wsSource.Cells(1, wsSource.Columns.Count).End(xlToLeft).Column
    
    ' Create dictionary to store artists and their song IDs
    Set artistDict = CreateObject("Scripting.Dictionary")
    
    ' Loop through each row (skip header if row 1 is header)
    For i = 2 To lastRow ' Change to 1 if no header
        artist = wsSource.Cells(i, 1).Value
        
        ' Initialize dictionary entry for this artist if needed
        If Not artistDict.Exists(artist) Then
            Set artistDict(artist) = CreateObject("Scripting.Dictionary")
        End If
        
        ' Collect all song IDs from this row
        For j = 2 To lastCol
            If Not IsEmpty(wsSource.Cells(i, j).Value) Then
                cell = CStr(wsSource.Cells(i, j).Value)
                ' Add to dictionary (automatically handles duplicates)
                If Not artistDict(artist).Exists(cell) Then
                    artistDict(artist).Add cell, Nothing
                End If
            End If
        Next j
    Next i
    
    ' Write headers to output sheet
    wsOutput.Cells(1, 1).Value = "Artist"
    wsOutput.Cells(1, 2).Value = "Song IDs"
    wsOutput.Range("A1:B1").Font.Bold = True
    
    ' Write merged data
    outputRow = 2
    For Each artist In artistDict.Keys
        wsOutput.Cells(outputRow, 1).Value = artist
        
        ' Build comma-separated list of song IDs
        songIDs = ""
        For Each cell In artistDict(artist).Keys
            If songIDs = "" Then
                songIDs = cell
            Else
                songIDs = songIDs & ", " & cell
            End If
        Next cell
        
        wsOutput.Cells(outputRow, 2).Value = songIDs
        outputRow = outputRow + 1
    Next artist
    
    ' Format output sheet
    wsOutput.Columns("A").ColumnWidth = 15
    wsOutput.Columns("B").ColumnWidth = 80
    wsOutput.Columns("B").WrapText = True
    
    ' Auto-fit rows
    wsOutput.Rows.AutoFit
    
    MsgBox "Merge complete! Check the 'MergedSongs' sheet.", vbInformation
    
End Sub
