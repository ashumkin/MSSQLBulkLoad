unit BulkLoadParser;

interface

uses
  Classes,
  OleDBUtils;

type
  TBulkLoadFieldDefsDDLParser = class
    class function Parse(const ADDL: string): IBulkLoadFieldDefs;
  end;

implementation

uses
  SysUtils, StrUtils;

{ TBulkLoadFieldDefsDDLParser }

class function TBulkLoadFieldDefsDDLParser.Parse(
  const ADDL: string): IBulkLoadFieldDefs;
var
  LStrings: TStrings;
  i: Integer;
  LString: string;
  LTokens: TStrings;
  LType: string;
  LSize: string;
  LTokenCount: Integer;
begin
  Result := TBulkLoadFieldDefs.Create;
  LStrings := TStringList.Create;
  try
    if ExtractStrings([','], [#9, ' '], PChar(ADDL), LStrings) = 0 then
      Exit;
    LTokens := TStringList.Create;
    try
      for i := 0 to LStrings.Count - 1 do
      begin
        LString := StringReplace(LStrings[i], '  ', ' ', [rfReplaceAll]);
        LString := StringReplace(LString, ' (', '(', [rfReplaceAll]);
        LTokens.Clear;
        LTokenCount := ExtractStrings(['[', ']', ' ', '(', ')'], [], PChar(LString), LTokens);
        // invalid field description?
        if LTokenCount < 3 then
          Continue;
        LType := LTokens[1];
        LSize := LTokens[2];
        if SameText(LType, 'varchar') then
          Result.AddFieldDef(TBulkLoadFieldDefString.Create(StrToIntDef(LSize, 0)))
        else if SameText(LType, 'money') then
          Result.AddFieldDef(TBulkLoadFieldDefCurrency.Create)
        else if SameText(LType, 'datetime') then
          Result.AddFieldDef(TBulkLoadFieldDefDateTime.Create)
        else if SameText(LType, 'int') then
          Result.AddFieldDef(TBulkLoadFieldDefInteger.Create)
        else if SameText(LType, 'bigint') then
          Result.AddFieldDef(TBulkLoadFieldDefInt64.Create)
        ;
      end;
    finally
      FreeAndNil(LTokens);
    end;
  finally
    FreeAndNil(LStrings);
  end;
end;

end.
