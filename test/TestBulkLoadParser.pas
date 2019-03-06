unit TestBulkLoadParser;

interface

uses
  Classes,
  TestFramework,
  OleDbUtils;

type
  TTestTBulkLoadFieldDefsDDLParser = class(TTestCase)
  private
  public
    procedure SetUp; override;
    procedure TearDown; override;
  published
    procedure TestParseDDL_EmptyString;
    procedure TestParseDDL_1;
  end;

implementation

uses
  SysUtils, DB,
  BulkLoadParser;

{ TTestBulkLoadParser }

procedure TTestTBulkLoadFieldDefsDDLParser.SetUp;
begin
  inherited;

end;

procedure TTestTBulkLoadFieldDefsDDLParser.TearDown;
begin
  inherited;

end;

procedure TTestTBulkLoadFieldDefsDDLParser.TestParseDDL_1;
var
  LFieldDefs: IBulkLoadFieldDefs;
begin
  LFieldDefs := TBulkLoadFieldDefsDDLParser.Parse(
    // just copied DDL
    '	[BAN] [varchar] (50) COLLATE Cyrillic_General_CI_AS NULL ,' + #13#10 +
    '	[BEN] [varchar]   (10) COLLATE Cyrillic_General_CI_AS NULL ,' + #13#10 +
    '	[CTN] [varchar](20)NOT NULL ,' + #13#10 +
    '     [Last] [datetime] NULL ,' + #13#10 +
    '[Orig] [money] NULL ,' + #13#10 +
    '	[SessionID] [int] NOT NULL ,' + #13#10 +
    '	[Session2] [bigint] NOT NULL'
    );
  CheckEquals(7, LFieldDefs.Count);

  CheckEquals(50, LFieldDefs.FieldDefs[0].DataSize, '0: DataSize');
  CheckEquals(Ord(ftString), Ord(LFieldDefs.FieldDefs[0].DataType), '0: DataType: ftString');

  CheckEquals(10, LFieldDefs.FieldDefs[1].DataSize, '1: DataSize');
  CheckEquals(Ord(ftString), Ord(LFieldDefs.FieldDefs[1].DataType), '1: DataType: ftString');

  CheckEquals(20, LFieldDefs.FieldDefs[2].DataSize, '2: DataSize');
  CheckEquals(Ord(ftString), Ord(LFieldDefs.FieldDefs[2].DataType), '2: DataType: ftString');

  CheckEquals(Ord(ftDateTime), Ord(LFieldDefs.FieldDefs[3].DataType), '3: DataType: ftDateTime');
  CheckEquals(Ord(ftCurrency), Ord(LFieldDefs.FieldDefs[4].DataType), '4: DataType: ftCurrency');
  CheckEquals(Ord(ftInteger), Ord(LFieldDefs.FieldDefs[5].DataType), '5: DataType: ftInteger');
  CheckEquals(Ord(ftLargeInt), Ord(LFieldDefs.FieldDefs[6].DataType), '6: DataType: ftLargeInt');
end;

procedure TTestTBulkLoadFieldDefsDDLParser.TestParseDDL_EmptyString;
var
  LFieldDefs: IBulkLoadFieldDefs;
begin
  LFieldDefs := TBulkLoadFieldDefsDDLParser.Parse('');
  CheckEquals(0, LFieldDefs.Count);
end;

initialization
end.
