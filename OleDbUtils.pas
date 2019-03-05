unit OleDbUtils;

{$IF CompilerVersion > 21}
{$DEFINE DELPHI_XE}
{$IFEND}

interface

uses
  Classes,
  SysUtils,
  ComObj,
  Windows,
  ActiveX,
  ADOInt,
  OleDb,
  DB,
  ADOConst,
  ADODB;

const
  DBPROPSET_SQLSERVERDATASOURCE: TGUID = '{28EFAEE4-2D2C-11D1-9807-00C04FC2AD98}';
  SSPROP_ENABLEFASTLOAD = 2;

  SIID_IRowsetFastLoad = '{5CF4CA13-EF21-11D0-97E7-00C04FC2AD98}';
  SIID_IErrorRecords = '{0C733A67-2A1C-11CE-ADE5-00AA0044773D}';

  IID_IRowsetFastLoad: TGUID = SIID_IRowsetFastLoad;
  IID_IErrorRecords: TGUID = SIID_IErrorRecords;

type
  DBLENGTH = ULONGLONG;

  PColumnData = ^TColumnData;
  TColumnData = record
    Length: DBLENGTH;
    Status: DBSTATUS;
    Data: array[0..0] of Byte;
  end;

  IBulkLoadFieldDef = interface
    ['{1B80665D-D7BE-4E7A-B214-F2DED9E4D862}']
    function DataSize: Integer;
    function DataType: TFieldType;
    function FieldNo: Integer;
    procedure SetBindingMaxLen(var ABinding: TDBBinding);
    procedure SetFieldNo(const AFieldNo: Integer);
  end;

  IBulkLoadFieldDefs = interface(IInterfaceList)
    ['{05F01A7D-58B5-4623-AB18-F844FD75C653}']
    function AddFieldDef(ABulkLoadFieldDef: IBulkLoadFieldDef): IBulkLoadFieldDefs;
    function GetFields(Index: Integer): IBulkLoadFieldDef;

    property FieldDefs[Index: Integer]: IBulkLoadFieldDef read GetFields;
  end;

  IBulkLoadField = interface
    ['{6578BCBC-9087-43D9-B852-6ED85FBA82E7}']
    procedure FillColumn(AColumn: PColumnData);
  end;

  IBulkLoader = interface
    ['{E7795A09-A93A-4532-A5B5-1D670ECEB9CD}']
    function Append(AField: IBulkLoadField): IBulkLoader;
    function AppendRow: IBulkLoader;
    procedure Commit;
  end;

  IRowsetFastLoad = interface
    [SIID_IRowsetFastLoad]
    function InsertRow(hAccessor: HACCESSOR; pData: Pointer): HRESULT; stdcall;
    function Commit(fDone: BOOL): HRESULT; stdcall;
  end;

  IErrorRecords = interface
    [SIID_IErrorRecords]
    function AddErrorRecord(pErrorInfo: PErrorInfo; dwLookupID: UINT; pDispParams: pointer; punkCustomError: IUnknown;
      dwDynamicErrorID: UINT): HResult; stdcall;
    function GetBasicErrorInfo(ulRecordNum: UINT; pErrorInfo: PErrorInfo): HResult; stdcall;
    function GetCustomErrorObject(ulRecordNum: UINT; const riid: TGUID; var ppObject: IUnknown): HResult; stdcall;
    function GetErrorInfo(ulRecordNum: UINT; lcid: UINT; var ppErrorInfo: IErrorInfo): HResult; stdcall;
    function GetErrorParameters(ulRecordNum: UINT; pDispParams: pointer): HResult; stdcall;
    function GetRecordCount(var pcRecords: UINT): HResult; stdcall;
  end;

  TOleDbError = record
    Guid: TGUID;
    BasicInfo: TErrorInfo;
    Source: WideString;
    Description: WideString;
    HelpFile: WideString;
    HelpContext: Integer;
    SqlNativeError: Integer;
    SqlState: WideString;
  end;
  TOleDbErrorDynArray = array of TOleDbError;

  EOleDbError = class(EOleSysError)
    Errors: TOleDbErrorDynArray;
  end;

  TBulkLoadFieldDef = class(TInterfacedObject, IBulkLoadFieldDef)
  private
    FFieldNo: Integer;
    FDataSize: Integer;
    FDataType: TFieldType;
    {$REGION 'IBulkLoadFieldDef'}
    function DataSize: Integer;
    function DataType: TFieldType;
    function FieldNo: Integer;
    procedure SetBindingMaxLen(var ABinding: TDBBinding); virtual;
    procedure SetFieldNo(const AFieldNo: Integer);
    {$ENDREGION}
  public
    constructor Create(const ADataType: TFieldType; ADataSize: Integer);
  end;

  TBulkLoadFieldDefs = class(TInterfaceList, IBulkLoadFieldDefs)
  private
    function GetFields(Index: Integer): IBulkLoadFieldDef;
  public
    {$REGION 'IBulkLoadFieldDefs'}
    function AddFieldDef(ABulkLoadFieldDef: IBulkLoadFieldDef): IBulkLoadFieldDefs;
    {$ENDREGION}
  end;

  TBulkLoadFieldDefString = class(TBulkLoadFieldDef)
  public
    constructor Create(const ALength: Integer);
  end;

  TBulkLoadFieldDefDateTime = class(TBulkLoadFieldDef)
  public
    constructor Create;
  end;

  TBulkLoadFieldDefCurrency = class(TBulkLoadFieldDef)
  public
    constructor Create;
  end;

  TBulkLoadFieldDefInteger = class(TBulkLoadFieldDef)
  public
    constructor Create;
  end;

  TBulkLoadFieldDefInt64 = class(TBulkLoadFieldDef)
  public
    constructor Create;
  end;

  TBulkLoadField = class(TInterfacedObject, IBulkLoadField)
  private
    procedure SetColumnSize(AColumn: PColumnData); virtual;
    {$REGION 'IBulkLoadField'}
    procedure FillColumn(AColumn: PColumnData); virtual;
    {$ENDREGION}
  end;

  TBulkLoadFieldNull = class(TBulkLoadField)
  private
    procedure FillColumn(AColumn: PColumnData); override;
  end;

  TBulkLoadFieldString = class(TBulkLoadField)
  private
    FString: string;
    procedure SetColumnSize(AColumn: PColumnData); override;
    {$REGION 'IBulkLoadField'}
    procedure FillColumn(AColumn: PColumnData); override;
    {$ENDREGION}
  public
    constructor Create(const AString: string);
  end;

  TBulkLoadFieldInteger = class(TBulkLoadField)
  private
    FInteger: Integer;
    procedure SetColumnSize(AColumn: PColumnData); override;
    {$REGION 'IBulkLoadField'}
    procedure FillColumn(AColumn: PColumnData); override;
    {$ENDREGION}
  public
    constructor Create(const AInteger: Integer);
  end;

  TBulkLoadFieldInt64 = class(TBulkLoadField)
  private
    FInt64: Int64;
    procedure SetColumnSize(AColumn: PColumnData); override;
    {$REGION 'IBulkLoadField'}
    procedure FillColumn(AColumn: PColumnData); override;
    {$ENDREGION}
  public
    constructor Create(const AInt64: Int64);
  end;

  TBulkLoadFieldCurrency = class(TBulkLoadField)
  private
    FCurrency: Currency;
    procedure SetColumnSize(AColumn: PColumnData); override;
    {$REGION 'IBulkLoadField'}
    procedure FillColumn(AColumn: PColumnData); override;
    {$ENDREGION}
  public
    constructor Create(const ACurrency: Currency);
  end;

  TBulkLoadFieldDateTime = class(TBulkLoadField)
  private
    FDateTime: TDateTime;
    {$REGION 'IBulkLoadField'}
    procedure FillColumn(AColumn: PColumnData); override;
    {$ENDREGION}
  public
    constructor Create(const ADateTime: TDateTime);
  end;

  TMSSQLBulkLoader = class(TInterfacedObject, IBulkLoader)
  private
    FAccessor: IAccessor;
    FConnection: TADOConnection;
    FFastLoad: IRowsetFastLoad;
    FStatusCodes: PUintArray;
    FBindings: PDBBindingArray;
    FAccessorHandle: THandle;
    FBuffer: Pointer;
    FFieldCount: Integer;
    FAppendedFieldCount: Integer;
    {$REGION 'IBulkLoader'}
    function Append(AField: IBulkLoadField): IBulkLoader;
    function AppendRow: IBulkLoader;
    procedure Commit;
    {$ENDREGION}
    procedure ResetAppendedFieldCount;
  public
    constructor Create(AConnection: TADOConnection; const ADstTableName: string;
      AFields: IBulkLoadFieldDefs);
    procedure BeforeDestruction; override;
  end;

procedure BulkCopy(Dataset: TDataSet; Connection: TADOConnection; const DstTableName: string);
function FieldTypeToOleDbType(const FieldType: TFieldType): DBTYPEENUM;
procedure OleDbCheck(Result: HResult; const Instance: IUnknown; const IID: TGUID; StatusCodes: PUintArray = nil;
  StatusCount: Integer = 0);
function OpenFastLoad(const DBCreateSession: IDBCreateSession; const TableName: WideString): IRowsetFastLoad; overload;
function OpenFastLoad(Connection: TADOConnection; const TableName: WideString): IRowsetFastLoad; overload;
procedure InitializeProperty(var Prop: TDBProp; PropID: DBPROPID; const Value: OleVariant);
procedure SetProperties(const DBProperties: IDBProperties; const PropertySetID: TGUID;
  const PropertyIDs: array of Cardinal; const Values: OleVariant); overload;
procedure SetProperties(Connection: TADOConnection; const PropertySetID: TGUID; const PropertyIDs: array of Cardinal;
  const Values: OleVariant); overload;
procedure SetProperty(const DBProperties: IDBProperties; const PropertySetID: TGUID; PropertyID: Cardinal;
  const Value: OleVariant); overload;
procedure SetProperty(Connection: TADOConnection; const PropertySetID: TGUID; PropertyID: Cardinal;
  const Value: OleVariant); overload;

implementation

uses
  Variants;

{$IFNDEF DELPHI_XE}
function WStrLen(const Str: PWideChar): Cardinal;
var
  P : PWideChar;
begin
  P := Str;
  while (P^ <> #0) do Inc(P);
  Result := (P - Str);
end;
{$ENDIF}

procedure Align(var Value: Integer; Alignment: Byte = 8); forward;
procedure GetFieldValue(Field: TField; const Binding: TDBBinding; Buffer: Pointer; BlobList: TList); forward;
procedure InitializeBinding(Field: TField; var Binding: TDBBinding; var Offset: Integer); forward;

procedure Align(var Value: Integer; Alignment: Byte = 8);
var
  M: Byte;
begin
  M := Value mod Alignment;
  if M <> 0 then
    Inc(Value, Alignment - M);
end;

procedure BulkCopy(Dataset: TDataSet; Connection: TADOConnection; const DstTableName: string);
var
  BlobList: TList;
  Bindings: PDBBindingArray;
  StatusCodes: PUIntArray;
  Buffer: Pointer;
  I, BufferSize: Integer;
  FastLoad: IRowsetFastLoad;
  Accessor: IAccessor;
  AccessorHandle: THandle;
begin
  BufferSize := 0;
  BlobList := nil;
  Bindings := AllocMem(Dataset.FieldCount * SizeOf(TDBBinding));
  try
    for I := 0 to Dataset.FieldCount - 1 do
    begin
      if not Assigned(BlobList) and Dataset.Fields[I].IsBlob then
        BlobList := TList.Create;
      InitializeBinding(Dataset.Fields[I], Bindings^[I], BufferSize);
    end;

    Buffer := AllocMem(BufferSize);
    try
      StatusCodes := AllocMem(Dataset.FieldCount * SizeOf(DBBINDSTATUS));
      try
        Connection.Connected := True;
        FastLoad := OpenFastLoad(Connection, DstTableName);
        OleDbCheck(FastLoad.QueryInterface(IID_IAccessor, Accessor), FastLoad, IID_IRowsetFastLoad, StatusCodes,
          Dataset.FieldCount);
        OleDbCheck(Accessor.CreateAccessor(DBACCESSOR_ROWDATA, Dataset.FieldCount, Bindings, BufferSize,
          AccessorHandle, StatusCodes), Accessor, IID_IAccessor, StatusCodes, Dataset.FieldCount);
        try
          while not Dataset.Eof do
          begin
            try
              for I := 0 to Dataset.FieldCount - 1 do
                GetFieldValue(Dataset.Fields[I], Bindings^[I], Buffer, BlobList);

              OleDbCheck(FastLoad.InsertRow(AccessorHandle, Buffer), FastLoad, IID_IRowsetFastLoad, StatusCodes,
                Dataset.FieldCount);
            finally
              if Assigned(BlobList) then
              begin
                for I := 0 to BlobList.Count - 1 do
                  TStream(BlobList[I]).Free;
                BlobList.Clear;
              end;
            end;

            Dataset.Next;
          end;

          OleDbCheck(FastLoad.Commit(True), FastLoad, IID_IRowsetFastLoad, StatusCodes, Dataset.FieldCount);
        finally
          OleDbCheck(Accessor.ReleaseAccessor(AccessorHandle, nil), Accessor, IID_IAccessor, nil, 0);
        end;
      finally
        FreeMem(StatusCodes);
      end;
    finally
      FreeMem(Buffer);
    end;
  finally
    FreeMem(Bindings);
    BlobList.Free;
  end;
end;

function FieldTypeToOleDbType(const FieldType: TFieldType): DBTYPEENUM;
begin
  case FieldType of
    ftUnknown: Result := DBTYPE_EMPTY;                                             // ?
    ftString, ftMemo, ftFixedChar: Result := DBTYPE_STR;                           // varchar
    ftWideString, ftWideMemo, ftFixedWideChar: Result := DBTYPE_WSTR;              // nvarchar
{$IFDEF DELPHI_XE}
    ftByte, ftShortint: Result := DBTYPE_I1;                                       // tinyint
{$ENDIF}
    ftSmallint, ftWord: Result := DBTYPE_I2;                                       // smallint
    ftInteger, ftAutoInc{$IFDEF DELPHI_XE}, ftLongWord{$ENDIF}: Result := DBTYPE_I4;                         // int, identity
    ftBoolean: Result := DBTYPE_BOOL;                                              // bit
    ftFloat: Result := DBTYPE_R8;                                                  // float
{$IFDEF DELPHI_XE}
    ftSingle: Result := DBTYPE_R4;                                                 // real
{$ENDIF}
    ftBCD, ftCurrency: Result := DBTYPE_CY;                                        // money
    ftDate: Result := DBTYPE_DBDATE;                                               // date
    ftTime: Result := DBTYPE_DBTIME;                                               // time
    ftDateTime, ftTimestamp: Result := DBTYPE_DBTIMESTAMP;                         // datetime
    ftBytes, ftVarBytes, ftBlob, ftGraphic..ftTypedBinary: Result := DBTYPE_BYTES; // binary, varbinary, image
    ftLargeint: Result := DBTYPE_I8;                                               // bigint
    ftVariant: Result := DBTYPE_VARIANT;                                           // sql_variant
    ftInterface: Result := DBTYPE_IUNKNOWN;                                        // ?
    ftIDispatch: Result := DBTYPE_IDISPATCH;                                       // ?
    ftGuid: Result := DBTYPE_GUID;                                                 // uniqueidentifier
  else
    DatabaseErrorFmt(SNoMatchingADOType, [FieldTypeNames[FieldType]]);
    Result := adEmpty;
  end;
end;

procedure GetFieldValue(Field: TField; const Binding: TDBBinding; Buffer: Pointer; BlobList: TList); overload;
var
  Column: PColumnData;
  BlobStream, Stream: TStream;
  MSec: Word;
begin
  Column := Pointer(NativeUInt(Buffer) + Binding.obLength);
  if Field.IsNull then
  begin
    Column^.Status := DBSTATUS_S_ISNULL;
    Column^.Length := 0;
  end
  else if Field.IsBlob then
  begin
    BlobStream := Field.DataSet.CreateBlobStream(Field, bmRead);
    try
      if BlobStream is TMemoryStream then
      begin
        Stream := BlobStream;
        BlobStream := nil;
        BlobList.Add(Stream);
      end
      else
      begin
        Stream := TMemoryStream.Create;
        try
          Stream.CopyFrom(BlobStream, 0);
          BlobList.Add(Stream);
        except
          Stream.Free;
          raise;
        end;
      end;

      Column^.Status := DBSTATUS_S_OK;
      Column^.Length := Stream.Size;
      PPointer(@Column^.Data[0])^ := TMemoryStream(Stream).Memory;
    finally
      BlobStream.Free;
    end;
  end
  else
  begin
    case Field.DataType of
      ftBCD, ftCurrency:
        PCurrency(@Column.Data[0])^ := Field.AsCurrency;
      ftDate:
        with PDBDate(@Column^.Data[0])^ do
          DecodeDate(Field.AsDateTime, Word(year), month, day);
      ftTime:
        with PDBTime(@Column^.Data[0])^ do
          DecodeTime(Field.AsDateTime, hour, minute, second, MSec);
      ftDateTime, ftTimeStamp:
        with PDBTimeStamp(@Column^.Data[0])^ do
        begin
          DecodeDate(Field.AsDateTime, Word(year), month, day);
          DecodeTime(Field.AsDateTime, hour, minute, second, MSec);
          fraction := MSec * 1000000;
        end;
      else
        Field.GetData(@Column^.Data[0], False);
    end;
    Column^.Status := DBSTATUS_S_OK;
    case Field.DataType of
      ftString, ftMemo:
        Column^.Length := StrLen(PAnsiChar(@Column^.Data[0]));
      ftWideString, ftWideMemo:
{$IFDEF DELPHI_XE}
        Column^.Length := StrLen(PWideChar(@Column^.Data[0])) * SizeOf(WideChar)
{$ELSE DELPHI_XE}
        Column^.Length := WStrLen(PWideChar(@Column^.Data[0])) * SizeOf(WideChar);
{$ENDIF}
      else
        Column^.Length := Field.DataSize;
    end;
  end;
end;

procedure GetFieldValue(AField: IBulkLoadField; const FBinding: TDBBinding; FBuffer: Pointer); overload;
var
  LColumn: PColumnData;
begin
  LColumn := Pointer(NativeUInt(FBuffer) + FBinding.obLength);
  // initialize column length with the maximum field length (mostly for strings)
  LColumn.Length := FBinding.cbMaxLen;
  AField.FillColumn(LColumn);
end;

{ TBulkLoadFieldDateTime }

constructor TBulkLoadFieldDateTime.Create(const ADateTime: TDateTime);
begin
  inherited Create;
  FDateTime := ADateTime;
end;

procedure TBulkLoadFieldDateTime.FillColumn(AColumn: PColumnData);
var
  MSec: Word;
begin
  with PDBTimeStamp(@AColumn^.Data[0])^ do
  begin
    DecodeDate(FDateTime, Word(year), month, day);
    DecodeTime(FDateTime, hour, minute, second, MSec);
    fraction := MSec * 1000000;
  end;
  inherited FillColumn(AColumn);
end;

procedure InitializeBinding(Field: TField; var Binding: TDBBinding; var Offset: Integer); overload;
begin
  Binding.iOrdinal := Field.FieldNo;
  Binding.wType := FieldTypeToOleDbType(Field.DataType);
  if Field.IsBlob then
    Binding.wType := Binding.wType or DBTYPE_BYREF;
  Binding.eParamIO := DBPARAMIO_NOTPARAM;
  Binding.dwMemOwner := DBMEMOWNER_CLIENTOWNED;
  Binding.obLength := Offset;
  Binding.obStatus := Binding.obLength + SizeOf(DBLENGTH);
  Binding.obValue := Binding.obStatus + SizeOf(DBSTATUS);
  Binding.dwPart := DBPART_LENGTH or DBPART_STATUS or DBPART_VALUE;
  case Field.DataType of
    ftDate:
      Binding.cbMaxLen := SizeOf(TDBDate);
    ftTime:
      Binding.cbMaxLen := SizeOf(TDBTime);
    ftDateTime, ftTimeStamp:
      Binding.cbMaxLen := SizeOf(TDBTimeStamp);
    else
      Binding.cbMaxLen := Field.DataSize;
  end;

  Inc(Offset, SizeOf(TColumnData) + Binding.cbMaxLen - 1);
  Align(Offset);
end;

function InitializeBinding(AField: IBulkLoadFieldDef; var ABinding: TDBBinding; AOffset: Integer): Integer; overload;
begin
  ABinding.iOrdinal := AField.FieldNo;
  ABinding.wType := FieldTypeToOleDbType(AField.DataType);
  ABinding.eParamIO := DBPARAMIO_NOTPARAM;
  ABinding.dwMemOwner := DBMEMOWNER_CLIENTOWNED;
  ABinding.obLength := AOffset;
  ABinding.obStatus := ABinding.obLength + SizeOf(DBLENGTH);
  ABinding.obValue := ABinding.obStatus + SizeOf(DBSTATUS);
  ABinding.dwPart := DBPART_LENGTH or DBPART_STATUS or DBPART_VALUE;

  AField.SetBindingMaxLen(ABinding);

  Inc(AOffset, SizeOf(TColumnData) + ABinding.cbMaxLen - 1);
  Align(AOffset);
  Result := AOffset;
end;

procedure InitializeProperty(var Prop: TDBProp; PropID: DBPROPID; const Value: OleVariant);
begin
  Prop.dwPropertyID := PropID;
  Prop.dwOptions := DBPROPOPTIONS_REQUIRED;
  Prop.colid := DB_NULLID;
  Prop.vValue := Value;
end;

procedure OleDbCheck(Result: HResult; const Instance: IUnknown; const IID: TGUID; StatusCodes: PUintArray;
  StatusCount: Integer);
var
  SupportErrorInfo: ISupportErrorInfo;
  ErrorInfo: IErrorInfo;
  I: Integer;
  ErrorRecords: IErrorRecords;
  ErrorCount: Cardinal;
  SqlErrorInfoIntf: IInterface;
  SqlErrorInfo: ISQLErrorInfo;
  Errors: TOleDbErrorDynArray;
  E: EOleDbError;
  SErrorMessage: string;
begin
  if Succeeded(Result) then
    Exit;
  if not Succeeded(Instance.QueryInterface(ISupportErrorInfo, SupportErrorInfo)) or
    not Succeeded(SupportErrorInfo.InterfaceSupportsErrorInfo(IID)) or
    not Succeeded(GetErrorInfo(0, ErrorInfo)) then
    OleCheck(Result);

  if Succeeded(ErrorInfo.QueryInterface(IID_IErrorRecords, ErrorRecords)) and
    Succeeded(ErrorRecords.GetRecordCount(ErrorCount)) then
    SetLength(Errors, ErrorCount + 1)
  else
    SetLength(Errors, 1);

  Errors[0].BasicInfo.hrError := Result;
  Errors[0].BasicInfo.iid := IID;
  ErrorInfo.GetGUID(Errors[0].Guid);
  ErrorInfo.GetSource(Errors[0].Source);
  ErrorInfo.GetDescription(Errors[0].Description);
  ErrorInfo.GetHelpFile(Errors[0].HelpFile);
  ErrorInfo.GetHelpContext(Errors[0].HelpContext);

  for I := 0 to ErrorCount - 1 do
  begin
    ErrorRecords.GetBasicErrorInfo(I, @Errors[I + 1].BasicInfo);
    if Succeeded(ErrorRecords.GetErrorInfo(I, LOCALE_SYSTEM_DEFAULT, ErrorInfo)) then
    begin
      ErrorInfo.GetGUID(Errors[I + 1].Guid);
      ErrorInfo.GetSource(Errors[I + 1].Source);
      ErrorInfo.GetDescription(Errors[I + 1].Description);
      ErrorInfo.GetHelpFile(Errors[I + 1].HelpFile);
      ErrorInfo.GetHelpContext(Errors[I + 1].HelpContext);
    end;
    if Succeeded(ErrorRecords.GetCustomErrorObject(I, IID_ISQLErrorInfo, SqlErrorInfoIntf)) and
      Supports(SqlErrorInfoIntf, IID_ISQLErrorInfo, SqlErrorInfo) then
      SqlErrorInfo.GetSQLInfo(Errors[I + 1].SqlState, Errors[I + 1].SqlNativeError);
  end;

  SErrorMessage := '';
  for I := 0 to StatusCount - 1 do
  begin
    if SErrorMessage <> '' then
      SErrorMessage := SErrorMessage + ', ';
    SErrorMessage := SErrorMessage + IntToStr(StatusCodes^[I]);
  end;

  if SErrorMessage = '' then
    SErrorMessage := Errors[0].Description
  else
    SErrorMessage := Format('%s (status codes: %s)', [Errors[0].Description, SErrorMessage]);

  E := EOleDbError.Create(SErrorMessage, Errors[0].BasicInfo.hrError, Errors[0].HelpContext);
  E.Errors := Errors;
  raise E;
end;

function OpenFastLoad(const DBCreateSession: IDBCreateSession; const TableName: WideString): IRowsetFastLoad; overload;
var
  OpenRowSet: IOpenRowset;
  TableID: TDBID;
begin
  OleDbCheck(DBCreateSession.CreateSession(nil, IID_IOpenRowset, IUnknown(OpenRowSet)), DBCreateSession,
    IID_IDBCreateSession, nil, 0);
  TableID.eKind := DBKIND_NAME;
  TableID.uName.pwszName := PWideChar(TableName);
  OleDbCheck(OpenRowSet.OpenRowset(nil, @TableID, nil, IID_IRowsetFastLoad, 0, nil, @Result), OpenRowSet,
    IID_IOpenRowset, nil, 0);
end;

function OpenFastLoad(Connection: TADOConnection; const TableName: WideString): IRowsetFastLoad; overload;
var
  ConnectionConstruction: ADOConnectionConstruction;
begin
  SetProperty(Connection, DBPROPSET_SQLSERVERDATASOURCE, SSPROP_ENABLEFASTLOAD, True);
  ConnectionConstruction := Connection.ConnectionObject as ADOConnectionConstruction;
  Result := OpenFastLoad(ConnectionConstruction.Get_DSO as IDBCreateSession, TableName);
end;

procedure SetProperties(const DBProperties: IDBProperties; const PropertySetID: TGUID;
  const PropertyIDs: array of Cardinal; const Values: OleVariant); overload;
var
  Count, I: Integer;
  DBPropSets: array[0..0] of TDBPropSet;
  DBProps: PDBPropArray;
begin
  Count := Length(PropertyIDs);
  if Count = 0 then
    Exit;

  DBProps := AllocMem(Count * SizeOf(TDBProp));
  try
    DBPropSets[0].rgProperties := DbProps;
    DBPropSets[0].cProperties := Count;
    DBPropSets[0].guidPropertySet := PropertySetID;

    for I := 0 to Count - 1 do
      InitializeProperty(DBProps^[I], PropertyIDs[I], VarArrayGet(Values, [I]));
    OleCheck(DBProperties.SetProperties(1, @DBPropSets));
  finally
    FreeMem(DBProps);
  end;
end;

procedure SetProperties(Connection: TADOConnection; const PropertySetID: TGUID; const PropertyIDs: array of Cardinal;
  const Values: OleVariant); overload;
var
  ConnectionConstruction: ADOConnectionConstruction;
begin
  ConnectionConstruction := Connection.ConnectionObject as ADOConnectionConstruction;
  SetProperties(ConnectionConstruction.Get_DSO as IDBProperties, PropertySetID, PropertyIDs, Values);
end;

procedure SetProperty(const DBProperties: IDBProperties; const PropertySetID: TGUID; PropertyID: Cardinal;
  const Value: OleVariant); overload;
var
  DBPropSets: array[0..0] of TDBPropSet;
  DBProps: array[0..0] of TDBProp;
begin
  DBPropSets[0].rgProperties := @DBProps;
  DBPropSets[0].cProperties := 1;
  DBPropSets[0].guidPropertySet := PropertySetID;
  InitializeProperty(DBProps[0], PropertyID, Value);
  OleCheck(DBProperties.SetProperties(1, @DBPropSets));
end;

procedure SetProperty(Connection: TADOConnection; const PropertySetID: TGUID; PropertyID: Cardinal;
  const Value: OleVariant); overload;
var
  ConnectionConstruction: ADOConnectionConstruction;
begin
  ConnectionConstruction := Connection.ConnectionObject as ADOConnectionConstruction;
  SetProperty(ConnectionConstruction.Get_DSO as IDBProperties, PropertySetID, PropertyID, Value);
end;

{ TBulkLoadFieldString }

constructor TBulkLoadFieldString.Create(const AString: string);
begin
  inherited Create;
  FString := AString;
end;

procedure TBulkLoadFieldString.FillColumn(AColumn: PColumnData);
var
  LDest: Pointer;
begin
  LDest := @AColumn^.Data[0];
  // AColumn^.Length must contain maximum field length, see GetFieldValue
  // copy "field length" bytes
  // even string is shorter it's ok
  Move(PChar(FString)^, LDest^, AColumn^.Length);
  inherited FillColumn(AColumn);
end;

procedure TBulkLoadFieldString.SetColumnSize(AColumn: PColumnData);
begin
  // AColumn^.Length must contain maximum field length, see GetFieldValue
  if Length(FString) < AColumn^.Length then
    AColumn^.Length := Length(FString);
end;

{ TBulkLoadFieldCurrency }

constructor TBulkLoadFieldCurrency.Create(const ACurrency: Currency);
begin
  inherited Create;
  FCurrency := ACurrency;
end;

procedure TBulkLoadFieldCurrency.FillColumn(AColumn: PColumnData);
begin
  PCurrency(@AColumn.Data[0])^ := FCurrency;
  inherited FillColumn(AColumn);
end;

procedure TBulkLoadFieldCurrency.SetColumnSize(AColumn: PColumnData);
begin
  AColumn^.Length := SizeOf(FCurrency);
end;

{ TMSSQLBulkLoader }

function TMSSQLBulkLoader.Append(AField: IBulkLoadField): IBulkLoader;
begin
  GetFieldValue(AField, FBindings^[FAppendedFieldCount], FBuffer);
  Inc(FAppendedFieldCount);
  Result := Self;
end;

function TMSSQLBulkLoader.AppendRow: IBulkLoader;
begin
  ResetAppendedFieldCount;
  OleDbCheck(FFastLoad.InsertRow(FAccessorHandle, FBuffer), FFastLoad,
    IID_IRowsetFastLoad, FStatusCodes, FFieldCount);
  Result := Self;
end;

procedure TMSSQLBulkLoader.BeforeDestruction;
begin
  if Assigned(FAccessor) then
    OleDbCheck(FAccessor.ReleaseAccessor(FAccessorHandle, nil), FAccessor, IID_IAccessor, nil, 0);
  FreeMem(FStatusCodes);
  FreeMem(FBuffer);
  FreeMem(FBindings);
  FreeAndNil(FConnection);
  inherited;
end;

procedure TMSSQLBulkLoader.Commit;
begin
  OleDbCheck(FFastLoad.Commit(True), FFastLoad, IID_IRowsetFastLoad,
    FStatusCodes, FFieldCount);
end;

constructor TMSSQLBulkLoader.Create(AConnection: TADOConnection; const
  ADstTableName: string; AFields: IBulkLoadFieldDefs);
var
  LBufferSize: Integer;
  i: Integer;
begin
  inherited Create;
  FConnection := AConnection;
  FFieldCount := AFields.Count;
  ResetAppendedFieldCount;
  FConnection.Open;
  try
    FFastLoad := OpenFastLoad(FConnection, ADstTableName);
  except
    FreeAndNil(FConnection);
    raise;
  end;
  FStatusCodes := AllocMem(FFieldCount * SizeOf(DBBINDSTATUS));

  LBufferSize := 0;
  FBindings := AllocMem(FFieldCount * SizeOf(TDBBinding));
  for i := 0 to FFieldCount - 1 do
    LBufferSize := InitializeBinding(AFields.FieldDefs[i], FBindings^[i], LBufferSize);

  FBuffer := AllocMem(LBufferSize);

  OleDbCheck(FFastLoad.QueryInterface(IID_IAccessor, FAccessor), FFastLoad, IID_IRowsetFastLoad, FStatusCodes,
    FFieldCount);
  OleDbCheck(FAccessor.CreateAccessor(DBACCESSOR_ROWDATA, FFieldCount, FBindings, LBufferSize,
    FAccessorHandle, FStatusCodes), FAccessor, IID_IAccessor, FStatusCodes, FFieldCount);
end;

procedure TMSSQLBulkLoader.ResetAppendedFieldCount;
begin
  FAppendedFieldCount := 0;
end;

{ TBulkLoadFieldDef }

constructor TBulkLoadFieldDef.Create(const ADataType: TFieldType;
  ADataSize: Integer);
begin
  inherited Create;
  FDataType := ADataType;
  FDataSize := ADataSize;
end;

function TBulkLoadFieldDef.DataSize: Integer;
begin
  Result := FDataSize;
end;

function TBulkLoadFieldDef.DataType: TFieldType;
begin
  Result := FDataType;
end;

function TBulkLoadFieldDef.FieldNo: Integer;
begin
  Result := FFieldNo;
end;

procedure TBulkLoadFieldDef.SetBindingMaxLen(var ABinding: TDBBinding);
begin
  ABinding.cbMaxLen := DataSize;
end;

procedure TBulkLoadFieldDef.SetFieldNo(const AFieldNo: Integer);
begin
  FFieldNo := AFieldNo;
end;

{ TBulkLoadFieldDefs }

function TBulkLoadFieldDefs.AddFieldDef(
  ABulkLoadFieldDef: IBulkLoadFieldDef): IBulkLoadFieldDefs;
var
  LAddedIndex: Integer;
begin
  LAddedIndex := Add(ABulkLoadFieldDef);
  GetFields(LAddedIndex).SetFieldNo(LAddedIndex + 1);
  Result := Self;
end;

function TBulkLoadFieldDefs.GetFields(Index: Integer): IBulkLoadFieldDef;
begin
  Result := Items[Index] as IBulkLoadFieldDef;
end;

{ TBulkLoadFieldDefString }

constructor TBulkLoadFieldDefString.Create(const ALength: Integer);
begin
  inherited Create(ftString, ALength);
end;

{ TBulkLoadFieldDefDateTime }

constructor TBulkLoadFieldDefDateTime.Create;
begin
  inherited Create(ftDateTime, SizeOf(TDBTimeStamp));
end;

{ TBulkLoadFieldDefCurrency }

constructor TBulkLoadFieldDefCurrency.Create;
begin
  inherited Create(ftCurrency, SizeOf(Currency));
end;

{ TBulkLoadField }

procedure TBulkLoadField.FillColumn(AColumn: PColumnData);
begin
  AColumn^.Status := DBSTATUS_S_OK;
  SetColumnSize(AColumn);
end;

procedure TBulkLoadField.SetColumnSize(AColumn: PColumnData);
begin
  AColumn^.Length := 0;
end;

{ TBulkLoadFieldNull }

procedure TBulkLoadFieldNull.FillColumn(AColumn: PColumnData);
begin
  AColumn^.Status := DBSTATUS_S_ISNULL;
  SetColumnSize(AColumn);
end;

{ TBulkLoadFieldDefInteger }

constructor TBulkLoadFieldDefInteger.Create;
begin
  inherited Create(ftInteger, SizeOf(Integer));
end;

{ TBulkLoadFieldInteger }

constructor TBulkLoadFieldInteger.Create(const AInteger: Integer);
begin
  inherited Create;
  FInteger := AInteger;
end;

procedure TBulkLoadFieldInteger.FillColumn(AColumn: PColumnData);
begin
  PInteger(@AColumn.Data[0])^ := FInteger;
  inherited FillColumn(AColumn);
end;

procedure TBulkLoadFieldInteger.SetColumnSize(AColumn: PColumnData);
begin
  AColumn^.Length := SizeOf(FInteger);
end;

{ TBulkLoadFieldDefInt64 }

constructor TBulkLoadFieldDefInt64.Create;
begin
  inherited Create(ftLargeint, SizeOf(Int64));
end;

{ TBulkLoadFieldInt64 }

constructor TBulkLoadFieldInt64.Create(const AInt64: Int64);
begin
  inherited Create;
  FInt64 := AInt64;
end;

procedure TBulkLoadFieldInt64.FillColumn(AColumn: PColumnData);
begin
  PInt64(@AColumn.Data[0])^ := FInt64;
  inherited FillColumn(AColumn);
end;

procedure TBulkLoadFieldInt64.SetColumnSize(AColumn: PColumnData);
begin
  AColumn^.Length := SizeOf(FInt64);
end;

end.
