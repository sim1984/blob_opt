program BlobOpt;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Classes, SysUtils, CustApp, IB, IBDatabase, IbSql, Math
  {$ifdef WINDOWS}
  , Windows
  {$endif}
  ;

const
  MAX_SEGMENT_SIZE = 65535;

type

  { TBlobOptApp }

  TBlobOptApp = class(TCustomApplication)
  private
    FDatabaseName: string;
    FUser: string;
    FPassword: string;
    FRole: string;
    FCharsetName: string;
    FTableName: string;
    FBlobFieldName: string;
    FKeyFieldName: string;
    FDatabase: TIBDatabase;
    FWhereFilter: string;
    FRowsLimit: Integer;
    FReadStatFlag: Boolean;
    FSelectFileName: string;
    FModifyFileName: string;
    FBlobType: TBlobType;
    FSegmentSize: Integer;
  protected
    procedure DoRun; override;
    procedure ConnectDB;
    function BuildSelectSql: string;
    function BuildModifySql: string;
    function ReadBlobStat(ABlob: IBlob): Double;
    function ConvertBlob(ABlob: IBlob; AWriteTransaction: ITransaction): IBlob;
  public
    constructor Create(TheOwner: TComponent); override;
    destructor Destroy; override;
    procedure WriteHelp; virtual;
    procedure Analyze;
    procedure Optimize;
  end;

{ TBlobOptApp }

procedure TBlobOptApp.DoRun;
var
  ErrorMsg: String;
  xBlobType: string;
begin
  CaseSensitiveOptions := False;
  // quick check parameters
  ErrorMsg:=CheckOptions('h d: a o', [
    'help',
    'database:',
    'user:',
    'password:',
    'role:',
    'charset:',
    'table:',
    'blobfield:',
    'keyfield:',
    'filter:',
    'rows:',
    'analyze',
    'optimize',
    'readstat',
    'sqlSelectFile:',
    'sqlModifyFile:',
    'blobType:',
    'segmentSize:'
  ]);
  if ErrorMsg<>'' then
  begin
    ShowException(Exception.Create(ErrorMsg));
    Terminate;
    Exit;
  end;

  // parse parameters
  if HasOption('h', 'help') or (ParamCount = 0) then
  begin
    WriteHelp;
    Terminate;
    Exit;
  end;

  FDatabaseName := GetOptionValue('d', 'database');
  FUser := GetOptionValue('user');
  FPassword := GetOptionValue('password');
  FCharsetName := GetOptionValue('charset');
  FRole := GetOptionValue('role');
  FTableName := GetOptionValue('table');
  FBlobFieldName := GetOptionValue('blobfield');
  FKeyFieldName := GetOptionValue('keyfield');
  FWhereFilter := GetOptionValue('filter');
  FSelectFileName := GetOptionValue('sqlSelectFile');
  FModifyFileName := GetOptionValue('sqlModifyFile');
  FRowsLimit := StrToIntDef(GetOptionValue('rows'), -1);
  FReadStatFlag := HasOption('readstat');
  xBlobType :=  Trim(GetOptionValue('blobType')).ToLower;
  FSegmentSize := StrToIntDef(GetOptionValue('segmentSize'), 65535);

  if (FSegmentSize < 1) or (FSegmentSize > MAX_SEGMENT_SIZE) then
     raise Exception.Create('Invalid segment size');

  if (xBlobType = '') or (xBlobType = 'segmented') then
    FBlobType := btSegmented
  else if (xBlobType = 'stream') then
    FBlobType := btStream
  else raise Exception.Create('Invalid blob type');

  if (Trim(FKeyFieldName) = '') then
    FKeyFieldName := 'DB_KEY';

  if HasOption('a', 'analyze') then
  begin
    Analyze;
    Terminate;
    Exit;
  end;

  if HasOption('o', 'optimize') then
  begin
    Optimize;
    Terminate;
    Exit;
  end;

  Terminate;
end;

procedure TBlobOptApp.ConnectDB;
begin
  FDatabase.DatabaseName := FDatabaseName;
  FDatabase.LoginPrompt := False;
  FDatabase.Params.Values['user_name'] := FUser;
  if (FPassword <> '') then
    FDatabase.Params.Values['password'] := FPassword;
  if (FCharsetName <> '') then
    FDatabase.Params.Values['lc_ctype'] := FCharsetName;
  if (FRole <> '') then
    FDatabase.Params.Values['role_name'] := FRole;
  FDatabase.Open;
end;

function TBlobOptApp.BuildSelectSql: string;
begin
  Result := 'select ';
  if (Trim(FBlobFieldName) = '') then
    raise Exception.Create('Blob field name not initialize');
  Result := Result + Trim(FBlobFieldName);
  if (Trim(FKeyFieldName) = '') or (Trim(FKeyFieldName) = 'DB_KEY') then
    Result := Result + ', rdb$db_key as db_key'
  else
    Result := Result + ', ' + Trim(FKeyFieldName);
  Result := Result + lineEnding;
  Result := Result + 'from ';
  if (Trim(FTableName) = '') then
    raise Exception.Create('Table name not initialize');
  Result := Result + Trim(FTableName);
  if (Trim(FWhereFilter) <> '') then
  begin
    Result := Result + lineEnding;
    Result := Result + 'where ' + Trim(FWhereFilter);
  end;
  if FRowsLimit > -1 then
  begin
    Result := Result + lineEnding;
    Result := Result + Format('rows %d', [FRowsLimit]);
  end;
end;

function TBlobOptApp.BuildModifySql: string;
begin
  Result := 'update ';
  if (Trim(FTableName) = '') then
    raise Exception.Create('Table name not initialize');
  Result := Result + Trim(FTableName);
  Result := Result + #13;
  Result := Result + 'set ';
  if (Trim(FBlobFieldName) = '') then
    raise Exception.Create('Blob field name not initialize');
  Result := Result + Trim(FBlobFieldName) + ' = :' + Trim(FBlobFieldName);
  Result := Result + #13;
  Result := Result + 'where ' + Trim(FWhereFilter);

  if (Trim(FKeyFieldName) = '') or (Trim(FKeyFieldName) = 'DB_KEY') then
    Result := Result + 'rdb$db_key = :db_key'
  else
    Result := Result + Trim(FKeyFieldName) + ' = :' + Trim(FKeyFieldName);
end;

function TBlobOptApp.ReadBlobStat(ABlob: IBlob): Double;
var
  xBuffer: array[0.. 2 * MAX_SEGMENT_SIZE + 1] of Byte;
  xReadSize: Longint;
  iCounterPerSec: Int64;
  T1, T2: Int64;
begin
  QueryPerformanceFrequency(iCounterPerSec); // determined the counter frequency
  QueryPerformanceCounter(T1); // timed the start of the operation

  xReadSize := ABlob.Read(xBuffer, MAX_SEGMENT_SIZE);
  while (xReadSize > 0) do
  begin
    xReadSize := ABlob.Read(xBuffer, MAX_SEGMENT_SIZE);
  end;

  QueryPerformanceCounter(T2); // timed the end

  Result := 1000.0 * (T2 - T1) / iCounterPerSec;
end;

function TBlobOptApp.ConvertBlob(ABlob: IBlob; AWriteTransaction: ITransaction): IBlob;
var
  xBPB: IBPB;
  xReadSize: Longint;
  xWriteSize: Longint;
  xWriteCounter: Longint;
  xBuffer: array[0.. MAX_SEGMENT_SIZE] of Byte;
begin
  xBPB := nil;
  if (FBlobType = btStream) then
  begin
    xBPB := FDatabase.Attachment.AllocateBPB;
    xBPB.Add(isc_bpb_type).AsByte:=isc_bpb_type_stream;
  end;
  Result := FDatabase.Attachment.CreateBlob(AWriteTransaction, ABlob.GetSubType, ABlob.GetCharsetId,  xBPB);
  xReadSize := ABlob.Read(xBuffer, MAX_SEGMENT_SIZE);
  while (xReadSize > 0) do
  begin
    if FBlobType = btStream then
    begin
      // the stream blob is rewritten with the same size as read
      Result.Write(xBuffer, xReadSize);
    end
    else
    begin
      // segmented blob rewrite with new segment size
      xWriteSize := 0;
      xWriteCounter := 0;
      while (xReadSize > xWriteCounter) do
      begin
        xWriteSize := Result.Write((@xBuffer[0] + xWriteCounter)^, Min(xReadSize - xWriteCounter, FSegmentSize));
        xWriteCounter := xWriteCounter +  xWriteSize;
      end;
    end;
    // continue reading blob
    xReadSize := ABlob.Read(xBuffer, MAX_SEGMENT_SIZE);
  end;
end;

constructor TBlobOptApp.Create(TheOwner: TComponent);
begin
  inherited Create(TheOwner);
  FDatabase := TIBDatabase.Create(Self);
  FRowsLimit := -1;
  StopOnException:=True;
end;

destructor TBlobOptApp.Destroy;
begin
  inherited Destroy;
end;

procedure TBlobOptApp.WriteHelp;
begin
  writeln('Usage: ', ExeName, ' <options>');
  writeln;
  writeln('Commands:');
  writeln('  -h or --help - help for usage blobopt util');
  writeln('  -a or --analyze - analyze blob field');
  writeln('  -o or --optimize - optimize blob field');
  writeln;
  writeln('Common options:');
  writeln('  -d <database> or --database=<database> - database connection string');
  writeln('  --user=<username> - database user name');
  writeln('  --password=<password> - database password');
  writeln('  --charset=<charset> - connection character set');
  writeln('  --role=<role> - database role name');
  writeln('  --table=<tablename> - the table in which the blob is analyzed or optimized. Used only if sqlSelectFile or sqlModifyFile options are not specified');
  writeln('  --keyfield=<keyfield> - name of the key field, if not specified, then rdb$db_key is used');
  writeln('  --blobfield=<blobfield> - name of the blob field');
  writeln('  --filter=<filter> - WHERE filter for auto build select sql query. Not used when sqlSelectFile is not specified');
  writeln('  --rows=<rows> - limiter ROWS for auto build select sql query. Not used when sqlSelectFile is not specified');
  writeln('  --sqlSelectFile=<filename> - name of the file containing the select query to analyze or optimize the BLOB field');
  writeln;
  writeln('Options for analyze:');
  writeln('  --readstat - blob field read statistics');
  writeln;
  writeln('Options for optimize:');
  writeln('  --blobType=<blobtype> - convert ot segmented or stream blob');
  writeln('  --maxSegmentSize=<segement_size> - maximum segment size for segmented blobs');
  writeln('  --sqlModifyFile=<filename> - name of the file containing the update query to optimize the BLOB field');
end;

procedure TBlobOptApp.Analyze;
const
  sBlobTypes: array[btSegmented .. btStream] of string = ('Segmented', 'Stream');
var
  xReadBlob: IBlob;
  xNumSegments: Int64;
  xMaxSegmentSize, xTotalSize: Int64;
  xBlobType: TBlobType;
  xReadTransaction: TIbTransaction;
  xSelectQuery: TIbSql;
  xKeyField, xBlobField: ISQLData;
  id: Int64;
  idStr: string;
  ca: TCharArray;
  cb: TByteArray absolute ca;
  c: Byte;
  idBinary: string;
  xReadTime: Double;
  stat: string;
begin
  ConnectDB;
  xReadTransaction := TIbTransaction.Create(Self);
  xReadTransaction.DefaultDatabase := FDatabase;
  xSelectQuery := TIbSql.create(Self);
  xSelectQuery.Transaction := xReadTransaction;
  try
    if (FSelectFileName <> '') then
      xSelectQuery.SQL.LoadFromFile(FSelectFileName)
    else
       xSelectQuery.SQL.Text := BuildSelectSql;
    Writeln;
    Writeln('Start analyze');
    xReadTransaction.StartTransaction;
    xSelectQuery.ExecQuery;
    if xSelectQuery.Eof then
    begin
      Writeln;
      Writeln('Finish analyze');
      Exit;
    end;
    xKeyField := xSelectQuery.FieldByName(FKeyFieldName);
    if xKeyField = nil then
      raise Exception.CreateFmt('Field %s not found', [FKeyFieldName]);

    xBlobField := xSelectQuery.FieldByName(FBlobFieldName);
    if xBlobField = nil then
      raise Exception.CreateFmt('Field %s not found', [FBlobFieldName]);

    while not xSelectQuery.Eof do
    begin
      if (xBlobField.IsNull) then
      begin
        xSelectQuery.Next;
        continue;
      end;

      case xKeyField.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          id := xKeyField.AsInt64;
        else begin
          idStr := xKeyField.AsString;
        end;
      end;

      xReadBlob := xBlobField.GetAsBlob;
      xReadBlob.GetInfo(xNumSegments, xMaxSegmentSize, xTotalSize, xBlobType);
      Writeln;
      stat := '';
      case xKeyField.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
        begin
          stat := Format('Key %s=%d;', [FKeyFieldName, id]);
        end
        else if (FKeyFieldName = 'DB_KEY') then
        begin
          ca := idStr.ToCharArray;
          idBinary := '';
          for c in cb do
            idBinary := idBinary + IntToHex(c, 2);
          stat := Format('Key DB_KEY=x''%s'';', [idBinary]);
        end
        else
          stat := Format('Key %s="%s";', [FKeyFieldName, idStr]);
      end;
      if (FReadStatFlag) then
      begin
        xReadTime := ReadBlobStat(xReadBlob);
        stat := stat + ' ' +  Format('ReadTime: %8.3f ms;', [xReadTime]);
      end;
      Writeln(stat);
      Writeln(Format(
        'NumSegments: %d; MaxSegmentSize: %d; TotalSize: %d; BlobType: %s;',
        [
          xNumSegments,
          xMaxSegmentSize,
          xTotalSize,
          sBlobTypes[xBlobType]
        ]
      ));
      xReadBlob.Close;
      xSelectQuery.Next;
    end;
    xSelectQuery.Close;
  finally
    xSelectQuery.Free;
    xReadTransaction.Rollback;
    xReadTransaction.Free;
  end;
  Writeln;
  Writeln('Finish analyze');
end;

procedure TBlobOptApp.Optimize;
var
  xReadTransaction: TIbTransaction;
  xWriteTransaction: TIbTransaction;
  xSelectQuery: TIbSql;
  xModifyQuery: TIbSql;
  xKeyField, xBlobField: ISQLData;
  xKeyParam, xBlobParam: ISQLParam;
  id: Int64;
  idStr: string;
  idBinary: string;
  xNumSegments: Int64;
  xMaxSegmentSize, xTotalSize: Int64;
  xBlobType: TBlobType;
  xReadBlob: IBlob;
  xWriteBlob: IBlob;
  xHexPrefix: string;
  ca: TCharArray;
  cb: TByteArray absolute ca;
  c: Byte;
begin
  ConnectDB;
  xReadTransaction := TIbTransaction.Create(Self);
  xReadTransaction.DefaultDatabase := FDatabase;
  xWriteTransaction := TIbTransaction.Create(Self);
  xWriteTransaction.DefaultDatabase := FDatabase;
  xSelectQuery := TIbSql.create(Self);
  xSelectQuery.Transaction := xReadTransaction;
  xModifyQuery := TIbSql.create(Self);
  xModifyQuery.Transaction := xWriteTransaction;
  try
    if (FSelectFileName <> '') then
      xSelectQuery.SQL.LoadFromFile(FSelectFileName)
    else
      xSelectQuery.SQL.Text := BuildSelectSql;

    if (FModifyFileName <> '') then
      xModifyQuery.SQL.LoadFromFile(FModifyFileName)
    else
      xModifyQuery.SQL.Text := BuildModifySql;

    Writeln;
    Writeln('Start optimize');
    xReadTransaction.StartTransaction;
    xWriteTransaction.StartTransaction;

    xModifyQuery.Prepare;
    xKeyParam := xModifyQuery.ParamByName(FKeyFieldName);
    if xKeyParam = nil then
      raise Exception.CreateFmt('Parameter %s not found', [FKeyFieldName]);

    xBlobParam := xModifyQuery.ParamByName(FBlobFieldName);
    if xBlobParam = nil then
      raise Exception.CreateFmt('Parameter %s not found', [FBlobFieldName]);

    xSelectQuery.ExecQuery;

    if (xSelectQuery.Eof) then
    begin
      xReadTransaction.Commit;
      xWriteTransaction.Rollback;

      Writeln;
      Writeln('Finish optimize');
      Exit;
    end;

    xKeyField := xSelectQuery.FieldByName(FKeyFieldName);
    if xKeyField = nil then
      raise Exception.CreateFmt('Field %s not found', [FKeyFieldName]);

    xBlobField := xSelectQuery.FieldByName(FBlobFieldName);
    if xBlobField = nil then
      raise Exception.CreateFmt('Field %s not found', [FBlobFieldName]);

    while not xSelectQuery.Eof do
    begin
      case xKeyField.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          id := xKeyField.AsInt64;
        else
          idStr := xKeyField.AsString;
      end;

      if (xBlobField.IsNull) then
      begin
        xSelectQuery.Next;
        continue;
      end;

      xReadBlob := xBlobField.GetAsBlob;
      xReadBlob.GetInfo(xNumSegments, xMaxSegmentSize, xTotalSize, xBlobType);
      if (xBlobType = FBlobType) and
         ((FBlobType = btStream) or ((FBlobType = btSegmented) and (xNumSegments = 1))) then
      begin
        // streaming blob does not need to be converted to streaming
        // a segmented blob with one segment does not need to be re-segmented
        xReadBlob.Close;
        case xKeyField.GetSQLType of
          SQL_INT64, SQL_LONG, SQL_SHORT:
          begin
            Writeln(Format('Key %s=%d. No change blob.', [FKeyFieldName, id]));
          end
          else
          begin
            if (FKeyFieldName = 'DB_KEY') then
            begin
              xHexPrefix := 'x';
              ca := idStr.ToCharArray;
              idBinary := '';
              for c in cb do
                idBinary := idBinary + IntToHex(c, 2);
            end
            else
              idBinary := idStr;
            Writeln(Format('Key %s=%s''%s''. No change blob.', [FKeyFieldName, xHexPrefix, idBinary]));
          end
        end;
        xSelectQuery.Next;
        continue;
      end;
      xWriteBlob := ConvertBlob(xReadBlob, xWriteTransaction.TransactionIntf);
      xReadBlob.Close;

      case xKeyParam.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          xKeyParam.AsInt64 := id;
        else
          xKeyParam.AsString := idStr;
      end;

      xBlobParam.AsBlob := xWriteBlob;
      xModifyQuery.ExecQuery;
      xWriteBlob.Cancel;
      xSelectQuery.Next;
      case xKeyField.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          if (xBlobType = btSegmented) and (FBlobType = btStream) then
          begin
            Writeln(Format('Key %s=%d. Convert segemented to streamed blob.', [FKeyFieldName, id]));
          end
          else if (xBlobType = btStream) and (FBlobType = btSegmented) then
          begin
            Writeln(Format('Key %s=%d. Convert streamed to segemented blob with max segment size %d', [FKeyFieldName, id, FSegmentSize]));
          end
          else
          begin
            Writeln(Format('Key %s=%d. Rewrite segmented blob with max segment size %d', [FKeyFieldName, id, FSegmentSize]));
          end
        else
        begin
          if (FKeyFieldName = 'DB_KEY') then
          begin
            xHexPrefix := 'x';
            ca := idStr.ToCharArray;
            idBinary := '';
            for c in cb do
              idBinary := idBinary + IntToHex(c, 2);
          end
          else
            idBinary := idStr;
          if (xBlobType = btSegmented) and (FBlobType = btStream) then
          begin
            Writeln(Format('Key %s=%s''%s''. Convert segemented to streamed blob.', [FKeyFieldName, xHexPrefix, idBinary]));
          end
          else if (xBlobType = btStream) and (FBlobType = btSegmented) then
          begin
            Writeln(Format('Key %s=%s''%s''. Convert streamed to segemented blob with max segment size %d', [FKeyFieldName, xHexPrefix, idBinary, FSegmentSize]));
          end
          else
          begin
            Writeln(Format('Key %s=%s''%s''. Rewrite segmented blob with max segment size %d', [FKeyFieldName, xHexPrefix, idBinary, FSegmentSize]));
          end
        end;
      end;
    end;

    xWriteTransaction.Commit;
  finally
    xModifyQuery.Free;
    xSelectQuery.Free;
    if xWriteTransaction.InTransaction then
      xWriteTransaction.Rollback;
    xReadTransaction.Rollback;
    xReadTransaction.Free;
    xWriteTransaction.Free;
  end;
  Writeln;
  Writeln('Finish optimize');
end;

var
  Application: TBlobOptApp;
begin
  Application:=TBlobOptApp.Create(nil);
  Application.Title:='Blob optimize';
  Application.Run;
  Application.Free;
end.

