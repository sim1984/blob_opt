unit main;

{$mode objfpc}{$H+}
{.$define blobopt}


interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls, Spin,
  JSONPropStorage, ExtCtrls, EditBtn, IBDatabase, Ib, IBSQL, eventlog
  {$ifdef WINDOWS}
  , Windows
  {$endif}
  ;

type

  { TMainForm }

  TMainForm = class(TForm)
    btnOptimize: TButton;
    btnSave: TButton;
    btnStat: TButton;
    cbxReadTimeStat: TCheckBox;
    cbxAutoBuildSql: TCheckBox;
    Database: TIBDatabase;
    edtLogDirectory: TDirectoryEdit;
    edtBLOBFieldName: TEdit;
    edtPKFieldName: TEdit;
    edtRows: TSpinEdit;
    edtTableName: TEdit;
    edtUser: TEdit;
    edtPassword: TEdit;
    edtCharset: TEdit;
    edtDatabase: TEdit;
    edtWhereFilter: TLabeledEdit;
    edtSelectSqlFilename: TFileNameEdit;
    edtModifySqlFilename: TFileNameEdit;
    EventLog: TEventLog;
    GroupBox1: TGroupBox;
    JSONPropStorage: TJSONPropStorage;
    Label1: TLabel;
    Label10: TLabel;
    lblModifySqlFilename: TLabel;
    Label12: TLabel;
    Label2: TLabel;
    lblSegmentSize: TLabel;
    Label4: TLabel;
    Label5: TLabel;
    Label6: TLabel;
    Label7: TLabel;
    Label8: TLabel;
    Label9: TLabel;
    mmLog: TMemo;
    qryWrite: TIBSQL;
    qryRead: TIBSQL;
    edtSegmentSize: TSpinEdit;
    rbBlobType: TRadioGroup;
    trRead: TIBTransaction;
    trWrite: TIBTransaction;
    procedure btnSaveClick(Sender: TObject);
    procedure btnOptimizeClick(Sender: TObject);
    procedure btnStatClick(Sender: TObject);
    procedure cbxAutoBuildSqlChange(Sender: TObject);
    procedure cbxReadTimeStatChange(Sender: TObject);
    procedure edtBLOBFieldNameChange(Sender: TObject);
    procedure edtCharsetChange(Sender: TObject);
    procedure edtDatabaseChange(Sender: TObject);
    procedure edtLogDirectoryClick(Sender: TObject);
    procedure edtModifySqlFilenameChange(Sender: TObject);
    procedure edtPasswordChange(Sender: TObject);
    procedure edtPKFieldNameChange(Sender: TObject);
    procedure edtRowsChange(Sender: TObject);
    procedure edtSegmentSizeChange(Sender: TObject);
    procedure edtSelectSqlFilenameChange(Sender: TObject);
    procedure edtTableNameChange(Sender: TObject);
    procedure edtUserChange(Sender: TObject);
    procedure edtWhereFilterChange(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure rbBlobTypeClick(Sender: TObject);
  private
    FSegmentSize: Integer;
    FRowsLimit: Integer;
    FBlobType: TBlobType;
    FReadStatFlag: Boolean;
    FAutoBuildSql: Boolean;
    FBlobFieldName: string;
    FPKFieldName: string;
    FTableName: string;
    FWhereFilter: string;
    FModifySqlFilename: string;
    FSelectSqlFilename: string;
    FLogDirectory: string;

    function GetAppDir: string;
    function ReadBlobStat(ABlob: IBlob): Double;
    function BuildSelectSql: string;
    function BuildModifySql: string;
  protected
    procedure ReadSettings;
    function ConvertBlob(ABlob: IBlob): IBlob;
    procedure EnabledControls;
    procedure DisabledControls;
  public
    property AppDir: string read GetAppDir;
  end;

const
  MAX_SEGMENT_SIZE = 65535;

var
  MainForm: TMainForm;

implementation

{$R *.lfm}

{ TMainForm }

procedure TMainForm.btnSaveClick(Sender: TObject);
begin
  JSONPropStorage.WriteString('database_name', Database.DatabaseName);
  JSONPropStorage.WriteStrings('database_params', Database.Params);
  JSONPropStorage.WriteInteger('segment_size', FSegmentSize);
  JSONPropStorage.WriteInteger('blob_type', Integer(FBlobType));
  JSONPropStorage.WriteString('blob_field', FBlobFieldName);
  JSONPropStorage.WriteString('pk_field', FPKFieldName);
  JSONPropStorage.WriteBoolean('time_stat', FReadStatFlag);
  JSONPropStorage.WriteBoolean('auto_build_sql', FAutoBuildSql);
  JSONPropStorage.WriteString('table_name', FTableName);
  JSONPropStorage.WriteInteger('rows_limit', FRowsLimit);
  JSONPropStorage.WriteString('log_dir', FLogDirectory);
  JSONPropStorage.WriteString('select_sql_filename', FSelectSqlFilename);
  JSONPropStorage.WriteString('modify_sql_filename', FModifySqlFilename);
  JSONPropStorage.Save;
end;

procedure TMainForm.btnOptimizeClick(Sender: TObject);
var
  xReadBlob: IBlob;
  xWriteBlob: IBlob;
  id: Int64;
  idStr: string;
  idBinary: string;
  xNumSegments: Int64;
  xMaxSegmentSize, xTotalSize: Int64;
  xBlobType: TBlobType;
  xKeyField, xBlobField: ISQLData;
  xKeyParam, xBlobParam: ISQLParam;
  ca: TCharArray;
  cb: TByteArray absolute ca;
  c: Byte;
  xHexPrefix: string;
begin
  DisabledControls;

  EventLog.FileName := FLogDirectory + DirectorySeparator + 'optimize_' + FormatDateTime('YYYY-MM-DD-hh-nn-ss', Now) + '.log';
  EventLog.Active := True;

  EventLog.Info('Start optimize');
  EventLog.Info('=========================================================');

  mmLog.Lines.Clear;
  mmLog.Lines.Add('Start optimize');
  mmLog.Lines.Add('=========================================================');

  try
    xHexPrefix := '';
    if FAutoBuildSql then
    begin
      qryRead.SQL.Text := BuildSelectSql;
      qryWrite.SQL.Text := BuildModifySql;
    end
    else
    begin
      qryRead.SQL.LoadFromFile(FSelectSqlFileName);
      qryWrite.SQL.LoadFromFile(FModifySqlFileName);
    end;

    Database.Open;
  except
    on E: Exception do
    begin
      Application.ShowException(E);
      EventLog.Error(E.Message);
      EventLog.Active := False;
      EnabledControls;
      Exit;
    end;
  end;

  if (Trim(FPKFieldName) = '') then
    FPKFieldName := 'DB_KEY';

  try
    trRead.StartTransaction;
    trWrite.StartTransaction;

    qryWrite.Prepare;
    xKeyParam := qryWrite.ParamByName(FPKFieldName);
    if xKeyParam = nil then
      raise Exception.CreateFmt('Parameter %s not found', [FPKFieldName]);

    xBlobParam := qryWrite.ParamByName(FBlobFieldName);
    if xBlobParam = nil then
      raise Exception.CreateFmt('Parameter %s not found', [FBlobFieldName]);

    qryRead.ExecQuery;

    if (qryRead.Eof) then
    begin
      trRead.Commit;
      trWrite.Rollback;
      EnabledControls;

      mmLog.Lines.Add('=========================================================');
      mmLog.Lines.Add('Finish optimize');

      EventLog.Info('=========================================================');
      EventLog.Info('Finish optimize');
      EventLog.Active := False;
      Exit;
    end;

    xKeyField := qryRead.FieldByName(FPKFieldName);
    if xKeyField = nil then
      raise Exception.CreateFmt('Field %s not found', [FPKFieldName]);

    xBlobField := qryRead.FieldByName(FBlobFieldName);
    if xBlobField = nil then
      raise Exception.CreateFmt('Field %s not found', [FBlobFieldName]);

    while not qryRead.Eof do
    begin
      case xKeyField.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          id := xKeyField.AsInt64;
        else
          idStr := xKeyField.AsString;
      end;

      if (xBlobField.IsNull) then
      begin
        qryRead.Next;
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
            mmLog.Lines.Add('Key %s=%d. No change blob.', [FPKFieldName, id]);
            EventLog.Info('Key %s=%d. No change blob.', [FPKFieldName, id]);
          end
          else
          begin
            if (FPKFieldName = 'DB_KEY') then
            begin
              xHexPrefix := 'x';
              ca := idStr.ToCharArray;
              idBinary := '';
              for c in cb do
                idBinary := idBinary + IntToHex(c, 2);
            end
            else
              idBinary := idStr;
            mmLog.Lines.Add('Key %s=%s''%s''. No change blob.', [FPKFieldName, xHexPrefix, idBinary]);
            EventLog.Info('Key %s=%s''%s''. No change blob.', [FPKFieldName, xHexPrefix, idBinary]);
          end
        end;
        qryRead.Next;
        continue;
      end;
      xWriteBlob := ConvertBlob(xReadBlob);
      xReadBlob.Close;

      case xKeyParam.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          xKeyParam.AsInt64 := id;
        else
          xKeyParam.AsString := idStr;
      end;

      xBlobParam.AsBlob := xWriteBlob;
      qryWrite.ExecQuery;
      xWriteBlob.Cancel;
      qryRead.Next;
      case xKeyField.GetSQLType of
        SQL_INT64, SQL_LONG, SQL_SHORT:
          if (xBlobType = btSegmented) and (FBlobType = btStream) then
          begin
            mmLog.Lines.Add('Key %s=%d. Convert segemented to streamed blob.', [FPKFieldName, id]);
            EventLog.Info('Key %s=%d. Convert segemented to streamed blob.', [FPKFieldName, id]);
          end
          else if (xBlobType = btStream) and (FBlobType = btSegmented) then
          begin
            mmLog.Lines.Add('Key %s=%d. Convert streamed to segemented blob with max segment size %d', [FPKFieldName, id, FSegmentSize]);
            EventLog.Info('Key %s=%d. Convert streamed to segemented blob with max segment size %d', [FPKFieldName, id, FSegmentSize])
          end
          else
          begin
            mmLog.Lines.Add('Key %s=%d. Rewrite segmented blob with max segment size %d', [FPKFieldName, id, FSegmentSize]);
            EventLog.Info('Key %s=%d. Rewrite segmented blob with max segment size %d', [FPKFieldName, id, FSegmentSize]);
          end
        else
        begin
          if (FPKFieldName = 'DB_KEY') then
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
            mmLog.Lines.Add('Key %s=%s''%s''. Convert segemented to streamed blob.', [FPKFieldName, xHexPrefix, idBinary]);
            EventLog.Info('Key %s=%s''%s''. Convert segemented to streamed blob.', [FPKFieldName, xHexPrefix, idBinary]);
          end
          else if (xBlobType = btStream) and (FBlobType = btSegmented) then
          begin
            mmLog.Lines.Add('Key %s=%s''%s''. Convert streamed to segemented blob with max segment size %d', [FPKFieldName, xHexPrefix, idBinary, FSegmentSize]);
            EventLog.Info('Key %s=%s''%s''. Convert streamed to segemented blob with max segment size %d', [FPKFieldName, xHexPrefix, idBinary, FSegmentSize]);
          end
          else
          begin
            mmLog.Lines.Add('Key %s=%s''%s''. Rewrite segmented blob with max segment size %d', [FPKFieldName, xHexPrefix, idBinary, FSegmentSize]);
            EventLog.Info('Key %s=%s''%s''. Rewrite segmented blob with max segment size %d', [FPKFieldName, xHexPrefix, idBinary, FSegmentSize]);
          end
        end;
      end;

    end;

    trWrite.Commit;
  except
    on E: Exception do
    begin
      trWrite.Rollback;
      Application.ShowException(E);
      EventLog.Error(E.Message);
    end;
  end;
  qryRead.Close;
  trRead.Commit;
  Database.Close();

  mmLog.Lines.Add('=========================================================');
  mmLog.Lines.Add('Finish optimize');

  EventLog.Info('=========================================================');
  EventLog.Info('Finish optimize');
  EventLog.Active := False;

  EnabledControls;
end;

procedure TMainForm.btnStatClick(Sender: TObject);
const
  sBlobTypes: array[btSegmented .. btStream] of string = ('Segmented', 'Stream');
var
  xReadBlob: IBlob;
  xNumSegments: Int64;
  xMaxSegmentSize, xTotalSize: Int64;
  xBlobType: TBlobType;
  xKeyField, xBlobField: ISQLData;
  id: Int64;
  idStr: string;
  ca: TCharArray;
  cb: TByteArray absolute ca;
  c: Byte;
  idBinary: string;
  xReadTime: Double;
begin
  DisabledControls;

  EventLog.FileName := FLogDirectory + DirectorySeparator + 'analyze_' + FormatDateTime('YYYY-MM-DD-hh-nn-ss', Now) + '.log';
  EventLog.Active := True;

  EventLog.Info('Start analyze');
  EventLog.Info('=========================================================');

  mmLog.Lines.Clear;
  mmLog.Lines.Add('Start analyze');
  mmLog.Lines.Add('=========================================================');

  try
    if FAutoBuildSql then
      qryRead.SQL.Text := BuildSelectSql
    else
      qryRead.SQL.LoadFromFile(FSelectSqlFileName);

    Database.Open;
  except
    on E: Exception do
    begin
      Application.ShowException(E);
      EventLog.Error(E.Message);
      EventLog.Active := False;
      DisabledControls;
      Exit;
    end;
  end;

  try
    trRead.StartTransaction;

    qryRead.ExecQuery;
    if (Trim(FPKFieldName) = '') then
      FPKFieldName := 'DB_KEY';

    if (qryRead.Eof) then
    begin
      trRead.Commit;
      EnabledControls;
      mmLog.Lines.Add('=========================================================');
      mmLog.Lines.Add('Finish analyze');

      EventLog.Info('=========================================================');
      EventLog.Info('Finish analyze');
      EventLog.Active := False;
      Exit;
    end;
    xKeyField := qryRead.FieldByName(FPKFieldName);
    if xKeyField = nil then
      raise Exception.CreateFmt('Field %s not found', [FPKFieldName]);

    xBlobField := qryRead.FieldByName(FBlobFieldName);
    if xBlobField = nil then
      raise Exception.CreateFmt('Field %s not found', [FBlobFieldName]);

    while not qryRead.Eof do
    begin
      if (xBlobField.IsNull) then
      begin
        qryRead.Next;
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
      if FReadStatFlag then
      begin
        xReadTime := ReadBlobStat(xReadBlob);
        case xKeyField.GetSQLType of
          SQL_INT64, SQL_LONG, SQL_SHORT:
          begin
            mmLog.Lines.Add('Key %s=%d; ReadTime: %8.3f ms;', [FPKFieldName, id, xReadTime]);
            EventLog.Info('Key %s=%d; ReadTime: %8.3f ms;', [FPKFieldName, id, xReadTime]);
          end
          else if (FPKFieldName = 'DB_KEY') then
          begin
            ca := idStr.ToCharArray;
            idBinary := '';
            for c in cb do
              idBinary := idBinary + IntToHex(c, 2);
            mmLog.Lines.Add('Key DB_KEY=x''%s''; ReadTime: %8.3f ms;', [idBinary, xReadTime]);
            EventLog.Info('Key DB_KEY=x''%s''; ReadTime: %8.3f ms;', [idBinary, xReadTime]);
          end
          else
          begin
            mmLog.Lines.Add('Key %s=''%s''; ReadTime: %8.3f ms;', [FPKFieldName, idStr, xReadTime]);
            EventLog.Info('Key %s=''%s''; ReadTime: %8.3f ms;', [FPKFieldName, idStr, xReadTime]);
          end;
        end;
        mmLog.Lines.Add(
          'NumSegments: %d; MaxSegmentSize: %d; TotalSize: %d; BlobType: %s;',
          [
            xNumSegments,
            xMaxSegmentSize,
            xTotalSize,
            sBlobTypes[xBlobType]
          ]
        );
        EventLog.Info(
          'NumSegments: %d; MaxSegmentSize: %d; TotalSize: %d; BlobType: %s;',
          [
            xNumSegments,
            xMaxSegmentSize,
            xTotalSize,
            sBlobTypes[xBlobType]
          ]
        );
        mmLog.Lines.Add('');
        EventLog.Info('');
      end
      else
      begin
        case xKeyField.GetSQLType of
          SQL_INT64, SQL_LONG, SQL_SHORT:
          begin
            mmLog.Lines.Add('Key %s=%d;', [FPKFieldName, id]);
            EventLog.Info('Key %s=%d;', [FPKFieldName, id]);
          end
          else if (FPKFieldName = 'DB_KEY') then
          begin
            ca := idStr.ToCharArray;
            idBinary := '';
            for c in cb do
              idBinary := idBinary + IntToHex(c, 2);
            mmLog.Lines.Add('Key DB_KEY=x''%s'';', [idBinary]);
            EventLog.Info('Key DB_KEY=x''%s'';', [idBinary]);
          end
          else
            mmLog.Lines.Add('Key %s="%s";', [FPKFieldName, idStr]);
        end;
        mmLog.Lines.Add(
          'NumSegments: %d; MaxSegmentSize: %d; TotalSize: %d; BlobType: %s;',
          [
            xNumSegments,
            xMaxSegmentSize,
            xTotalSize,
            sBlobTypes[xBlobType]
          ]
        );
        EventLog.Info(
          'NumSegments: %d; MaxSegmentSize: %d; TotalSize: %d; BlobType: %s;',
          [
            xNumSegments,
            xMaxSegmentSize,
            xTotalSize,
            sBlobTypes[xBlobType]
          ]
        );
        mmLog.Lines.Add('');
        EventLog.Info('');
      end;
      xReadBlob.Close;
      qryRead.Next;
    end;
  except
    on E: Exception do
    begin
      Application.ShowException(E);
      EventLog.Error(E.Message);
    end;
  end;
  qryRead.Close;
  trRead.Commit;
  Database.Close();
  EnabledControls;

  mmLog.Lines.Add('=========================================================');
  mmLog.Lines.Add('Finish analyze');

  EventLog.Info('=========================================================');
  EventLog.Info('Finish analyze');
  EventLog.Active := False;
end;

procedure TMainForm.cbxAutoBuildSqlChange(Sender: TObject);
begin
  FAutoBuildSql := cbxAutoBuildSql.Checked;
  edtTableName.Enabled := FAutoBuildSql;
  edtRows.Enabled := FAutoBuildSql;
  edtWhereFilter.Enabled := FAutoBuildSql;
  edtSelectSqlFileName.Enabled := not FAutoBuildSql;
  edtModifySqlFileName.Enabled := not FAutoBuildSql;
end;

procedure TMainForm.cbxReadTimeStatChange(Sender: TObject);
begin
  FReadStatFlag := cbxReadTimeStat.Checked;
end;

procedure TMainForm.edtBLOBFieldNameChange(Sender: TObject);
begin
  FBlobFieldName := edtBLOBFieldName.Text;
end;

procedure TMainForm.edtCharsetChange(Sender: TObject);
begin
  Database.Params.Values['lc_ctype'] := edtCharset.Text;
end;

procedure TMainForm.edtDatabaseChange(Sender: TObject);
begin
  Database.DatabaseName := edtDatabase.Text;
end;

procedure TMainForm.edtLogDirectoryClick(Sender: TObject);
begin
  FLogDirectory := edtLogDirectory.Directory;
end;

procedure TMainForm.edtModifySqlFilenameChange(Sender: TObject);
begin
  FModifySqlFilename := edtModifySqlFilename.Text;
end;

procedure TMainForm.edtPasswordChange(Sender: TObject);
begin
  Database.Params.Values['password'] := edtPassword.Text;
end;

procedure TMainForm.edtPKFieldNameChange(Sender: TObject);
begin
  FPKFieldName := edtPKFieldName.Text;
end;

procedure TMainForm.edtRowsChange(Sender: TObject);
begin
  FRowsLimit := edtRows.Value;
end;

procedure TMainForm.edtSegmentSizeChange(Sender: TObject);
begin
  FSegmentSize := edtSegmentSize.Value;
end;

procedure TMainForm.edtSelectSqlFilenameChange(Sender: TObject);
begin
  FSelectSqlFilename := edtSelectSqlFilename.Text;
end;

procedure TMainForm.edtTableNameChange(Sender: TObject);
begin
  FTableName := edtTableName.Text;
end;

procedure TMainForm.edtUserChange(Sender: TObject);
begin
  Database.Params.Values['user_name'] := edtUser.Text;
end;

procedure TMainForm.edtWhereFilterChange(Sender: TObject);
begin
  FWhereFilter := edtWhereFilter.Text;
end;


procedure TMainForm.FormCreate(Sender: TObject);
begin
  JSONPropStorage.JSONFileName := AppDir + '/settings.json';
end;

procedure TMainForm.FormShow(Sender: TObject);
begin
  {$ifndef blobopt}
  edtSegmentSize.Visible := False;
  rbBlobType.Visible := False;
  edtModifySqlFileName.Visible := False;
  btnOptimize.Visible := False;
  lblSegmentSize.Visible := False;
  lblModifySqlFilename.Visible := False;
  {$endif}

  edtSelectSqlFileName.InitialDir := AppDir;
  edtModifySqlFileName.InitialDir := AppDir;
  ReadSettings;
  edtSegmentSize.Value := FSegmentSize;
  edtPKFieldName.Text := FPKFieldName;
  edtBlobFieldName.Text := FBlobFieldName;
  edtDatabase.Text := Database.DatabaseName;
  edtUser.Text := Database.Params.Values['user_name'];
  edtPassword.Text := Database.Params.Values['password'];
  edtCharset.Text := Database.Params.Values['lc_ctype'];
  rbBlobType.ItemIndex := Integer(FBlobType);
  cbxReadTimeStat.Checked := FReadStatFlag;
  cbxAutoBuildSql.Checked := FAutoBuildSql;
  edtTableName.Text := FTableName;
  edtTableName.Enabled := FAutoBuildSql;
  edtRows.Value := FRowsLimit;
  edtRows.Enabled := FAutoBuildSql;
  edtWhereFilter.Text := FWhereFilter;
  edtWhereFilter.Enabled := FAutoBuildSql;
  edtLogDirectory.Directory := FLogDirectory;
  edtSelectSqlFileName.FileName := FSelectSqlFilename;
  edtModifySqlFileName.Filename := FModifySqlFilename;
  edtSelectSqlFileName.Enabled := not FAutoBuildSql;
  edtModifySqlFileName.Enabled := not FAutoBuildSql;
  case FBlobType of
    btSegmented: edtSegmentSize.Enabled := True;
    btStream: edtSegmentSize.Enabled := False;
  end;
end;



procedure TMainForm.rbBlobTypeClick(Sender: TObject);
begin
  FBlobType := TBlobType(rbBlobType.ItemIndex);
  case FBlobType of
    btSegmented: edtSegmentSize.Enabled := True;
    btStream: edtSegmentSize.Enabled := False;
  end;
end;


function TMainForm.GetAppDir: string;
begin
  Result := ExtractFileDir(Application.ExeName);
end;

function TMainForm.ReadBlobStat(ABlob: IBlob): Double;
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

function TMainForm.BuildSelectSql: string;
begin
  Result := 'select ';
  if (Trim(FBlobFieldName) = '') then
    raise Exception.Create('Blob field name not initialize');
  Result := Result + Trim(FBlobFieldName);
  if (Trim(FPKFieldName) = '') or (Trim(FPKFieldName) = 'DB_KEY') then
    Result := Result + ', rdb$db_key as db_key'
  else
    Result := Result + ', ' + Trim(FPKFieldName);
  Result := Result + #13;
  Result := Result + 'from ';
  if (Trim(FTableName) = '') then
    raise Exception.Create('Table name not initialize');
  Result := Result + Trim(FTableName);
  if (Trim(FWhereFilter) <> '') then
  begin
    Result := Result + #13;
    Result := Result + 'where ' + Trim(FWhereFilter);
  end;
  if FRowsLimit > -1 then
  begin
    Result := Result + #13;
    Result := Result + Format('rows %d', [FRowsLimit]);
  end;
end;

function TMainForm.BuildModifySql: string;
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

  if (Trim(FPKFieldName) = '') or (Trim(FPKFieldName) = 'DB_KEY') then
    Result := Result + 'rdb$db_key = :db_key'
  else
    Result := Result + Trim(FPKFieldName) + ' = :' + Trim(FPKFieldName);
end;

procedure TMainForm.ReadSettings;
begin
  FSegmentSize := JSONPropStorage.ReadInteger('segment_size', 65535);
  FRowsLimit := JSONPropStorage.ReadInteger('rows_limit', -1);
  FReadStatFlag := JSONPropStorage.ReadBoolean('time_stat', False);
  FBlobType := TBlobType(JSONPropStorage.ReadInteger('blob_type', Integer(btSegmented)));
  FBlobFieldName := JSONPropStorage.ReadString('blob_field', '');
  FPKFieldName := JSONPropStorage.ReadString('pk_field', 'ID');
  FAutoBuildSql := JSONPropStorage.ReadBoolean('auto_build_sql', true);
  FTableName := JSONPropStorage.ReadString('table_name', '');
  FWhereFilter := JSONPropStorage.ReadString('where_filter', '');
  FLogDirectory := JSONPropStorage.ReadString('log_dir', AppDir);
  FSelectSqlFilename := JSONPropStorage.ReadString('select_sql_filename', AppDir + DirectorySeparator + 'select.sql');
  FModifySqlFilename := JSONPropStorage.ReadString('modify_sql_filename', AppDir + DirectorySeparator + 'modify.sql');
  Database.DatabaseName := JSONPropStorage.ReadString('database_name', '');
  JSONPropStorage.ReadStrings('database_params', Database.Params);
end;

function TMainForm.ConvertBlob(ABlob: IBlob): IBlob;
var
  xBPB: IBPB;
  xReadSize: Longint;
  xStream: TBytesStream;
  xStreamReadSize: Longint;
  xBuffer: array[0.. 2 * MAX_SEGMENT_SIZE + 1] of Byte;
begin
  xStream := nil;
  xBPB := nil;
  if (FBlobType = btStream) then
  begin
    xBPB := Database.Attachment.AllocateBPB;
    xBPB.Add(isc_bpb_type).AsByte:=isc_bpb_type_stream;
  end
  else
  begin
    xStream := TBytesStream.create();
  end;
  Result := Database.Attachment.CreateBlob(trWrite.TransactionIntf, ABlob.GetSubType, ABlob.GetCharsetId,  xBPB);
  xReadSize := ABlob.Read(xBuffer, MAX_SEGMENT_SIZE);
  while (xReadSize > 0) do
  begin
    if (FBlobType = btStream) then
    begin
      Result.Write(xBuffer, xReadSize);
    end
    else
    begin
      xStream.Write(xBuffer, xReadSize); // remainder + xReadSize
      // when the size exceeds the maximum segment size
      if (xStream.Size >= FSegmentSize) then
      begin
        xStream.Position := 0;
        // read the size of the new segment and write to the blob
        xStreamReadSize := xStream.Read(xBuffer, FSegmentSize);
        while (xStreamReadSize = FSegmentSize) do
        begin
          // write it to a new blob
          Result.Write(xBuffer, xStreamReadSize);
          xStreamReadSize := xStream.Read(xBuffer, FSegmentSize);
        end;

        // flush the stream and write this buffer back
        if xStreamReadSize > 0 then
        begin
          xStream.Clear;
          xStream.Write(xBuffer, xStreamReadSize);
        end;
      end
      else
      begin
        Result.Write(xBuffer, xReadSize);
        xStream.Clear;
      end;
    end;
    // and continue reading blob
    xReadSize := ABlob.Read(xBuffer, MAX_SEGMENT_SIZE);
  end;
  if Assigned(xStream) then
  begin
    xStream.Position := 0;
    if xStream.Size > 0 then
    begin
      xReadSize := xStream.Write(xBuffer, xStream.Size);
      Result.Write(xBuffer, xReadSize);
    end;
    xStream.Free;
  end;
end;

procedure TMainForm.EnabledControls;
begin
  edtDatabase.Enabled := True;
  edtUser.Enabled := True;
  edtPassword.Enabled := True;
  edtCharset.Enabled := True;
  edtPKFieldName.Enabled := True;
  edtBLOBFieldName.Enabled := True;
  btnStat.Enabled := True;
  btnOptimize.Enabled := True;
  btnSave.Enabled := True;
  rbBlobType.Enabled := True;
  cbxAutoBuildSql.Enabled := True;
  edtTableName.Enabled := FAutoBuildSql;
  edtRows.Enabled := FAutoBuildSql;
  edtWhereFilter.Enabled := FAutoBuildSql;
  edtLogDirectory.Enabled := True;
  edtSelectSqlFilename.Enabled := not FAutoBuildSql;
  edtModifySqlFilename.Enabled := not FAutoBuildSql;
  case FBlobType of
    btSegmented: edtSegmentSize.Enabled := True;
    btStream: edtSegmentSize.Enabled := False;
  end;
end;

procedure TMainForm.DisabledControls;
begin
  edtDatabase.Enabled := False;
  edtUser.Enabled := False;
  edtPassword.Enabled := False;
  edtCharset.Enabled := False;
  edtPKFieldName.Enabled := False;
  edtBLOBFieldName.Enabled := False;
  btnStat.Enabled := False;
  btnOptimize.Enabled := False;
  btnSave.Enabled := False;
  rbBlobType.Enabled := False;
  edtSegmentSize.Enabled := False;
  cbxAutoBuildSql.Enabled := False;
  edtTableName.Enabled := False;
  edtRows.Enabled := False;
  edtLogDirectory.Enabled := False;
  edtWhereFilter.Enabled := False;
  edtSelectSqlFilename.Enabled := False;
  edtModifySqlFilename.Enabled := False;
end;

end.

