unit main;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls, Spin,
  JSONPropStorage, ExtCtrls, IBDatabase, Ib, IBSQL
  {$ifdef WINDOWS}
  , Windows
  {$endif}
  ;

type

  { TMainForm }

  TMainForm = class(TForm)
    btnStart: TButton;
    btnSave: TButton;
    btnStat: TButton;
    cbxReadTimeStat: TCheckBox;
    Database: TIBDatabase;
    edtUser: TEdit;
    edtPassword: TEdit;
    edtCharset: TEdit;
    edtDatabase: TEdit;
    edtBLOBFieldName: TEdit;
    edtPKFieldName: TEdit;
    JSONPropStorage: TJSONPropStorage;
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    Label5: TLabel;
    Label6: TLabel;
    Label7: TLabel;
    mmLog: TMemo;
    qryWrite: TIBSQL;
    qryRead: TIBSQL;
    edtSegmentSize: TSpinEdit;
    rbBlobType: TRadioGroup;
    trRead: TIBTransaction;
    trWrite: TIBTransaction;
    procedure btnSaveClick(Sender: TObject);
    procedure btnStartClick(Sender: TObject);
    procedure btnStatClick(Sender: TObject);
    procedure cbxReadTimeStatChange(Sender: TObject);
    procedure edtBLOBFieldNameChange(Sender: TObject);
    procedure edtCharsetChange(Sender: TObject);
    procedure edtDatabaseChange(Sender: TObject);
    procedure edtPasswordChange(Sender: TObject);
    procedure edtPKFieldNameChange(Sender: TObject);
    procedure edtSegmentSizeChange(Sender: TObject);
    procedure edtUserChange(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure rbBlobTypeClick(Sender: TObject);
  private
    FSegmentSize: Integer;
    FBlobType: TBlobType;
    FReadStatFlag: Boolean;
    FBlobFieldName: string;
    FPKFieldName: string;

    function GetAppDir: string;
    function ReadBlobStat(ABlob: IBlob): Double;
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
  JSONPropStorage.Save;
end;

procedure TMainForm.btnStartClick(Sender: TObject);
var
  xReadBlob: IBlob;
  xWriteBlob: IBlob;
  id: Int64;
  idStr: string;
  xNumSegments: Int64;
  xMaxSegmentSize, xTotalSize: Int64;
  xBlobType: TBlobType;
  xKeyField, xBlobField: ISQLData;
  xKeyParam, xBlobParam: ISQLParam;
begin
  mmLog.Lines.Clear;

  try
    Database.Open;
  except
    on E: Exception do
    begin
      Application.ShowException(E);
      Exit;
    end;
  end;

  DisabledControls;

  trRead.StartTransaction;
  trWrite.StartTransaction;
  try
    qryWrite.Prepare;
    xKeyParam := qryWrite.ParamByName(FPKFieldName);
    if xKeyParam = nil then
      raise Exception.CreateFmt('Parameter %s not found', [FPKFieldName]);

    xBlobParam := qryWrite.ParamByName(FBlobFieldName);
    if xBlobParam = nil then
      raise Exception.CreateFmt('Parameter %s not found', [FBlobFieldName]);

    qryRead.ExecQuery;
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
          mmLog.Lines.Add('Convert blob. Id=%d', [id]);
        else
          mmLog.Lines.Add('Convert blob. Id="%s"', [idStr]);
      end;

    end;

    trWrite.Commit;
  except
    on E: Exception do
    begin
      trWrite.Rollback;
      Application.ShowException(E);
    end;
  end;
  qryRead.Close;
  trRead.Commit;
  Database.Close();

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
  xReadTime: Double;
begin
  mmLog.Lines.Clear;

  try
    Database.Open;
  except
    on E: Exception do
    begin
      Application.ShowException(E);
      Exit;
    end;
  end;
  DisabledControls;
  trRead.StartTransaction;

  try
    qryRead.ExecQuery;
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
        else
          idStr := xKeyField.AsString;
      end;

      xReadBlob := xBlobField.GetAsBlob;
      xReadBlob.GetInfo(xNumSegments, xMaxSegmentSize, xTotalSize, xBlobType);
      if FReadStatFlag then
      begin
        xReadTime := ReadBlobStat(xReadBlob);
        case xKeyField.GetSQLType of
          SQL_INT64, SQL_LONG, SQL_SHORT:
            mmLog.Lines.Add('Key %s=%d; ReadTime: %8.3f ms;', [FPKFieldName, id, xReadTime]);
          else
            mmLog.Lines.Add('Key %s="%s"; ReadTime: %8.3f ms;', [FPKFieldName, idStr, xReadTime]);
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
        mmLog.Lines.Add('');
      end
      else
      begin
        case xKeyField.GetSQLType of
          SQL_INT64, SQL_LONG, SQL_SHORT:
            mmLog.Lines.Add('Key %s=%d;', [FPKFieldName, id]);
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
        mmLog.Lines.Add('');
      end;
      xReadBlob.Close;
      qryRead.Next;
    end;
  except
    on E: Exception do
    begin
      Application.ShowException(E);
    end;
  end;
  qryRead.Close;
  trRead.Commit;
  Database.Close();
  EnabledControls;
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

procedure TMainForm.edtPasswordChange(Sender: TObject);
begin
  Database.Params.Values['password'] := edtPassword.Text;
end;

procedure TMainForm.edtPKFieldNameChange(Sender: TObject);
begin
  FPKFieldName := edtPKFieldName.Text;
end;

procedure TMainForm.edtSegmentSizeChange(Sender: TObject);
begin
  FSegmentSize := edtSegmentSize.Value;
end;

procedure TMainForm.edtUserChange(Sender: TObject);
begin
  Database.Params.Values['user_name'] := edtUser.Text;
end;


procedure TMainForm.FormCreate(Sender: TObject);
begin
  JSONPropStorage.JSONFileName := AppDir + '/settings.json';
end;

procedure TMainForm.FormShow(Sender: TObject);
begin
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

procedure TMainForm.ReadSettings;
begin
  FSegmentSize := JSONPropStorage.ReadInteger('segment_size', 65535);
  FReadStatFlag := JSONPropStorage.ReadBoolean('time_stat', False);
  FBlobType := TBlobType(JSONPropStorage.ReadInteger('blob_type', Integer(btSegmented)));
  FBlobFieldName := JSONPropStorage.ReadString('blob_field', '');
  FPKFieldName := JSONPropStorage.ReadString('pk_field', 'ID');
  Database.DatabaseName := JSONPropStorage.ReadString('database_name', '');
  JSONPropStorage.ReadStrings('database_params', Database.Params);

  qryRead.SQL.LoadFromFile(AppDir + '/select.sql');
  qryWrite.SQL.LoadFromFile(AppDir + '/modify.sql');
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
  btnStart.Enabled := True;
  btnSave.Enabled := True;
  rbBlobType.Enabled := True;
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
  btnStart.Enabled := False;
  btnSave.Enabled := False;
  rbBlobType.Enabled := False;
  edtSegmentSize.Enabled := False;
end;

end.

