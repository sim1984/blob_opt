unit main;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls, Spin,
  JSONPropStorage, ExtCtrls, IBDatabase, Ib, IBSQL;

type

  { TMainForm }

  TMainForm = class(TForm)
    btnStart: TButton;
    btnSave: TButton;
    btnStat: TButton;
    Database: TIBDatabase;
    edtBLOBFieldName: TEdit;
    edtPKFieldName: TEdit;
    JSONPropStorage: TJSONPropStorage;
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
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
    procedure edtBLOBFieldNameChange(Sender: TObject);
    procedure edtPKFieldNameChange(Sender: TObject);
    procedure edtSegmentSizeChange(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure rbBlobTypeClick(Sender: TObject);
  private
    FSegmentSize: Smallint;
    FBlobType: TBlobType;
    FBlobFieldName: string;
    FPKFieldName: string;

    function GetAppDir: string;
  protected
    procedure ReadSettings;
    function ConvertBlob(ABlob: IBlob): IBlob;
  public
    property AppDir: string read GetAppDir;
  end;

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
  Database.Open;
  trRead.StartTransaction;
  trWrite.StartTransaction;

  qryWrite.Prepare;
  xKeyParam := qryWrite.ParamByName(FPKFieldName);
  xBlobParam := qryWrite.ParamByName(FBlobFieldName);

  qryRead.ExecQuery;
  xKeyField := qryRead.FieldByName(FPKFieldName);
  xBlobField := qryRead.FieldByName(FBlobFieldName);
  while not qryRead.Eof do
  begin
    case xKeyField.GetSQLType of
      SQL_INT64, SQL_LONG, SQL_SHORT:
        id := xKeyField.AsInt64;
      else
        idStr := xKeyField.AsString;
    end;

    xReadBlob := xBlobField.GetAsBlob;
    xReadBlob.GetInfo(xNumSegments, xMaxSegmentSize, xTotalSize, xBlobType);
    if (xBlobType = FBlobType) and
       ((FBlobType = btStream) or ((FBlobType = btSegmented) and (xNumSegments = 1))) then
    begin
      // потоковый блоб не надо преобрзовывать в потоковый
      // сегментированный блоб с одним сегментом не надо пересегментировать
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
    mmLog.Lines.Add('Convert blob. Id=%d', [id]);
  end;
  qryRead.Close;
  trWrite.Commit;
  trRead.Commit;
  Database.Close();
end;

procedure TMainForm.btnStatClick(Sender: TObject);
const
  sBlobTypes: array[btSegmented .. btStream] of string = ('Segmented', 'Stream');
var
  xReadBlob: IBlob;
  xNumSegments: Int64;
  xMaxSegmentSize, xTotalSize: Int64;
  xBlobType: TBlobType;
  xBlobField: ISQLData;
begin
  mmLog.Lines.Clear;
  Database.Open;
  trRead.StartTransaction;
  qryRead.ExecQuery;
  xBlobField := qryRead.FieldByName(FBlobFieldName);
  while not qryRead.Eof do
  begin
    xReadBlob := xBlobField.GetAsBlob;
    xReadBlob.GetInfo(xNumSegments, xMaxSegmentSize, xTotalSize, xBlobType);
    mmLog.Lines.Add(
      'NumSegments: %d; MaxSegmentSize: %d; TotalSize: %d; BlobType: %s;',
      [
        xNumSegments,
        xMaxSegmentSize,
        xTotalSize,
        sBlobTypes[xBlobType]
      ]
    );
    xReadBlob.Close;
    qryRead.Next;
  end;
  qryRead.Close;
  trRead.Commit;
  Database.Close();
end;

procedure TMainForm.edtBLOBFieldNameChange(Sender: TObject);
begin
  FBlobFieldName := edtBLOBFieldName.Text;
end;

procedure TMainForm.edtPKFieldNameChange(Sender: TObject);
begin
  FPKFieldName := edtPKFieldName.Text;
end;

procedure TMainForm.edtSegmentSizeChange(Sender: TObject);
begin
  FSegmentSize := edtSegmentSize.Value;
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
  rbBlobType.ItemIndex := Integer(FBlobType);
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

procedure TMainForm.ReadSettings;
begin
  FSegmentSize := JSONPropStorage.ReadInteger('segment_size', 1024);
  FBlobType := TBlobType(JSONPropStorage.ReadInteger('blob_type', Integer(btSegmented)));
  FBlobFieldName := JSONPropStorage.ReadString('blob_field', '');
  FPKFieldName := JSONPropStorage.ReadString('pk_field', 'ID');
  Database.DatabaseName := JSONPropStorage.ReadString('database_name', '');
  JSONPropStorage.ReadStrings('database_params', Database.Params);

  qryRead.SQL.LoadFromFile(AppDir + '/select.sql');
  qryWrite.SQL.LoadFromFile(AppDir + '/modify.sql');
end;

function TMainForm.ConvertBlob(ABlob: IBlob): IBlob;
const
  MaxSegmantSize = 32765;
var
  xBPB: IBPB;
  xReadSize: Longint;
  xStream: TBytesStream;
  xStreamReadSize: Longint;
  xBuffer: array[0..65535] of Byte;
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
  xReadSize := ABlob.Read(xBuffer, MaxSegmantSize);
  while (xReadSize > 0) do
  begin
    if (FBlobType = btStream) then
    begin
      Result.Write(xBuffer, xReadSize);
    end
    else
    begin
      xStream.Write(xBuffer, xReadSize); // остаток + xReadSize
      // когда размер превышает макс размер сегмента
      if (xStream.Size >= FSegmentSize) then
      begin
        xStream.Position := 0;
        // читать размером нового сегмента и писать в блоб
        xStreamReadSize := xStream.Read(xBuffer, FSegmentSize);
        while (xStreamReadSize = FSegmentSize) do
        begin
          // записать его в новый блоб
          Result.Write(xBuffer, xStreamReadSize);
          xStreamReadSize := xStream.Read(xBuffer, FSegmentSize);
        end;

        // сбросить поток и записать этот буфер обратно
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
    // и продолжить читать блоб
    xReadSize := ABlob.Read(xBuffer, MaxSegmantSize);
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

end.

