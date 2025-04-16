// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v3.12.4
// source: app/server/datasource/rdbms/logging/split.proto

package logging

import (
	common "github.com/ydb-platform/fq-connector-go/api/common"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TSplitDescription struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Payload:
	//
	//	*TSplitDescription_Ydb
	Payload       isTSplitDescription_Payload `protobuf_oneof:"payload"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TSplitDescription) Reset() {
	*x = TSplitDescription{}
	mi := &file_app_server_datasource_rdbms_logging_split_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TSplitDescription) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TSplitDescription) ProtoMessage() {}

func (x *TSplitDescription) ProtoReflect() protoreflect.Message {
	mi := &file_app_server_datasource_rdbms_logging_split_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TSplitDescription.ProtoReflect.Descriptor instead.
func (*TSplitDescription) Descriptor() ([]byte, []int) {
	return file_app_server_datasource_rdbms_logging_split_proto_rawDescGZIP(), []int{0}
}

func (x *TSplitDescription) GetPayload() isTSplitDescription_Payload {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *TSplitDescription) GetYdb() *TSplitDescription_TYdb {
	if x != nil {
		if x, ok := x.Payload.(*TSplitDescription_Ydb); ok {
			return x.Ydb
		}
	}
	return nil
}

type isTSplitDescription_Payload interface {
	isTSplitDescription_Payload()
}

type TSplitDescription_Ydb struct {
	Ydb *TSplitDescription_TYdb `protobuf:"bytes,1,opt,name=ydb,proto3,oneof"`
}

func (*TSplitDescription_Ydb) isTSplitDescription_Payload() {}

// TYdb is used to describe the column shards of the OLAP YDB database
// that is used as an underlying storage for Cloud Logging.
type TSplitDescription_TYdb struct {
	state         protoimpl.MessageState   `protogen:"open.v1"`
	Endpoint      *common.TGenericEndpoint `protobuf:"bytes,1,opt,name=endpoint,proto3" json:"endpoint,omitempty"`
	DatabaseName  string                   `protobuf:"bytes,2,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	TableName     string                   `protobuf:"bytes,3,opt,name=table_name,json=tableName,proto3" json:"table_name,omitempty"`
	TabletIds     []uint64                 `protobuf:"varint,4,rep,packed,name=tablet_ids,json=tabletIds,proto3" json:"tablet_ids,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TSplitDescription_TYdb) Reset() {
	*x = TSplitDescription_TYdb{}
	mi := &file_app_server_datasource_rdbms_logging_split_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TSplitDescription_TYdb) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TSplitDescription_TYdb) ProtoMessage() {}

func (x *TSplitDescription_TYdb) ProtoReflect() protoreflect.Message {
	mi := &file_app_server_datasource_rdbms_logging_split_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TSplitDescription_TYdb.ProtoReflect.Descriptor instead.
func (*TSplitDescription_TYdb) Descriptor() ([]byte, []int) {
	return file_app_server_datasource_rdbms_logging_split_proto_rawDescGZIP(), []int{0, 0}
}

func (x *TSplitDescription_TYdb) GetEndpoint() *common.TGenericEndpoint {
	if x != nil {
		return x.Endpoint
	}
	return nil
}

func (x *TSplitDescription_TYdb) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *TSplitDescription_TYdb) GetTableName() string {
	if x != nil {
		return x.TableName
	}
	return ""
}

func (x *TSplitDescription_TYdb) GetTabletIds() []uint64 {
	if x != nil {
		return x.TabletIds
	}
	return nil
}

var File_app_server_datasource_rdbms_logging_split_proto protoreflect.FileDescriptor

var file_app_server_datasource_rdbms_logging_split_proto_rawDesc = string([]byte{
	0x0a, 0x2f, 0x61, 0x70, 0x70, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x64, 0x61, 0x74,
	0x61, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2f, 0x72, 0x64, 0x62, 0x6d, 0x73, 0x2f, 0x6c, 0x6f,
	0x67, 0x67, 0x69, 0x6e, 0x67, 0x2f, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x32, 0x4e, 0x59, 0x71, 0x6c, 0x2e, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x41, 0x70, 0x70, 0x2e, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x44, 0x61, 0x74,
	0x61, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x52, 0x44, 0x42, 0x4d, 0x53, 0x2e, 0x4c, 0x6f,
	0x67, 0x67, 0x69, 0x6e, 0x67, 0x1a, 0x3b, 0x79, 0x71, 0x6c, 0x2f, 0x65, 0x73, 0x73, 0x65, 0x6e,
	0x74, 0x69, 0x61, 0x6c, 0x73, 0x2f, 0x70, 0x72, 0x6f, 0x76, 0x69, 0x64, 0x65, 0x72, 0x73, 0x2f,
	0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x61, 0x74,
	0x65, 0x77, 0x61, 0x79, 0x73, 0x5f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x9e, 0x02, 0x0a, 0x11, 0x54, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x44, 0x65, 0x73,
	0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x5e, 0x0a, 0x03, 0x79, 0x64, 0x62, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x4a, 0x2e, 0x4e, 0x59, 0x71, 0x6c, 0x2e, 0x43, 0x6f, 0x6e,
	0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x41, 0x70, 0x70, 0x2e, 0x53, 0x65, 0x72, 0x76, 0x65,
	0x72, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x52, 0x44, 0x42,
	0x4d, 0x53, 0x2e, 0x4c, 0x6f, 0x67, 0x67, 0x69, 0x6e, 0x67, 0x2e, 0x54, 0x53, 0x70, 0x6c, 0x69,
	0x74, 0x44, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x54, 0x59, 0x64,
	0x62, 0x48, 0x00, 0x52, 0x03, 0x79, 0x64, 0x62, 0x1a, 0x9d, 0x01, 0x0a, 0x04, 0x54, 0x59, 0x64,
	0x62, 0x12, 0x32, 0x0a, 0x08, 0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x4e, 0x59, 0x71, 0x6c, 0x2e, 0x54, 0x47, 0x65, 0x6e, 0x65,
	0x72, 0x69, 0x63, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x52, 0x08, 0x65, 0x6e, 0x64,
	0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73,
	0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64, 0x61,
	0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61,
	0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x74, 0x61, 0x62, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61, 0x62,
	0x6c, 0x65, 0x74, 0x5f, 0x69, 0x64, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x04, 0x52, 0x09, 0x74,
	0x61, 0x62, 0x6c, 0x65, 0x74, 0x49, 0x64, 0x73, 0x42, 0x09, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c,
	0x6f, 0x61, 0x64, 0x42, 0x4e, 0x5a, 0x4c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x79, 0x64, 0x62, 0x2d, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x2f, 0x66,
	0x71, 0x2d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2d, 0x67, 0x6f, 0x2f, 0x61,
	0x70, 0x70, 0x2f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x2f, 0x72, 0x64, 0x62, 0x6d, 0x73, 0x2f, 0x6c, 0x6f, 0x67, 0x67, 0x69,
	0x6e, 0x67, 0x2f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_app_server_datasource_rdbms_logging_split_proto_rawDescOnce sync.Once
	file_app_server_datasource_rdbms_logging_split_proto_rawDescData []byte
)

func file_app_server_datasource_rdbms_logging_split_proto_rawDescGZIP() []byte {
	file_app_server_datasource_rdbms_logging_split_proto_rawDescOnce.Do(func() {
		file_app_server_datasource_rdbms_logging_split_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_app_server_datasource_rdbms_logging_split_proto_rawDesc), len(file_app_server_datasource_rdbms_logging_split_proto_rawDesc)))
	})
	return file_app_server_datasource_rdbms_logging_split_proto_rawDescData
}

var file_app_server_datasource_rdbms_logging_split_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_app_server_datasource_rdbms_logging_split_proto_goTypes = []any{
	(*TSplitDescription)(nil),       // 0: NYql.Connector.App.Server.DataSource.RDBMS.Logging.TSplitDescription
	(*TSplitDescription_TYdb)(nil),  // 1: NYql.Connector.App.Server.DataSource.RDBMS.Logging.TSplitDescription.TYdb
	(*common.TGenericEndpoint)(nil), // 2: NYql.TGenericEndpoint
}
var file_app_server_datasource_rdbms_logging_split_proto_depIdxs = []int32{
	1, // 0: NYql.Connector.App.Server.DataSource.RDBMS.Logging.TSplitDescription.ydb:type_name -> NYql.Connector.App.Server.DataSource.RDBMS.Logging.TSplitDescription.TYdb
	2, // 1: NYql.Connector.App.Server.DataSource.RDBMS.Logging.TSplitDescription.TYdb.endpoint:type_name -> NYql.TGenericEndpoint
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_app_server_datasource_rdbms_logging_split_proto_init() }
func file_app_server_datasource_rdbms_logging_split_proto_init() {
	if File_app_server_datasource_rdbms_logging_split_proto != nil {
		return
	}
	file_app_server_datasource_rdbms_logging_split_proto_msgTypes[0].OneofWrappers = []any{
		(*TSplitDescription_Ydb)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_app_server_datasource_rdbms_logging_split_proto_rawDesc), len(file_app_server_datasource_rdbms_logging_split_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_app_server_datasource_rdbms_logging_split_proto_goTypes,
		DependencyIndexes: file_app_server_datasource_rdbms_logging_split_proto_depIdxs,
		MessageInfos:      file_app_server_datasource_rdbms_logging_split_proto_msgTypes,
	}.Build()
	File_app_server_datasource_rdbms_logging_split_proto = out.File
	file_app_server_datasource_rdbms_logging_split_proto_goTypes = nil
	file_app_server_datasource_rdbms_logging_split_proto_depIdxs = nil
}
