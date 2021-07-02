// Code generated by protoc-gen-go. DO NOT EDIT.
// source: transaction.proto

package spanner

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type AbortRequest struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Ts                   int64    `protobuf:"varint,2,opt,name=ts,proto3" json:"ts,omitempty"`
	CId                  int64    `protobuf:"varint,3,opt,name=cId,proto3" json:"cId,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AbortRequest) Reset()         { *m = AbortRequest{} }
func (m *AbortRequest) String() string { return proto.CompactTextString(m) }
func (*AbortRequest) ProtoMessage()    {}
func (*AbortRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{0}
}

func (m *AbortRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AbortRequest.Unmarshal(m, b)
}
func (m *AbortRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AbortRequest.Marshal(b, m, deterministic)
}
func (m *AbortRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AbortRequest.Merge(m, src)
}
func (m *AbortRequest) XXX_Size() int {
	return xxx_messageInfo_AbortRequest.Size(m)
}
func (m *AbortRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_AbortRequest.DiscardUnknown(m)
}

var xxx_messageInfo_AbortRequest proto.InternalMessageInfo

func (m *AbortRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *AbortRequest) GetTs() int64 {
	if m != nil {
		return m.Ts
	}
	return 0
}

func (m *AbortRequest) GetCId() int64 {
	if m != nil {
		return m.CId
	}
	return 0
}

type CommitResult struct {
	Commit               bool     `protobuf:"varint,1,opt,name=commit,proto3" json:"commit,omitempty"`
	Id                   string   `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	Ts                   int64    `protobuf:"varint,3,opt,name=ts,proto3" json:"ts,omitempty"`
	CId                  int64    `protobuf:"varint,4,opt,name=cId,proto3" json:"cId,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CommitResult) Reset()         { *m = CommitResult{} }
func (m *CommitResult) String() string { return proto.CompactTextString(m) }
func (*CommitResult) ProtoMessage()    {}
func (*CommitResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{1}
}

func (m *CommitResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CommitResult.Unmarshal(m, b)
}
func (m *CommitResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CommitResult.Marshal(b, m, deterministic)
}
func (m *CommitResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CommitResult.Merge(m, src)
}
func (m *CommitResult) XXX_Size() int {
	return xxx_messageInfo_CommitResult.Size(m)
}
func (m *CommitResult) XXX_DiscardUnknown() {
	xxx_messageInfo_CommitResult.DiscardUnknown(m)
}

var xxx_messageInfo_CommitResult proto.InternalMessageInfo

func (m *CommitResult) GetCommit() bool {
	if m != nil {
		return m.Commit
	}
	return false
}

func (m *CommitResult) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *CommitResult) GetTs() int64 {
	if m != nil {
		return m.Ts
	}
	return 0
}

func (m *CommitResult) GetCId() int64 {
	if m != nil {
		return m.CId
	}
	return 0
}

type Empty struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Empty) Reset()         { *m = Empty{} }
func (m *Empty) String() string { return proto.CompactTextString(m) }
func (*Empty) ProtoMessage()    {}
func (*Empty) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{2}
}

func (m *Empty) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Empty.Unmarshal(m, b)
}
func (m *Empty) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Empty.Marshal(b, m, deterministic)
}
func (m *Empty) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Empty.Merge(m, src)
}
func (m *Empty) XXX_Size() int {
	return xxx_messageInfo_Empty.Size(m)
}
func (m *Empty) XXX_DiscardUnknown() {
	xxx_messageInfo_Empty.DiscardUnknown(m)
}

var xxx_messageInfo_Empty proto.InternalMessageInfo

type ReadRequest struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Keys                 []string `protobuf:"bytes,2,rep,name=keys,proto3" json:"keys,omitempty"`
	Ts                   int64    `protobuf:"varint,3,opt,name=ts,proto3" json:"ts,omitempty"`
	CId                  int64    `protobuf:"varint,4,opt,name=cId,proto3" json:"cId,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ReadRequest) Reset()         { *m = ReadRequest{} }
func (m *ReadRequest) String() string { return proto.CompactTextString(m) }
func (*ReadRequest) ProtoMessage()    {}
func (*ReadRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{3}
}

func (m *ReadRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ReadRequest.Unmarshal(m, b)
}
func (m *ReadRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ReadRequest.Marshal(b, m, deterministic)
}
func (m *ReadRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ReadRequest.Merge(m, src)
}
func (m *ReadRequest) XXX_Size() int {
	return xxx_messageInfo_ReadRequest.Size(m)
}
func (m *ReadRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ReadRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ReadRequest proto.InternalMessageInfo

func (m *ReadRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *ReadRequest) GetKeys() []string {
	if m != nil {
		return m.Keys
	}
	return nil
}

func (m *ReadRequest) GetTs() int64 {
	if m != nil {
		return m.Ts
	}
	return 0
}

func (m *ReadRequest) GetCId() int64 {
	if m != nil {
		return m.CId
	}
	return 0
}

type ValVer struct {
	Val                  string   `protobuf:"bytes,2,opt,name=val,proto3" json:"val,omitempty"`
	Ver                  string   `protobuf:"bytes,3,opt,name=ver,proto3" json:"ver,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ValVer) Reset()         { *m = ValVer{} }
func (m *ValVer) String() string { return proto.CompactTextString(m) }
func (*ValVer) ProtoMessage()    {}
func (*ValVer) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{4}
}

func (m *ValVer) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ValVer.Unmarshal(m, b)
}
func (m *ValVer) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ValVer.Marshal(b, m, deterministic)
}
func (m *ValVer) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ValVer.Merge(m, src)
}
func (m *ValVer) XXX_Size() int {
	return xxx_messageInfo_ValVer.Size(m)
}
func (m *ValVer) XXX_DiscardUnknown() {
	xxx_messageInfo_ValVer.DiscardUnknown(m)
}

var xxx_messageInfo_ValVer proto.InternalMessageInfo

func (m *ValVer) GetVal() string {
	if m != nil {
		return m.Val
	}
	return ""
}

func (m *ValVer) GetVer() string {
	if m != nil {
		return m.Ver
	}
	return ""
}

type KeyVal struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Val                  string   `protobuf:"bytes,2,opt,name=val,proto3" json:"val,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyVal) Reset()         { *m = KeyVal{} }
func (m *KeyVal) String() string { return proto.CompactTextString(m) }
func (*KeyVal) ProtoMessage()    {}
func (*KeyVal) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{5}
}

func (m *KeyVal) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyVal.Unmarshal(m, b)
}
func (m *KeyVal) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyVal.Marshal(b, m, deterministic)
}
func (m *KeyVal) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyVal.Merge(m, src)
}
func (m *KeyVal) XXX_Size() int {
	return xxx_messageInfo_KeyVal.Size(m)
}
func (m *KeyVal) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyVal.DiscardUnknown(m)
}

var xxx_messageInfo_KeyVal proto.InternalMessageInfo

func (m *KeyVal) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyVal) GetVal() string {
	if m != nil {
		return m.Val
	}
	return ""
}

type KeyVer struct {
	Key                  string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Ver                  string   `protobuf:"bytes,2,opt,name=ver,proto3" json:"ver,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *KeyVer) Reset()         { *m = KeyVer{} }
func (m *KeyVer) String() string { return proto.CompactTextString(m) }
func (*KeyVer) ProtoMessage()    {}
func (*KeyVer) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{6}
}

func (m *KeyVer) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_KeyVer.Unmarshal(m, b)
}
func (m *KeyVer) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_KeyVer.Marshal(b, m, deterministic)
}
func (m *KeyVer) XXX_Merge(src proto.Message) {
	xxx_messageInfo_KeyVer.Merge(m, src)
}
func (m *KeyVer) XXX_Size() int {
	return xxx_messageInfo_KeyVer.Size(m)
}
func (m *KeyVer) XXX_DiscardUnknown() {
	xxx_messageInfo_KeyVer.DiscardUnknown(m)
}

var xxx_messageInfo_KeyVer proto.InternalMessageInfo

func (m *KeyVer) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *KeyVer) GetVer() string {
	if m != nil {
		return m.Ver
	}
	return ""
}

type ReadReply struct {
	Vals                 []*ValVer `protobuf:"bytes,1,rep,name=vals,proto3" json:"vals,omitempty"`
	Abort                bool      `protobuf:"varint,2,opt,name=abort,proto3" json:"abort,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *ReadReply) Reset()         { *m = ReadReply{} }
func (m *ReadReply) String() string { return proto.CompactTextString(m) }
func (*ReadReply) ProtoMessage()    {}
func (*ReadReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{7}
}

func (m *ReadReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ReadReply.Unmarshal(m, b)
}
func (m *ReadReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ReadReply.Marshal(b, m, deterministic)
}
func (m *ReadReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ReadReply.Merge(m, src)
}
func (m *ReadReply) XXX_Size() int {
	return xxx_messageInfo_ReadReply.Size(m)
}
func (m *ReadReply) XXX_DiscardUnknown() {
	xxx_messageInfo_ReadReply.DiscardUnknown(m)
}

var xxx_messageInfo_ReadReply proto.InternalMessageInfo

func (m *ReadReply) GetVals() []*ValVer {
	if m != nil {
		return m.Vals
	}
	return nil
}

func (m *ReadReply) GetAbort() bool {
	if m != nil {
		return m.Abort
	}
	return false
}

type CommitRequest struct {
	Id                   string    `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Ts                   int64     `protobuf:"varint,2,opt,name=ts,proto3" json:"ts,omitempty"`
	CId                  int64     `protobuf:"varint,3,opt,name=cId,proto3" json:"cId,omitempty"`
	RKV                  []*KeyVer `protobuf:"bytes,4,rep,name=rKV,proto3" json:"rKV,omitempty"`
	WKV                  []*KeyVal `protobuf:"bytes,5,rep,name=wKV,proto3" json:"wKV,omitempty"`
	Pp                   []int32   `protobuf:"varint,6,rep,packed,name=pp,proto3" json:"pp,omitempty"`
	CoordPId             int64     `protobuf:"varint,7,opt,name=coordPId,proto3" json:"coordPId,omitempty"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *CommitRequest) Reset()         { *m = CommitRequest{} }
func (m *CommitRequest) String() string { return proto.CompactTextString(m) }
func (*CommitRequest) ProtoMessage()    {}
func (*CommitRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{8}
}

func (m *CommitRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CommitRequest.Unmarshal(m, b)
}
func (m *CommitRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CommitRequest.Marshal(b, m, deterministic)
}
func (m *CommitRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CommitRequest.Merge(m, src)
}
func (m *CommitRequest) XXX_Size() int {
	return xxx_messageInfo_CommitRequest.Size(m)
}
func (m *CommitRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_CommitRequest.DiscardUnknown(m)
}

var xxx_messageInfo_CommitRequest proto.InternalMessageInfo

func (m *CommitRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *CommitRequest) GetTs() int64 {
	if m != nil {
		return m.Ts
	}
	return 0
}

func (m *CommitRequest) GetCId() int64 {
	if m != nil {
		return m.CId
	}
	return 0
}

func (m *CommitRequest) GetRKV() []*KeyVer {
	if m != nil {
		return m.RKV
	}
	return nil
}

func (m *CommitRequest) GetWKV() []*KeyVal {
	if m != nil {
		return m.WKV
	}
	return nil
}

func (m *CommitRequest) GetPp() []int32 {
	if m != nil {
		return m.Pp
	}
	return nil
}

func (m *CommitRequest) GetCoordPId() int64 {
	if m != nil {
		return m.CoordPId
	}
	return 0
}

type CommitReply struct {
	Commit               bool     `protobuf:"varint,1,opt,name=commit,proto3" json:"commit,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CommitReply) Reset()         { *m = CommitReply{} }
func (m *CommitReply) String() string { return proto.CompactTextString(m) }
func (*CommitReply) ProtoMessage()    {}
func (*CommitReply) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{9}
}

func (m *CommitReply) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CommitReply.Unmarshal(m, b)
}
func (m *CommitReply) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CommitReply.Marshal(b, m, deterministic)
}
func (m *CommitReply) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CommitReply.Merge(m, src)
}
func (m *CommitReply) XXX_Size() int {
	return xxx_messageInfo_CommitReply.Size(m)
}
func (m *CommitReply) XXX_DiscardUnknown() {
	xxx_messageInfo_CommitReply.DiscardUnknown(m)
}

var xxx_messageInfo_CommitReply proto.InternalMessageInfo

func (m *CommitReply) GetCommit() bool {
	if m != nil {
		return m.Commit
	}
	return false
}

type PrepareRequest struct {
	Id                   string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Prepared             bool     `protobuf:"varint,2,opt,name=prepared,proto3" json:"prepared,omitempty"`
	PId                  int32    `protobuf:"varint,3,opt,name=pId,proto3" json:"pId,omitempty"`
	Ts                   int64    `protobuf:"varint,4,opt,name=ts,proto3" json:"ts,omitempty"`
	CId                  int64    `protobuf:"varint,5,opt,name=cId,proto3" json:"cId,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PrepareRequest) Reset()         { *m = PrepareRequest{} }
func (m *PrepareRequest) String() string { return proto.CompactTextString(m) }
func (*PrepareRequest) ProtoMessage()    {}
func (*PrepareRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_2cc4e03d2c28c490, []int{10}
}

func (m *PrepareRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PrepareRequest.Unmarshal(m, b)
}
func (m *PrepareRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PrepareRequest.Marshal(b, m, deterministic)
}
func (m *PrepareRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PrepareRequest.Merge(m, src)
}
func (m *PrepareRequest) XXX_Size() int {
	return xxx_messageInfo_PrepareRequest.Size(m)
}
func (m *PrepareRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PrepareRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PrepareRequest proto.InternalMessageInfo

func (m *PrepareRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *PrepareRequest) GetPrepared() bool {
	if m != nil {
		return m.Prepared
	}
	return false
}

func (m *PrepareRequest) GetPId() int32 {
	if m != nil {
		return m.PId
	}
	return 0
}

func (m *PrepareRequest) GetTs() int64 {
	if m != nil {
		return m.Ts
	}
	return 0
}

func (m *PrepareRequest) GetCId() int64 {
	if m != nil {
		return m.CId
	}
	return 0
}

func init() {
	proto.RegisterType((*AbortRequest)(nil), "spanner.AbortRequest")
	proto.RegisterType((*CommitResult)(nil), "spanner.CommitResult")
	proto.RegisterType((*Empty)(nil), "spanner.Empty")
	proto.RegisterType((*ReadRequest)(nil), "spanner.ReadRequest")
	proto.RegisterType((*ValVer)(nil), "spanner.valVer")
	proto.RegisterType((*KeyVal)(nil), "spanner.keyVal")
	proto.RegisterType((*KeyVer)(nil), "spanner.keyVer")
	proto.RegisterType((*ReadReply)(nil), "spanner.ReadReply")
	proto.RegisterType((*CommitRequest)(nil), "spanner.CommitRequest")
	proto.RegisterType((*CommitReply)(nil), "spanner.CommitReply")
	proto.RegisterType((*PrepareRequest)(nil), "spanner.PrepareRequest")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// SpannerClient is the client API for Spanner service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SpannerClient interface {
	Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadReply, error)
	Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitReply, error)
	Abort(ctx context.Context, in *AbortRequest, opts ...grpc.CallOption) (*Empty, error)
	Prepare(ctx context.Context, in *PrepareRequest, opts ...grpc.CallOption) (*Empty, error)
	CommitDecision(ctx context.Context, in *CommitResult, opts ...grpc.CallOption) (*Empty, error)
}

type spannerClient struct {
	cc *grpc.ClientConn
}

func NewSpannerClient(cc *grpc.ClientConn) SpannerClient {
	return &spannerClient{cc}
}

func (c *spannerClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadReply, error) {
	out := new(ReadReply)
	err := c.cc.Invoke(ctx, "/spanner.Spanner/Read", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *spannerClient) Commit(ctx context.Context, in *CommitRequest, opts ...grpc.CallOption) (*CommitReply, error) {
	out := new(CommitReply)
	err := c.cc.Invoke(ctx, "/spanner.Spanner/Commit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *spannerClient) Abort(ctx context.Context, in *AbortRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/spanner.Spanner/Abort", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *spannerClient) Prepare(ctx context.Context, in *PrepareRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/spanner.Spanner/Prepare", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *spannerClient) CommitDecision(ctx context.Context, in *CommitResult, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/spanner.Spanner/CommitDecision", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SpannerServer is the server API for Spanner service.
type SpannerServer interface {
	Read(context.Context, *ReadRequest) (*ReadReply, error)
	Commit(context.Context, *CommitRequest) (*CommitReply, error)
	Abort(context.Context, *AbortRequest) (*Empty, error)
	Prepare(context.Context, *PrepareRequest) (*Empty, error)
	CommitDecision(context.Context, *CommitResult) (*Empty, error)
}

func RegisterSpannerServer(s *grpc.Server, srv SpannerServer) {
	s.RegisterService(&_Spanner_serviceDesc, srv)
}

func _Spanner_Read_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SpannerServer).Read(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spanner.Spanner/Read",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SpannerServer).Read(ctx, req.(*ReadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Spanner_Commit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SpannerServer).Commit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spanner.Spanner/Commit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SpannerServer).Commit(ctx, req.(*CommitRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Spanner_Abort_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AbortRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SpannerServer).Abort(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spanner.Spanner/Abort",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SpannerServer).Abort(ctx, req.(*AbortRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Spanner_Prepare_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PrepareRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SpannerServer).Prepare(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spanner.Spanner/Prepare",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SpannerServer).Prepare(ctx, req.(*PrepareRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Spanner_CommitDecision_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitResult)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SpannerServer).CommitDecision(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/spanner.Spanner/CommitDecision",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SpannerServer).CommitDecision(ctx, req.(*CommitResult))
	}
	return interceptor(ctx, in, info, handler)
}

var _Spanner_serviceDesc = grpc.ServiceDesc{
	ServiceName: "spanner.Spanner",
	HandlerType: (*SpannerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Read",
			Handler:    _Spanner_Read_Handler,
		},
		{
			MethodName: "Commit",
			Handler:    _Spanner_Commit_Handler,
		},
		{
			MethodName: "Abort",
			Handler:    _Spanner_Abort_Handler,
		},
		{
			MethodName: "Prepare",
			Handler:    _Spanner_Prepare_Handler,
		},
		{
			MethodName: "CommitDecision",
			Handler:    _Spanner_CommitDecision_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "transaction.proto",
}

func init() { proto.RegisterFile("transaction.proto", fileDescriptor_2cc4e03d2c28c490) }

var fileDescriptor_2cc4e03d2c28c490 = []byte{
	// 482 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x54, 0x4d, 0x8b, 0xdb, 0x3c,
	0x10, 0xc6, 0xdf, 0xc9, 0x64, 0xdf, 0xbc, 0xad, 0x48, 0xb7, 0xc6, 0xa7, 0xd4, 0xa5, 0xe0, 0x43,
	0x09, 0x25, 0x85, 0xf6, 0xda, 0xd2, 0x0f, 0x58, 0xf6, 0xb2, 0x68, 0x21, 0xf4, 0xaa, 0xb5, 0x75,
	0x30, 0x71, 0x6c, 0x55, 0xd6, 0xa6, 0xf8, 0x5f, 0xf4, 0xbf, 0xf4, 0x0f, 0x96, 0x91, 0x14, 0x35,
	0xeb, 0x6c, 0xa0, 0xf4, 0x36, 0x33, 0xcf, 0xcc, 0x33, 0xf3, 0xcc, 0x58, 0x86, 0xa7, 0x4a, 0xb2,
	0xb6, 0x67, 0xa5, 0xaa, 0xbb, 0x76, 0x25, 0x64, 0xa7, 0x3a, 0x92, 0xf4, 0x82, 0xb5, 0x2d, 0x97,
	0xf9, 0x07, 0xb8, 0xf8, 0x78, 0xd7, 0x49, 0x45, 0xf9, 0xf7, 0x7b, 0xde, 0x2b, 0x32, 0x07, 0xbf,
	0xae, 0x52, 0x6f, 0xe9, 0x15, 0x53, 0xea, 0xd7, 0x15, 0xfa, 0xaa, 0x4f, 0xfd, 0xa5, 0x57, 0x04,
	0xd4, 0x57, 0x3d, 0x79, 0x02, 0x41, 0x79, 0x55, 0xa5, 0x81, 0x0e, 0xa0, 0x99, 0x7f, 0x83, 0x8b,
	0x4f, 0xdd, 0x6e, 0x57, 0x2b, 0xca, 0xfb, 0xfb, 0x46, 0x91, 0x4b, 0x88, 0x4b, 0xed, 0x6b, 0x96,
	0x09, 0xb5, 0x9e, 0x65, 0xf6, 0x47, 0xcc, 0xc1, 0x98, 0x39, 0xfc, 0xc3, 0x9c, 0x40, 0xf4, 0x65,
	0x27, 0xd4, 0x90, 0xdf, 0xc2, 0x8c, 0x72, 0x56, 0x9d, 0x9b, 0x91, 0x40, 0xb8, 0xe5, 0x03, 0x4e,
	0x19, 0x14, 0x53, 0xaa, 0xed, 0xbf, 0x60, 0x7f, 0x0d, 0xf1, 0x9e, 0x35, 0x1b, 0x2e, 0x11, 0xdb,
	0xb3, 0xc6, 0x8e, 0x86, 0xa6, 0x8e, 0x70, 0xa9, 0xcb, 0x31, 0xc2, 0x25, 0x66, 0x6f, 0xf9, 0xb0,
	0x31, 0xd8, 0x96, 0x0f, 0xb6, 0x3d, 0x9a, 0xa7, 0xf5, 0x87, 0x6c, 0xc3, 0xfd, 0x48, 0x36, 0x97,
	0x2e, 0x9b, 0xcb, 0xfc, 0x2b, 0x4c, 0x8d, 0x3c, 0xd1, 0x0c, 0xe4, 0x25, 0x84, 0x7b, 0xd6, 0xf4,
	0xa9, 0xb7, 0x0c, 0x8a, 0xd9, 0xfa, 0xff, 0x95, 0x3d, 0xd4, 0xca, 0xcc, 0x4a, 0x35, 0x48, 0x16,
	0x10, 0x31, 0xbc, 0x9a, 0x66, 0x99, 0x50, 0xe3, 0xe4, 0xbf, 0x3c, 0xf8, 0xef, 0x70, 0x8a, 0x7f,
	0xbc, 0x26, 0x79, 0x01, 0x81, 0xbc, 0xde, 0xa4, 0xe1, 0xa8, 0xbb, 0x51, 0x43, 0x11, 0xc3, 0x94,
	0x1f, 0xd7, 0x9b, 0x34, 0x7a, 0x24, 0x85, 0x35, 0x14, 0x31, 0xec, 0x23, 0x44, 0x1a, 0x2f, 0x83,
	0x22, 0xa2, 0xbe, 0x10, 0x24, 0x83, 0x49, 0xd9, 0x75, 0xb2, 0xba, 0xb9, 0xaa, 0xd2, 0x44, 0x37,
	0x73, 0x7e, 0xfe, 0x0a, 0x66, 0x87, 0xa1, 0x51, 0xff, 0x99, 0xcf, 0x27, 0x17, 0x30, 0xbf, 0x91,
	0x5c, 0x30, 0xc9, 0xcf, 0x89, 0xcb, 0x60, 0x22, 0x4c, 0x46, 0x65, 0xf7, 0xe2, 0x7c, 0x14, 0x2a,
	0xac, 0xd0, 0x88, 0xa2, 0x69, 0x57, 0x11, 0x8e, 0x57, 0x11, 0xb9, 0x55, 0xac, 0x7f, 0xfa, 0x90,
	0xdc, 0x1a, 0x71, 0xe4, 0x0d, 0x84, 0x78, 0x22, 0xb2, 0x70, 0x72, 0x8f, 0x3e, 0xc8, 0x8c, 0x8c,
	0xa2, 0xa8, 0xe3, 0x1d, 0xc4, 0x46, 0x16, 0xb9, 0x74, 0xe8, 0x83, 0xe3, 0x64, 0x8b, 0x93, 0x38,
	0xd6, 0xad, 0x20, 0xd2, 0x0f, 0x92, 0x3c, 0x73, 0xf0, 0xf1, 0x03, 0xcd, 0xe6, 0x2e, 0xac, 0xdf,
	0x06, 0x59, 0x43, 0x62, 0xf7, 0x42, 0x9e, 0x3b, 0xe8, 0xe1, 0xa6, 0x4e, 0x6a, 0xde, 0xc3, 0xdc,
	0xb4, 0xfc, 0xcc, 0xcb, 0xba, 0xaf, 0xbb, 0xf6, 0xa8, 0xd9, 0xf1, 0x5b, 0x1e, 0x17, 0xde, 0xc5,
	0xfa, 0xef, 0xf1, 0xf6, 0x77, 0x00, 0x00, 0x00, 0xff, 0xff, 0xf1, 0xf8, 0x0f, 0x23, 0x52, 0x04,
	0x00, 0x00,
}
