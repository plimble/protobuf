// Code generated by protoc-gen-go.
// source: github.com/plimble/protobuf/ptypes/any/any.proto
// DO NOT EDIT!

/*
Package any is a generated protocol buffer package.

It is generated from these files:
	github.com/plimble/protobuf/ptypes/any/any.proto

It has these top-level messages:
	Any
*/
package any

import proto "github.com/plimble/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// `Any` contains an arbitrary serialized protocol buffer message along with a
// URL that describes the type of the serialized message.
//
// Protobuf library provides support to pack/unpack Any values in the form
// of utility functions or additional generated methods of the Any type.
//
// Example 1: Pack and unpack a message in C++.
//
//     Foo foo = ...;
//     Any any;
//     any.PackFrom(foo);
//     ...
//     if (any.UnpackTo(&foo)) {
//       ...
//     }
//
// Example 2: Pack and unpack a message in Java.
//
//     Foo foo = ...;
//     Any any = Any.pack(foo);
//     ...
//     if (any.is(Foo.class)) {
//       foo = any.unpack(Foo.class);
//     }
//
//  Example 3: Pack and unpack a message in Python.
//
//     foo = Foo(...)
//     any = Any()
//     any.Pack(foo)
//     ...
//     if any.Is(Foo.DESCRIPTOR):
//       any.Unpack(foo)
//       ...
//
// The pack methods provided by protobuf library will by default use
// 'type.googleapis.com/full.type.name' as the type URL and the unpack
// methods only use the fully qualified type name after the last '/'
// in the type URL, for example "foo.bar.com/x/y.z" will yield type
// name "y.z".
//
//
// JSON
// ====
// The JSON representation of an `Any` value uses the regular
// representation of the deserialized, embedded message, with an
// additional field `@type` which contains the type URL. Example:
//
//     package google.profile;
//     message Person {
//       string first_name = 1;
//       string last_name = 2;
//     }
//
//     {
//       "@type": "type.googleapis.com/google.profile.Person",
//       "firstName": <string>,
//       "lastName": <string>
//     }
//
// If the embedded message type is well-known and has a custom JSON
// representation, that representation will be embedded adding a field
// `value` which holds the custom JSON in addition to the `@type`
// field. Example (for message [google.protobuf.Duration][]):
//
//     {
//       "@type": "type.googleapis.com/google.protobuf.Duration",
//       "value": "1.212s"
//     }
//
type Any struct {
	// A URL/resource name whose content describes the type of the
	// serialized protocol buffer message.
	//
	// For URLs which use the scheme `http`, `https`, or no scheme, the
	// following restrictions and interpretations apply:
	//
	// * If no scheme is provided, `https` is assumed.
	// * The last segment of the URL's path must represent the fully
	//   qualified name of the type (as in `path/google.protobuf.Duration`).
	//   The name should be in a canonical form (e.g., leading "." is
	//   not accepted).
	// * An HTTP GET on the URL must yield a [google.protobuf.Type][]
	//   value in binary format, or produce an error.
	// * Applications are allowed to cache lookup results based on the
	//   URL, or have them precompiled into a binary to avoid any
	//   lookup. Therefore, binary compatibility needs to be preserved
	//   on changes to types. (Use versioned type names to manage
	//   breaking changes.)
	//
	// Schemes other than `http`, `https` (or the empty scheme) might be
	// used with implementation specific semantics.
	//
	TypeUrl string `protobuf:"bytes,1,opt,name=type_url,json=typeUrl" json:"type_url,omitempty"`
	// Must be a valid serialized protocol buffer of the above specified type.
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Any) Reset()                    { *m = Any{} }
func (m *Any) String() string            { return proto.CompactTextString(m) }
func (*Any) ProtoMessage()               {}
func (*Any) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }
func (*Any) XXX_WellKnownType() string   { return "Any" }

func init() {
	proto.RegisterType((*Any)(nil), "google.protobuf.Any")
}

func init() { proto.RegisterFile("github.com/plimble/protobuf/ptypes/any/any.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 187 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xd2, 0x4f, 0xcf, 0x2c, 0xc9,
	0x28, 0x4d, 0xd2, 0x4b, 0xce, 0xcf, 0xd5, 0x4f, 0xcf, 0xcf, 0x49, 0xcc, 0x4b, 0xd7, 0x2f, 0x28,
	0xca, 0x2f, 0xc9, 0x4f, 0x2a, 0x4d, 0xd3, 0x2f, 0x28, 0xa9, 0x2c, 0x48, 0x2d, 0xd6, 0x4f, 0xcc,
	0xab, 0x04, 0x61, 0x3d, 0xb0, 0xb8, 0x10, 0x7f, 0x7a, 0x7e, 0x7e, 0x7a, 0x4e, 0xaa, 0x1e, 0x4c,
	0x95, 0x92, 0x19, 0x17, 0xb3, 0x63, 0x5e, 0xa5, 0x90, 0x24, 0x17, 0x07, 0x48, 0x79, 0x7c, 0x69,
	0x51, 0x8e, 0x04, 0xa3, 0x02, 0xa3, 0x06, 0x67, 0x10, 0x3b, 0x88, 0x1f, 0x5a, 0x94, 0x23, 0x24,
	0xc2, 0xc5, 0x5a, 0x96, 0x98, 0x53, 0x9a, 0x2a, 0xc1, 0xa4, 0xc0, 0xa8, 0xc1, 0x13, 0x04, 0xe1,
	0x38, 0x15, 0x71, 0x09, 0x27, 0xe7, 0xe7, 0xea, 0xa1, 0x19, 0xe7, 0xc4, 0xe1, 0x98, 0x57, 0x19,
	0x00, 0xe2, 0x04, 0x30, 0x46, 0xa9, 0x12, 0xe5, 0xb8, 0x05, 0x8c, 0x8c, 0x8b, 0x98, 0x98, 0xdd,
	0x03, 0x9c, 0x56, 0x31, 0xc9, 0xb9, 0x43, 0x4c, 0x0b, 0x80, 0xaa, 0xd2, 0x0b, 0x4f, 0xcd, 0xc9,
	0xf1, 0xce, 0xcb, 0x2f, 0xcf, 0x0b, 0x01, 0xa9, 0x4e, 0x62, 0x03, 0x6b, 0x37, 0x06, 0x04, 0x00,
	0x00, 0xff, 0xff, 0xc6, 0x4d, 0x03, 0x23, 0xf6, 0x00, 0x00, 0x00,
}
