// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: errorspb/tags.proto

package errorspb

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// TagsPayload is the error payload for a WithContext
// marker.
// See errors/contexttags/withcontext.go and the RFC on
// error handling for details.
type TagsPayload struct {
	Tags []TagPayload `protobuf:"bytes,1,rep,name=tags,proto3" json:"tags"`
}

func (m *TagsPayload) Reset()         { *m = TagsPayload{} }
func (m *TagsPayload) String() string { return proto.CompactTextString(m) }
func (*TagsPayload) ProtoMessage()    {}
func (*TagsPayload) Descriptor() ([]byte, []int) {
	return fileDescriptor_2f0d1dc5c54e9f63, []int{0}
}
func (m *TagsPayload) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TagsPayload) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	b = b[:cap(b)]
	n, err := m.MarshalToSizedBuffer(b)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}
func (m *TagsPayload) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TagsPayload.Merge(m, src)
}
func (m *TagsPayload) XXX_Size() int {
	return m.Size()
}
func (m *TagsPayload) XXX_DiscardUnknown() {
	xxx_messageInfo_TagsPayload.DiscardUnknown(m)
}

var xxx_messageInfo_TagsPayload proto.InternalMessageInfo

type TagPayload struct {
	Tag   string `protobuf:"bytes,1,opt,name=tag,proto3" json:"tag,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *TagPayload) Reset()         { *m = TagPayload{} }
func (m *TagPayload) String() string { return proto.CompactTextString(m) }
func (*TagPayload) ProtoMessage()    {}
func (*TagPayload) Descriptor() ([]byte, []int) {
	return fileDescriptor_2f0d1dc5c54e9f63, []int{1}
}
func (m *TagPayload) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TagPayload) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	b = b[:cap(b)]
	n, err := m.MarshalToSizedBuffer(b)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}
func (m *TagPayload) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TagPayload.Merge(m, src)
}
func (m *TagPayload) XXX_Size() int {
	return m.Size()
}
func (m *TagPayload) XXX_DiscardUnknown() {
	xxx_messageInfo_TagPayload.DiscardUnknown(m)
}

var xxx_messageInfo_TagPayload proto.InternalMessageInfo

func init() {
	proto.RegisterType((*TagsPayload)(nil), "cockroach.errorspb.TagsPayload")
	proto.RegisterType((*TagPayload)(nil), "cockroach.errorspb.TagPayload")
}

func init() { proto.RegisterFile("errorspb/tags.proto", fileDescriptor_2f0d1dc5c54e9f63) }

var fileDescriptor_2f0d1dc5c54e9f63 = []byte{
	// 201 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x4e, 0x2d, 0x2a, 0xca,
	0x2f, 0x2a, 0x2e, 0x48, 0xd2, 0x2f, 0x49, 0x4c, 0x2f, 0xd6, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17,
	0x12, 0x4a, 0xce, 0x4f, 0xce, 0x2e, 0xca, 0x4f, 0x4c, 0xce, 0xd0, 0x83, 0x49, 0x4b, 0x89, 0xa4,
	0xe7, 0xa7, 0xe7, 0x83, 0xa5, 0xf5, 0x41, 0x2c, 0x88, 0x4a, 0x25, 0x77, 0x2e, 0xee, 0x90, 0xc4,
	0xf4, 0xe2, 0x80, 0xc4, 0xca, 0x9c, 0xfc, 0xc4, 0x14, 0x21, 0x0b, 0x2e, 0x16, 0x90, 0x31, 0x12,
	0x8c, 0x0a, 0xcc, 0x1a, 0xdc, 0x46, 0x72, 0x7a, 0x98, 0xe6, 0xe8, 0x85, 0x24, 0xa6, 0x43, 0x55,
	0x3b, 0xb1, 0x9c, 0xb8, 0x27, 0xcf, 0x10, 0x04, 0xd6, 0xa1, 0x64, 0xc2, 0xc5, 0x85, 0x90, 0x11,
	0x12, 0xe0, 0x62, 0x2e, 0x49, 0x4c, 0x97, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0c, 0x02, 0x31, 0x85,
	0x44, 0xb8, 0x58, 0xcb, 0x12, 0x73, 0x4a, 0x53, 0x25, 0x98, 0xc0, 0x62, 0x10, 0x8e, 0x93, 0xd6,
	0x89, 0x87, 0x72, 0x0c, 0x27, 0x1e, 0xc9, 0x31, 0x5e, 0x78, 0x24, 0xc7, 0x78, 0xe3, 0x91, 0x1c,
	0xe3, 0x83, 0x47, 0x72, 0x8c, 0x13, 0x1e, 0xcb, 0x31, 0x5c, 0x78, 0x2c, 0xc7, 0x70, 0xe3, 0xb1,
	0x1c, 0x43, 0x14, 0x07, 0xcc, 0xe2, 0x24, 0x36, 0xb0, 0x8b, 0x8d, 0x01, 0x01, 0x00, 0x00, 0xff,
	0xff, 0x0a, 0x90, 0xd1, 0xc9, 0xf2, 0x00, 0x00, 0x00,
}

func (m *TagsPayload) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TagsPayload) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TagsPayload) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Tags) > 0 {
		for iNdEx := len(m.Tags) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Tags[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTags(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *TagPayload) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TagPayload) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TagPayload) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Value) > 0 {
		i -= len(m.Value)
		copy(dAtA[i:], m.Value)
		i = encodeVarintTags(dAtA, i, uint64(len(m.Value)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Tag) > 0 {
		i -= len(m.Tag)
		copy(dAtA[i:], m.Tag)
		i = encodeVarintTags(dAtA, i, uint64(len(m.Tag)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintTags(dAtA []byte, offset int, v uint64) int {
	offset -= sovTags(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *TagsPayload) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Tags) > 0 {
		for _, e := range m.Tags {
			l = e.Size()
			n += 1 + l + sovTags(uint64(l))
		}
	}
	return n
}

func (m *TagPayload) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Tag)
	if l > 0 {
		n += 1 + l + sovTags(uint64(l))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovTags(uint64(l))
	}
	return n
}

func sovTags(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTags(x uint64) (n int) {
	return sovTags(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *TagsPayload) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTags
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TagsPayload: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TagsPayload: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tags", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTags
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTags
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTags
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Tags = append(m.Tags, TagPayload{})
			if err := m.Tags[len(m.Tags)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTags(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTags
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *TagPayload) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTags
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TagPayload: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TagPayload: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tag", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTags
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTags
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTags
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Tag = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTags
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTags
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTags
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTags(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthTags
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTags(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTags
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTags
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTags
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTags
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTags
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTags
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTags        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTags          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTags = fmt.Errorf("proto: unexpected end of group")
)
