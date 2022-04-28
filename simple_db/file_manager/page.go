package file_manager 

import (
	"encoding/binary"
)

type Page struct {
	buffer []byte
}

func NewPageBySize(block_size uint64) *Page {
	bytes := make([]byte, block_size)
	return &Page{
		buffer: bytes,
	}
}

func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes,
	}
}

func (p *Page) GetInt(offset uint64) uint64 {
	num := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	return num 
}

func uint64ToByteArray(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b 
}

func (p *Page) SetInt(offset uint64, val uint64) {
	b := uint64ToByteArray(val)
	copy(p.buffer[offset:], b)
}

func (p *Page) GetBytes(offset uint64) []byte {
	len := binary.LittleEndian.Uint64(p.buffer[offset : offset+8]) //读取数组长度
	new_buf := make([]byte, len)
	copy(new_buf, p.buffer[offset+8:])
	return new_buf
}

func (p *Page)SetBytes(offset uint64, b []byte) {
	//首先写入数组的长度，然后再写入数组内容
	length := uint64(len(b))
	len_buf := uint64ToByteArray(length)
    copy(p.buffer[offset:], len_buf) //写入长度
	copy(p.buffer[offset+8:], b)
}

func (p *Page) GetString(offset uint64) string {
	str_bytes := p.GetBytes(offset)
	return string(str_bytes)
}

func (p *Page) SetString(offset uint64, s string) {
	str_bytes := []byte(s)
	p.SetBytes(offset, str_bytes)
}

func (p *Page) MaxLengthForString(s string) uint64 {
	//hello,世界 长度13
	bs := []byte(s)
	uint64_size := 8
	return uint64(uint64_size + len(bs))
}

func (p *Page) contents() []byte{
    return p.buffer
}

