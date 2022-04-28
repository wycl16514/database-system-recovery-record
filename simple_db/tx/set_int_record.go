package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)



type SetIntRecord struct {
	tx_num uint64
	offset uint64
	val    uint64
	blk    *fm.BlockId
}

func NewSetIntRecord(p *fm.Page) *SetIntRecord {
	tpos := uint64(UINT64_LENGTH)
	tx_num := p.GetInt(tpos)
	fpos := tpos + UINT64_LENGTH
	filename := p.GetString(fpos)
	bpos := fpos + p.MaxLengthForString(filename)
	blknum := p.GetInt(bpos)
	blk := fm.NewBlockId(filename, blknum)
	opos := bpos + UINT64_LENGTH
	offset := p.GetInt(opos)
	vpos := opos + UINT64_LENGTH
	val := p.GetInt(vpos) //将日志中的字符串再次写入给定位置

	return &SetIntRecord{
		tx_num: tx_num,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (s *SetIntRecord) Op() RECORD_TYPE {
	return SETSTRING
}

func (s *SetIntRecord) TxNumber() uint64 {
	return s.tx_num
}

func (s *SetIntRecord) ToString() string {
	str := fmt.Sprintf("<SETINT %d %d %d %d>", s.tx_num, s.blk.Number(),
		s.offset, s.val)

	return str
}

func (s *SetIntRecord) Undo(tx TransationInterface) {
	tx.Pin(s.blk)
	tx.SetInt(s.blk, s.offset, s.val, false) //将原来的字符串写回去
	tx.UnPin(s.blk)
}

func WriteSetIntLog(log_manager *lg.LogManager, tx_num uint64,
	blk *fm.BlockId, offset uint64, val uint64) (uint64, error) {
	
	tpos := uint64(UINT64_LENGTH)
	fpos := uint64(tpos + UINT64_LENGTH)
	p := fm.NewPageBySize(1)
	bpos := uint64(fpos + p.MaxLengthForString(blk.FileName()))
	opos := uint64(bpos + UINT64_LENGTH)
	vpos := uint64(opos + UINT64_LENGTH)
	rec_len := uint64(vpos + UINT64_LENGTH)
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(tpos, tx_num)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	return log_manager.Append(rec)
}
