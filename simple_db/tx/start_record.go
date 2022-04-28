package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)

type StartRecord struct {
	tx_num      uint64
	log_manager *lg.LogManager
}

func NewStartRecord(p *fm.Page, log_manager *lg.LogManager) *StartRecord {
	//p的头8字节对应日志的类型，从偏移8开始对应交易号
	tx_num := p.GetInt(UINT64_LENGTH)
	return &StartRecord{
		tx_num:      tx_num,
		log_manager: log_manager,
	}
}

func (s *StartRecord) Op() RECORD_TYPE {
	return START
}

func (s *StartRecord) TxNumber() uint64 {
	return s.tx_num
}

func (s *StartRecord) Undo() {
	//该记录没有回滚操作的必要
}

func (s *StartRecord) ToString() string {
	str := fmt.Sprintf("<START %d>", s.tx_num)
	return str
}

func (s *StartRecord) WriteToLog() (uint64, error) {
	//日志写的不是字符串而是二进制数值
	record := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(record)
	p.SetInt(uint64(0), uint64(START))
	p.SetInt(UINT64_LENGTH, s.tx_num)
	return s.log_manager.Append(record)
}
