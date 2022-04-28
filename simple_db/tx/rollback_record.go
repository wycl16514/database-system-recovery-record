package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)

type RollBackRecord struct {
	tx_num uint64 
}

func NewRollBackRecord(p *fm.Page) *RollBackRecord {
	return &RollBackRecord {
		tx_num : p.GetInt(UINT64_LENGTH),
	}
}

func (r *RollBackRecord) Op() RECORD_TYPE {
	return ROLLBACK
}

func (r *RollBackRecord) TxNumber() uint64 {
	return r.tx_num
}

func(r *RollBackRecord) Undo() {
	//它没有回滚操作
}

func (r *RollBackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.tx_num)
}

func WriteRollBackLog(lgmr *lg.LogManager, tx_num uint64) (uint64, error){
	rec := make([]byte, 2 * UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(ROLLBACK))
	p.SetInt(UINT64_LENGTH, tx_num)

	return lgmr.Append(rec)
}