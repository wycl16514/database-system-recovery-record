package log_manager

import (
	fm "file_manager"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func makeRecord(s string, n uint64) []byte {
	//使用page提供接口来设置字节数组的内容
	p := fm.NewPageBySize(1)
	npos := p.MaxLengthForString(s)
	b := make([]byte, npos+UINT64_LEN)
	p = fm.NewPageByBytes(b)
	p.SetString(0, s)
	p.SetInt(npos, n)
	return b
}

func createRecords(lm *LogManager, start uint64, end uint64) {
	for i := start; i <=end; i++ {
		//一条记录包含两个信息，一个是字符串record 一个是数值i
		rec := makeRecord(fmt.Sprintf("record%d", i), i)
		lm.Append(rec)
	}
}

func TestLogManager(t *testing.T) {
	file_manager, _ := fm.NewFileManager("logtest", 400)
	log_manager, err := NewLogManager(file_manager, "logfile")
	require.Nil(t, err)

	createRecords(log_manager, 1, 35)

	iter := log_manager.Iterator()
	rec_num := uint64(35)
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)

		require.Equal(t, fmt.Sprintf("record%d", rec_num), s)
		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		require.Equal(t, val, rec_num)
		rec_num -= 1
	}

	createRecords(log_manager, 36, 70)
	log_manager.FlushByLSN(65)

	iter = log_manager.Iterator()
	rec_num = uint64(70)
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)
		require.Equal(t, fmt.Sprintf("record%d", rec_num), s)
		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		require.Equal(t, val, rec_num)
		rec_num -= 1
	}
}
