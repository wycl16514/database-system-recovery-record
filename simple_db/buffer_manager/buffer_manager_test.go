package buffer_manager

import (
	fm "file_manager"
	"github.com/stretchr/testify/require"
	lm "log_manager"
	"testing"
)

func TestBufferManager(t *testing.T) {
	file_manager, _ := fm.NewFileManager("buffertest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	bm := NewBufferManager(file_manager, log_manager, 3)

	buff1, err := bm.Pin(fm.NewBlockId("testfile", 1)) //这块缓存区在后面会被写入磁盘
	require.Nil(t, err)

	p := buff1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buff1.SetModified(1, 0) //这里两个参数先不要管
	buff1.Unpin()

	buff2, err := bm.Pin(fm.NewBlockId("testfile", 2))
	require.Nil(t, err)
	_, err = bm.Pin(fm.NewBlockId("testfile", 3))
	require.Nil(t, err)
	//下面的pin将迫使缓存管理区将buff1的数据写入磁盘
	_, err = bm.Pin(fm.NewBlockId("testfile", 4))
	require.Nil(t, err)
	bm.Unpin(buff2)
	buff2, err = bm.Pin(fm.NewBlockId("testfile", 1))
	require.Nil(t, err)

	p2 := buff2.Contents()
	p2.SetInt(80, 9999)
	buff2.SetModified(1, 0)
	bm.Unpin(buff2) //注意这里不会将buff2的数据写入磁盘

	//将testfile 的区块1读入，并确认buff1的数据的确写入磁盘
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockId("testfile", 1)
	file_manager.Read(b1, page)
	n1 := page.GetInt(80)
	require.Equal(t, n+1, n1)
}
