package main

import (
	//"encoding/binary"
	fm "file_manager"
	lm "log_manager"

	bmg "buffer_manager"

	"fmt"

	"tx"
)

func main() {
	file_manager, _ := fm.NewFileManager("buffertest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	bm := bmg.NewBufferManager(file_manager, log_manager, 3)

	str := "original string"
	dummy_blk := fm.NewBlockId("dummy_id", 1)
	tx_num := uint64(1)
	offset := uint64(13)
	tx.WriteSetStringLog(log_manager, tx_num, dummy_blk, offset, str)
	pp := fm.NewPageBySize(400)
	pp.SetString(offset, str)
	iter := log_manager.Iterator()
	rec := iter.Next()
	log_p := fm.NewPageByBytes(rec)
	setStrRec := tx.NewSetStringRecord(log_p)
	fmt.Println(setStrRec.ToString())
	//txStub := fm.NewTxStub(pp)

	blk := fm.NewBlockId("testfile", 1)
	buff1, _ := bm.Pin(blk) //这块缓存区在后面会被写入磁盘

	p := buff1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buff1.SetModified(1, 0) //这里两个参数先不要管
	bm.Unpin(buff1)
	buff2, _ := bm.Pin(fm.NewBlockId("testfile", 2))

	_, _ = bm.Pin(fm.NewBlockId("testfile", 3))

	//下面的pin将迫使缓存管理区将buff1的数据写入磁盘
	_, _ = bm.Pin(fm.NewBlockId("testfile", 4))

	bm.Unpin(buff2)
	buff2, _ = bm.Pin(fm.NewBlockId("testfile", 1))

	p2 := buff2.Contents()
	p2.SetInt(80, 9999)
	buff2.SetModified(1, 0)
	bm.Unpin(buff2) //注意这里不会将buff2的数据写入磁盘

	//将testfile 的区块1读入，并确认buff1的数据的确写入磁盘
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockId("testfile", 1)
	file_manager.Read(b1, page)
	n1 := page.GetInt(80)
	fmt.Println(n1 == n+1)

}
