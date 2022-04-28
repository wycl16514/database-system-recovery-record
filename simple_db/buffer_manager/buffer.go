package buffer_manager

import (
	fmgr "file_manager"
	log "log_manager"
)

type Buffer struct {
	fm       *fmgr.FileManager
	lm       *log.LogManager
	contents *fmgr.Page //用于存储磁盘数据的缓存页面
	blk      *fmgr.BlockId
	pins     uint32 //被引用计数
	txnum    int32  //交易号，暂时忽略其作用
	lsn      uint64 //对应日志号，暂时忽略其作用
}

func NewBuffer(file_mgr *fmgr.FileManager, log_mgr *log.LogManager) *Buffer {
	return &Buffer{
		fm:       file_mgr,
		lm:       log_mgr,
		txnum:    -1,
		contents: fmgr.NewPageBySize(file_mgr.BlockSize()),
	}
}

func (b *Buffer) Contents() *fmgr.Page {
	return b.contents
}

func (b *Buffer) Block() *fmgr.BlockId {
	return b.blk
}

func (b *Buffer) SetModified(txnum int32, lsn uint64) {
	//如果客户修改了页面数据，必须调用该接口通知Buffer
	b.txnum = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	return b.txnum
}

func (b *Buffer) AssignToBlock(block *fmgr.BlockId) {
	//将当前页面分发给其他区块
	b.Flush() //当页面分发给新数据时需要判断当前页面数据是否需要写入磁盘
	b.blk = block
	b.fm.Read(b.blk, b.Contents()) //将对应数据从磁盘读取页面
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txnum >= 0 {
		//当前页面数据已经被修改过，需要写入磁盘
		b.lm.FlushByLSN(b.lsn)          //先将修改操作对应的日志写入
		b.fm.Write(b.blk, b.Contents()) //将数据写入磁盘
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins = b.pins + 1
}

func (b *Buffer) Unpin() {
	b.pins = b.pins - 1
}
