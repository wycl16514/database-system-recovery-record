package log_manager

import (
	fm "file_manager"
)



/*
LogIterator用于遍历给定区块内的记录,由于记录从底部往上写，因此记录1,2,3,4写入后在区块的排列为
4,3,2,1，因此LogIterator会从上往下遍历记录，于是得到的记录就是4,3,2,1
*/

type LogIterator struct {
	file_manager *fm.FileManager 
	blk         *fm.BlockId 
    p           *fm.Page 
	current_pos uint64 
	boundary    uint64 
}

func NewLogIterator(file_manager *fm.FileManager, blk *fm.BlockId) *LogIterator{
    it := LogIterator{
		file_manager: file_manager,
		blk: blk , 
	}

	//现将给定区块的数据读入
	it.p = fm.NewPageBySize(file_manager.BlockSize())
	err := it.moveToBlock(blk) 
    if err != nil {
		return nil 
	}
	return &it 
}

func (l *LogIterator) moveToBlock(blk *fm.BlockId) error {
	//打开存储日志数据的文件，遍历到给定区块，将数据读入内存
	_, err := l.file_manager.Read(blk, l.p)
	if err != nil {
		return err 
	}

	//获得日志的起始地址
	l.boundary = l.p.GetInt(0)
	l.current_pos = l.boundary
	return nil
}

func (l *LogIterator) Next() []byte {
	//先读取最新日志，也就是编号大的，然后依次读取编号小的
	if l.current_pos == l.file_manager.BlockSize() {
		l.blk = fm.NewBlockId(l.blk.FileName(), l.blk.Number() - 1)
		l.moveToBlock(l.blk)
	}

	record := l.p.GetBytes(l.current_pos)
	l.current_pos += UINT64_LEN + uint64(len(record))

	return record 
}

func (l *LogIterator) HasNext() bool {
	//如果当前偏移位置小于区块大那么还有数据可以从当前区块读取
	//如果当前区块数据已经全部读完，但是区块号不为0，那么可以读取前面区块获得老的日志数据
	return l.current_pos < l.file_manager.BlockSize() || l.blk.Number() > 0
}
