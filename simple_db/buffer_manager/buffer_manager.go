package buffer_manager

import (
	fm "file_manager"
	lm "log_manager"
	"sync"
	"errors"
	"time"
)

const (
	MAX_TIME = 3 //分配页面时最多等待3秒
)

type BufferManager struct {
	buffer_pool []*Buffer
	num_available uint32
	mu sync.Mutex 
}

func NewBufferManager(fm *fm.FileManager, lm *lm.LogManager, num_buffers uint32) *BufferManager {
    buffer_manager := &BufferManager{
		num_available: num_buffers,
	}
	for i := uint32(0); i < num_buffers; i++ {
		buffer := NewBuffer(fm, lm)
		buffer_manager.buffer_pool = append(buffer_manager.buffer_pool, buffer)
	}

	return buffer_manager
}

func (b *BufferManager) Available() uint32 {
	//当前可用缓存页面数量
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.num_available
}

func (b *BufferManager) FlushAll(txnum int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	//将给定交易的读写数据全部写入磁盘
	for _, buff := range b.buffer_pool {
		if buff.ModifyingTx() == txnum {
			buff.Flush()
		}
	}
}

func (b *BufferManager) Pin(blk *fm.BlockId) (*Buffer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

    start := time.Now()
	buff := b.tryPin(blk)
	for buff == nil && b.waitingTooLong(start) == false {
		//如果无法获得缓存页面，那么让调用者等待一段时间后再次尝试
		time.Sleep(MAX_TIME * time.Second)
		buff = b.tryPin(blk)
		if buff == nil {
			return nil, errors.New("No buffer available , cafule for dead lock")
		}
	}

	return buff, nil 
}

func (b *BufferManager) Unpin(buff *Buffer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if buff == nil {
		return 
	}

	buff.Unpin()
	if !buff.IsPinned() {
		b.num_available = b.num_available + 1 
		//notifyAll() //唤醒所有等待它的线程,等到设计并发管理器时再做处理
	}
}

func (b *BufferManager) waitingTooLong(start time.Time) bool{
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_TIME {
		return true
	}

	return false
}

func (b *BufferManager) tryPin(blk *fm.BlockId) *Buffer {
	//首先看给定的区块是否已经被读入某个缓存页
	buff := b.findExistingBuffer(blk)
	if buff == nil {
		//查看是否还有可用缓存页，然后将区块数据写入
		buff = b.chooseUnpinBuffer()
		if buff == nil {
			return nil 
		}
		buff.AssignToBlock(blk)
	}

	if buff.IsPinned() == false {
		b.num_available = b.num_available - 1
	}

	buff.Pin()
	return buff
}

func (b *BufferManager) findExistingBuffer(blk *fm.BlockId) *Buffer{
	//查看当前请求的区块是否已经被加载到了某个缓存页，如果是，那么直接返回即可
	for _, buffer := range b.buffer_pool {
		block := buffer.Block()
		if block != nil && block.Equal(blk) {
			return buffer 
		}
	}

	return nil 
}

func (b *BufferManager) chooseUnpinBuffer() *Buffer {
	//选取一个没有被使用的缓存页
	for _, buffer := range b.buffer_pool {
		if !buffer.IsPinned() {
			return buffer 
		}
	}

	return nil 
}