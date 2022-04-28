package log_manager

import (
	fm "file_manager"
	"sync"
)

const (
	UINT64_LEN = 8
)

type LogManager struct {
	file_manager   *fm.FileManager
	log_file       string
	log_page       *fm.Page
	current_blk    *fm.BlockId
	latest_lsn     uint64 //当前日志序列号
	last_saved_lsn uint64 //上次存储到磁盘的日志序列号
	mu             sync.Mutex
}

func (l *LogManager) appendNewBlock() (*fm.BlockId, error) {
	blk, err := l.file_manager.Append(l.log_file)
	if err != nil {
		return nil, err
	}
	/*
		添加日志时从内存的底部往上走，例如内存400字节，日志100字节，那么
		日志将存储在内存的300到400字节处，因此我们需要把当前内存可用底部偏移
		写入头8个字节
	*/
	l.log_page.SetInt(0, uint64(l.file_manager.BlockSize()))
	l.file_manager.Write(&blk, l.log_page)
	return &blk, nil
}

func NewLogManager(file_manager *fm.FileManager, log_file string) (*LogManager, error) {
	log_mgr := LogManager{
		file_manager:   file_manager,
		log_file:       log_file,
		log_page:       fm.NewPageBySize(file_manager.BlockSize()),
		last_saved_lsn: 0,
		latest_lsn:     0,
	}

	log_size, err := file_manager.Size(log_file)
	if err != nil {
		return nil, err
	}

	if log_size == 0 { //如果文件为空则添加新区块
		blk, err := log_mgr.appendNewBlock()
		if err != nil {
			return nil, err
		}
		log_mgr.current_blk = blk
	} else { //文件有数据，则在文件末尾的区块读入内存，最新的日志总会存储在文件末尾
		log_mgr.current_blk = fm.NewBlockId(log_mgr.log_file, log_size-1)
		file_manager.Read(log_mgr.current_blk, log_mgr.log_page)
	}

	return &log_mgr, nil
}

func (l *LogManager) FlushByLSN(lsn uint64) error {
	/*
	将给定编号及其之前的日志写入磁盘，注意这里会把与给定日志在同一个区块，也就是Page中的
	日志也写入磁盘。例如调用FlushLSN(65)表示把编号65及其之前的日志写入磁盘，如果编号为
	66,67的日志也跟65在同一个Page里，那么它们也会被写入磁盘
	*/
	if lsn > l.last_saved_lsn {
		err := l.Flush()
		if err != nil {
			return err
		}
		l.last_saved_lsn = lsn
	}

	return nil
}

func (l *LogManager) Flush() error {
	//将当前区块数据写入写入磁盘
	_, err := l.file_manager.Write(l.current_blk, l.log_page)
	if err != nil {
		return err
	}

	return nil
}

func (l *LogManager) Append(log_record []byte) (uint64, error) {
	//添加日志
	l.mu.Lock()
	defer l.mu.Unlock()

	boundary := l.log_page.GetInt(0) //获得可写入的底部偏移
	record_size := uint64(len(log_record))
	bytes_need := record_size + UINT64_LEN
	var err error
	if int(boundary-bytes_need) < int(UINT64_LEN) {
		//当前容量不够,先将当前日志写入磁盘
		err = l.Flush()
		if err != nil {
			return l.latest_lsn, err
		}
		//生成新区块用于写新数据
		l.current_blk, err = l.appendNewBlock()
		if err != nil {
			return l.latest_lsn, err
		}

		boundary = l.log_page.GetInt(0)
	}

	record_pos := boundary - bytes_need         //我们从底部往上写入
	l.log_page.SetBytes(record_pos, log_record) //设置下次可以写入的位置
	l.log_page.SetInt(0, record_pos)
	l.latest_lsn += 1 //记录新加入日志的编号

	return l.latest_lsn, err
}

func (l *LogManager) Iterator() *LogIterator {
	//生成日志遍历器
	l.Flush()
	return NewLogIterator(l.file_manager, l.current_blk)
}