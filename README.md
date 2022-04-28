数据库系统有一个及其重要的功能，那就是要保持数据一致性。在用户往数据库写入数据后，如果数据库返回写入成功，那么数据就必须永久性的保存在磁盘上。此外作为一个系统，它必须具备自恢复功能，也就是如果系统出现意外奔溃，无论是内部错误，还是外部原因，例如突然断电等，系统都必须要保持数据的一致性。

例如我们从数据库中订购一张机票，假设机票数量正确减一，但还没扣款，此时系统突然奔溃，如果系统没有预防措施就会导致数据出现不一致性，也就是机票出票数量和相应的支付款项不一致，没有容错性的数据库系统就不会有市场，本节的目的是设计恢复机制，确保数据在任何突如其来的意外情况下依然保持数据一致性。

因此数据库系统必须遵守ACID原则，他们分别是atomicity, consistency, isolation, durability:
atomicity: 其意思是任何数据操作要不完全执行，要不就一点作用也没有。数据库中有一个叫“交易”的概念，也就是transation，它表示一系列必须全部完成的读写操作，必须是序列化的，也就是交易所给定的执行步骤在运行时不能被打断，或者是中间突然插入其他交易的步骤，所以它也叫原子化。

consistency:意思是任何交易都必须确保数据处于一致状态。也就是说交易中所定义的一系列读写步骤必须作为一个统一的单元进行执行，当交易进行时，数据库系统的运行状态就好像是一个单线程应用。

isolation:意思是交易执行时，它的执行环境或者上下文使得它好像是整个系统唯一正在运行的交易，实际上同一时刻可能有多个交易正在执行，但系统必须保证每个交易运行时就好像整个系统只有它一个。

durability: 思思是任何被执行完毕的交易所更改的数据必须持久化的存储在磁盘或相关介质上。

要保证ACID原则的执行，我们需要设计两个模块，分别是恢复管理器和并发管理器，前者确保系统在出现意外奔溃或关闭时，数据依然处于一致性状态，后者确保多个交易在同时进行时，相互之间不产生干扰，本节先着重前者的实现。

恢复管理器的功能依赖于日志，系统在将数据写入磁盘前，必须将写入前的数据和写入后的数据记录在日志中，这样恢复管理器才能从日志中将数据还原，相应的日志格式如下：
```
<START, 1>
<COMMIT , 1>
<START , 2>
<SETINT, 2, testfile, 1, 80, 1 ,2>
<SETSTRING, 2, testfile, 1, 40, one, one!>
<COMMIT, 2>
<START, 3>
<SETINT, 3, testfile, 1, 80, 2, 9999>
<ROLLBACK, 3>
<START, 4>
<COMMIT, 4>
```
上面日志的逻辑为<START, 1>表示系统启动一次交易，交易对应的号码为1， 从上面日志可以看到，交易1启动后什么数据都没有写入就直接完成交易。然后系统启动交易2，日志<SETINT, 2,  testfile, 1, 80, 1, 2>表示交易2向文件testfile写入整形数据，写入的区块号为1，在区块内部的偏移为80，在写入前给定位置的数据为数值1，写入后数据变为2。我们可以发现有了这样的日志，恢复管理器就能执行灾后恢复，例如系统在进行交易2时，在执行SETINT操作时，系统突然奔溃，下次重启后回复管理器读取日志，它会发现有<START, 2>但是找不到对应的<COMMIT,2>于是这时它就明白交易2在进行过程中发送了错误使得交易没有完成，此时它就能执行恢复，它读取日志<SETINT, 2,  testfile, 1, 80, 1, 2>,于是就能知道交易2在文件testfile的区块1中，偏移80字节处写入了数值2，在写入前数值为1，于是它就能将数值1重新写入到testfile文件区块1偏移为80字节位置，于是就相当于恢复了原来的写操作。

从上面日志可以看出，对于交易的记录总共有四种类型，分别为start, commit, rollback, 和update，update分为两种情况，也就是SETINT，写入整形数值，SETSTRING，写入字符串。  这里需要注意的是，系统为了支持高并发就会允许多个交易同时进行，于是有关交易的日志就会交叉出现在日志中，例如有可能<START 2>，之后就会跟着<START 3>等等，不同交易的日志记录交叉出现不会影响我们的识别逻辑，因为同一个交易不同时间操作一定会从上到下的呈现。

有了日志系统也能支持回滚操作，假设交易3写入数值9999到文件testfile区块号为1，偏移为80的位置，那么它会先生成日志<SETINT, 3, testfile, 1, 80, 2, 9999>，然后它立刻进行回滚操作，这时候我们可以从日志中发现，写入9999前，对应位置的数值是2，于是我们只要把数值2重新写入区块号为1偏移为80的位置就相当于还原了写入操作。因此回滚操作的步骤如下：

1，获得要执行回滚操作的交易号x
2，从下往上读取日志，如果记录对应的交易号不是x，那么忽略，继续往上读取
3，如果交易号是x，读取日志中数据写入前的数据，
4，将写入前的数据重新写入到日志记录的位置，继续执行步骤2

注意执行回滚时，我们要从日志文件的底部往前读，因为一个地方的数值可能会被写入多次，假设testfile区块号为1，偏移为80的地方，在第一次写入前数值为1，假设交易对这个位置分别写入了3次，写入的数值为2,3,4，那么回滚后给定位置的数值应该恢复为1，要实现这个效果，我们必须要从日志的底部往上读取。

我们再看容灾恢复，每次系统启动时它首先要执行灾后恢复工作。其目的是要保持数据的“一致性”，所谓“一致性”是指，所有没有执行commit的交易，它所写入的数据都要恢复为写入前的数据，所有已经执行了commit的交易，一定要确保写入的数据都已经存储到磁盘上。第二种情况完全有可能发生，因为数据会首先写入内存，然后系统会根据具体情况有选择的将数据写入磁盘，这是出于效率考虑，假设交易执行了commit操作，部分写入的数据还存储在内存中，此时系统突然奔溃，那么这部分在内存中的数据就不会写入到磁盘。

在恢复管理器看来，只要日志中有了COMMIT记录，那么交易就完成了，但是它并不能保证交易写入的数据都已经存储在磁盘上了。所以恢复管理器有可能需要将日志中已经完成的交易再执行一次。

从上面描述可以看到，恢复管理器严重依赖于日志，因此我们必须确保在数据写入前，日志必须要先完成，如果顺序倒过来，先写入数据，再写入日志，如果写入数据后系统突然奔溃，那么写入信息就不会记录在日志里，那么恢复管理器就不能执行恢复功能了。要执行交易的重新执行功能，需要执行的步骤如下：
1，从头开始读取日志
2，当遇到"\<START X\>" 类似的日志时，记录下当前交易号。
3，如果读到<SETINT, X, testfile , 1, 80, 1,2>的日志时，将数值2再次写入到文件testfile,区块号为1，偏移为80的地方

恢复管理器在重新执行交易时，它需要对日志进行两次扫描，第一次扫描是从底部往上读取日志，这样恢复管理器才能知道哪些交易已经执行了commit操作，同时执行undo功能，也就是将没有执行commit操作的交易修改进行恢复，于是第一次扫描时它把那些已经执行commit操作的交易号记录下来，第二次扫描则是从日志的头开始读取，一旦读到\<START x\>这样的日志时，它会查找x是否是第一次扫描时已经记录下来的执行了commit操作的日志，如果是，那么它将x对应的SETINT,SETSTRING操作再执行一次，然后要求缓存管理器立马将写入的数据存储到磁盘上。

问题在于第二步也就是重新执行交易对应操作可能不必要，因为交易修改极有可能已经写入到磁盘，如果再次进行磁盘写操作就会降低系统效率。我们可以避免第二步重写操作，只要我们让缓存管理器把所有修改先写入磁盘，然后再把commit记录写入日志即可，这样带来的代价是由于系统要频繁的写入磁盘由此会降低系统效率。同时我们也能让第一步变得没有必要，只要我们确保交易在执行commit前数据不写入磁盘即可，但如此带来的代价是，缓存的数据不写入磁盘，那么系统的吞吐量就会下降，因为缓存数据不写入磁盘，缓存页面就不能重新分配，于是新的交易就无法执行，因为得不到缓存。

现在还存在一个问题是，系统运行久了日志会非常庞大，它的数量甚至比数据要大，如果每次恢复都要读取日志，那么恢复流程会越来越久。因此恢复管理器在执行时，它只能读取部分日志，问题在于它如何决定读取多少日志数据呢。它只需要知道两个条件就能停止继续读取日志：
1，当前读取位置以上的日志都对应已经执行了commit操作的交易
2，所有已经执行commit的交易，其数据都已经写入到了磁盘。

当恢复管理器知道第一点，那么它就不用在执行回滚操作，知道第二点就不需要再将已经commit的操作再次执行。为了满足满足以上两点，系统需要执行以下步骤：
1，停止启动新的交易
2，等待当前所有正在进行的交易全部完成
3，将所有修改的缓存写入磁盘
4，插入一个中断点日志表示上面操作已经完成，并将中断点日志写入磁盘文件
5，开始接收新的交易

我们看一个具体例子：
<START, 0>
<SETINT, 0, junk, 33, 8, 542, 543>
<START, 1>
<START, 2>
<SETSTRING, 2, junk, 44,  20, hello, ciao>
//在这里启动上面步骤，停止接收新的交易
<SETINT, 0,  junk, 33, 12, joe, joseph>
<COMMIT, 0>
//交易3准备发起，但是它只能等待
<SETINT, 2 , junk, 66, 8, 0, 116>
<COMMIT, 2>
\<CHECKPONT\> //中断点，上面的日志不用再考虑,下面交易3可以启动
<START, 3>
<SETINT, 3, junk, 33, 8, 43, 120>

从上面日志中，恢复管理器从下往上读取时，只要看到checkpoint记录就可以停止了。这种做法也有明显缺陷，那就是整个系统必须要停止一段时间，这对于数据吞吐量大的情形是不可接受的。为了处理这个问题，我们对原来算法进行改进，其步骤如下：
1，假设当前正在运行的交易为1,2,3，。。。。k
2，停止创建新的交易
3，将所有修改的缓存页面数据写入磁盘
4，将当前正在进行的交易号记录下来，例如<NQCKPT 1,2,3,4...k>
5，运行新交易创建

有了上面步骤后，恢复管理器在执行恢复时，依然要从底部往上读取日志，那么它如何知道怎么停止继续读取日志呢，当它读取到NQCHKPT这条记录时，它把记录中的交易号用一个队列存储起来，然后继续往上读取日志，当它读取到\<START x\>这样的日志时，它查看x是否在队列中，如果在，那么就将它从队列中去除，这个步骤一直进行到队列为空，此时它就不用再继续读取日志了。

这个办法能大大缩短系统停止交易创建的时间，我们看个具体例子：
```
<START, 0>
<SETINT, 0, junk, 33, 8, 542, 543>
<START, 1>
<START, 2>
<COMMIT, 1>
<SETSTRING, 2, junk, 44, 20, hello, ciao>
<NQCKPT, 0, 2>
<SETSTRING, 0, junk, 33, 12, joe, joseph>
<COMMIT, 0>
<START, 3>
<SETINT, 2, junk, 66, 8, 0, 116>
<SETINT, 3, junk, 33, 8, 543, 120>
```
恢复管理器在执行恢复任务时，依然从底部往上读取，当它读取最后一条日志<SETINT, 3...>时发现交易3没有对应的COMMIT日志，于是系统知道它没有完成，于是执行回滚操作。读取<SETINT, 2...>时同样执行回滚操作。当读取到<COMMIT, 0>时，将0加入交易完成列表，注意系统并不能确定交易3的对应的数据是否都已经写入磁盘，因此需要找到交易0的起始处，让后把所有写入缓存的日志重新写入磁盘。

因此系统继续往上读取<SETSTRING, 0...>，此时系统知道交易0已经执行commit，所以忽略这条日志。继续往上读，读取到<SETSTRING, 2...>时，执行回滚操作，然后继续往上读取，一直读到<START, 2>时停止继续往上读，此时它开始从这里往下读，把所有有关交易0的操作对应的数据再次执行，然后写入磁盘，往下读取一直遇到<COMMIT, 0>时停止。

理论已经够多了，我们需要进入代码设计。首先在工程目录下创建一个子文件夹叫tx，它里面包含了所有与交易相关的模块，例如恢复管理器和并发管理器，后者我们在下一节讨论。首先我们先定义交易对象的接口，等完成并发管理器完成后再讨论它的实现，增加一个文件叫interface.go，添加代码如下：
```
package tx

import(
	fm "file_manager"
	lg "log_manager"
)

type TransationInterface interface {
	Commit()
	RollBack()
	Recover()
    Pin(blk *fm.BlockId)
	UnPin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) uint64 
	GetString(blk *fm.BlockId, offset uint64) string 
	SetInt(blk *fm.BlockId, offset uint64, val uint64, okToLog bool)
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool)
	AvailableBuffers() uint64
	Size(filename string) uint64 
	Append(filename string) *fm.BlockId
	BlockSize() uint64
}
```
从上面代码看到，“交易”接口跟原先实现的Buffer接口很像，它其实是对Buffer接口的封装，在调用后者前，先使用恢复管理器和并发管理器做一些前提工作，交易对象的实现在后面再实现。

首先我们先看恢复日志的实现，从前面例子看，总共有六种用于恢复的日志，分别为START, COMMIT, ROLLBACK, SETINT, SETSTRING, CHECKPOINT，所以我们先设定日志记录的接口，然后针对每种记录类型再实现对应实例，继续在interface.go中添加内容如下：
```
type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota 
	START 
	COMMIT 
	ROLLBACK 
	SETINT 
	SETSTRING 
)

const (
	UINT64_LENGTH = 8
)

type LogRecordInterface interface {
	Op() RECORD_TYPE //返回记录的类别
	TxNumber() uint32 //对应交易的号码
	Undo(tx TransationInterface) //回滚操作
	ToString() string //获得记录的字符串内容
}
```
接下来我们分别创建继承LogRecordInterface接口的记录实例，首先是start 记录，增加文件start_record.go,添加内容如下：
```
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

```
它的逻辑很简单，只需要关注ToString()和WriteToLog两个函数，前者返回其字符串格式，后者将START常量和交易号以二进制的形式写入缓存页面，下面我们运行上面的代码看看，增加record_test.go，添加代码如下：
```
package tx

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	fm "file_manager"
	lm "log_manager"
	"encoding/binary"
)

func TestStartRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "record_file")
	
	tx_num := uint64(13) //交易号
	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(8, uint64(tx_num)) 
	start_record := NewStartRecord(p, log_manager)
	expected_str := fmt.Sprintf("<START %d>", tx_num)
	require.Equal(t, expected_str, start_record.ToString())

	_, err := start_record.WriteToLog()
	require.Nil(t, err)

	iter := log_manager.Iterator()
	//检查写入的日志是否符号预期
	rec := iter.Next()
    rec_op := binary.LittleEndian.Uint64(rec[0:8])
	rec_tx_num :=  binary.LittleEndian.Uint64(rec[8:len(rec)])
	require.Equal(t, rec_op, START)
	require.Equal(t, rec_tx_num, tx_num)
}
```
在测试中，我们初始化了StartRecord实例，然后调用其ToString和WriteToLog两个接口，然后检验其返回或者是写入缓存的数据是否正确，上面测试用例可以通过，因此我们当前实现的StartRecord逻辑能保证基本正确性。

接下来我们继续实现其他几种恢复日志，首先是SETSTRING格式的日志，创建set_string_record.go，实现代码如下：
```
package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)

/*
在理论上一条SETSTRING记录有7个字段，例如<SETSTRING, 0, junk, 33, 12, joe, joseph>，
在实现上我们只用6个字段，上面的记录实际上对应了两次字符串的写入，第一次写入字符串"joseph"，
第二次写入joe，因此在实现上它对应了两条包含六个字段的记录：
<SETSTRING, 0, junk, 33, 12, joseph>
....
<SETSTRING, 0, junk, 33, 12, joe>
回忆一下前面我们实现日志，日志是从下往上写，也就是<SETSTRING, 0, junk, 33, 12, joe>会写在前面，
<SETSTRING, 0, junk, 33, 12, joseph>会写在后面，
在回滚的时候，我们从上往下读取，因此我们会先读到joe,然后读到joseph，于是执行回滚时我们只要把
读到的字符串写入到给定位置就可以，例如我们先读到joe，然后写入junk文件区块为33偏移为12的地方，
然后又读取joseph，再次将它写入到junk文件区块为33偏移为12的地方，于是就实现了回滚效果，
所以实现上SETSTRING记录不用写入7个字段，只有6个就可以
*/

type SetStringRecord struct {
	tx_num uint64
	offset uint64
	val    string
	blk    *fm.BlockId
}

func NewSetStringRecord(p *fm.Page) *SetStringRecord {
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
	val := p.GetString(vpos) //将日志中的字符串再次写入给定位置

	return &SetStringRecord{
		tx_num: tx_num,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (s *SetStringRecord) Op() RECORD_TYPE {
	return SETSTRING
}

func (s *SetStringRecord) TxNumber() uint64 {
	return s.tx_num
}

func (s *SetStringRecord) ToString() string {
	str := fmt.Sprintf("<SETSTRING %d %d %d %s>", s.tx_num, s.blk.Number(),
		s.offset, s.val)

	return str
}

func (s *SetStringRecord) Undo(tx TransationInterface) {
	tx.Pin(s.blk)
	tx.SetString(s.blk, s.offset, s.val, false) //将原来的字符串写回去
	tx.UnPin(s.blk)
}

func WriteSetStringLog(log_manager *lg.LogManager, tx_num uint64,
	blk *fm.BlockId, offset uint64, val string) (uint64, error) {
	/*
		构造字符串内容的日志,SetStringReord在构造中默认给定缓存页面已经有了字符串信息,
		但是在初始状态，缓存页面可能还没有相应日志信息，这个接口的作用就是为给定缓存写入
		字符串日志
	*/
	tpos := uint64(UINT64_LENGTH)
	fpos := uint64(tpos + UINT64_LENGTH)
	p := fm.NewPageBySize(1)
	bpos := uint64(fpos + p.MaxLengthForString(blk.FileName()))
	opos := uint64(bpos + UINT64_LENGTH)
	vpos := uint64(opos + UINT64_LENGTH)
	rec_len := uint64(vpos + p.MaxLengthForString(val))
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(tpos, tx_num)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	return log_manager.Append(rec)
}

```

需要注意的是上面代码实现的SETSTRING记录跟前面理论有所不同，传递给SetStringRecord的是一个缓存页面，它其实对应了SETSTRING的日志记录，WriteSetStringLog方法用于在给定日志中写入SETSTRING记录。同时需要注意的是，它的Undo方法需要通过实现了TransationInterface的对象来完成，由于我们现在还没有实现交易对象，因此我们需要实现一个伪对象来测试上面代码，创建tx_sub.go,添加代码如下：
```
package tx

import (
	fm "file_manager"
)

type TxStub struct {
	p *fm.Page
}

func NewTxStub(p *fm.Page) *TxStub {
	return &TxStub{
		p: p,
	}
}

func (t *TxStub) Commit() {

}

func (t *TxStub) RollBack() {

}

func (t *TxStub) Recover() {

}

func (t *TxStub) Pin(_ *fm.BlockId) {

}

func (t *TxStub) UnPin(_ *fm.BlockId) {

}
func (t *TxStub) GetInt(_ *fm.BlockId, offset uint64) uint64 {

	return t.p.GetInt(offset)
}
func (t *TxStub) GetString(_ *fm.BlockId, offset uint64) string {
	val := t.p.GetString(offset)
	return val
}

func (t *TxStub) SetInt(_ *fm.BlockId, offset uint64, val uint64, _ bool) {
    t.p.SetInt(offset, val)
}

func (t *TxStub) SetString(_ *fm.BlockId, offset uint64, val string, _ bool) {
	t.p.SetString(offset, val)
}

func (t *TxStub) AvailableBuffers() uint64 {
	return 0
}

func (t *TxStub) Size(_ string) uint64 {
	return 0
}

func (t *TxStub) Append(_ string) *fm.BlockId {
	return nil
}

func (t *TxStub) BlockSize() uint64 {
	return 0
}

```
下面我们写测试用例，以便检测代码的逻辑，在record_test.go中添加代码如下：
```
func TestSetStringRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "setstring")

	str := "original string"
	blk := uint64(1)
	dummy_blk := fm.NewBlockId("dummy_id", blk)
	tx_num := uint64(1)
	offset := uint64(13)
	//写入用于恢复的日志
	WriteSetStringLog(log_manager, tx_num, dummy_blk, offset, str)
	pp := fm.NewPageBySize(400)
	pp.SetString(offset, str)
	iter := log_manager.Iterator()
	rec := iter.Next()
	log_p := fm.NewPageByBytes(rec)
	setStrRec := NewSetStringRecord(log_p)
	expectd_str := fmt.Sprintf("<SETSTRING %d %d %d %s>", tx_num, blk, offset, str)

	require.Equal(t, expectd_str, setStrRec.ToString())

	pp.SetString(offset, "modify string 1")
	pp.SetString(offset, "modify string 2")
	txStub := NewTxStub(pp)
	setStrRec.Undo(txStub)
	recover_str := pp.GetString(offset)

	require.Equal(t, recover_str, str)
}
```
我们继续实现SETINT记录，它的实现就是把SETSTRING记录的实现代码拷贝一份然后简单修改一下，创建set_int_record.go，然后把set_string_record.go的代码拷贝进去然后做一些修改如下：
```
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

```

然后在record_test.go里面添加新的测试用例：
```
func TestSetIntRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "setstring")

	val := uint64(11)
	blk := uint64(1)
	dummy_blk := fm.NewBlockId("dummy_id", blk)
	tx_num := uint64(1)
	offset := uint64(13)
	//写入用于恢复的日志
	WriteSetIntLog(log_manager, tx_num, dummy_blk, offset, val)
	pp := fm.NewPageBySize(400)
	pp.SetInt(offset, val)
	iter := log_manager.Iterator()
	rec := iter.Next()
	log_p := fm.NewPageByBytes(rec)
	setIntRec := NewSetIntRecord(log_p)
	expectd_str := fmt.Sprintf("<SETINT %d %d %d %d>", tx_num, blk, offset, val)

	require.Equal(t, expectd_str, setIntRec.ToString())

	pp.SetInt(offset, 22)
	pp.SetInt(offset,33)
	txStub := NewTxStub(pp)
	setIntRec.Undo(txStub)
	recover_val := pp.GetInt(offset)

	require.Equal(t, recover_val, val)
}
```
最后还剩下ROLLBACK 和 COMMIT两个记录，它们内容简单，我们一并放出来，创建rollback_record.go，添加代码如下：
```
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
```
同理在record_test.go中添加测试用例如下：
```
func TestRollBackRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "rollback")
	tx_num := uint64(13) 
	WriteRollBackLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewRollBackRecord(pp)
	expected_str := fmt.Sprintf("<ROLLBACK %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}
```

接下来我们添加COMMIT记录，它的实现跟ROLLBACK差不多，添加commit_record.go然后添加代码如下：
```
package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)

type CommitRecord struct {
	tx_num uint64 
}

func NewCommitkRecordRecord(p *fm.Page) *CommitRecord {
	return &CommitRecord {
		tx_num : p.GetInt(UINT64_LENGTH),
	}
}

func (r *CommitRecord) Op() RECORD_TYPE {
	return COMMIT
}

func (r *CommitRecord) TxNumber() uint64 {
	return r.tx_num
}

func(r *CommitRecord) Undo() {
	//它没有回滚操作
}

func (r *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", r.tx_num)
}

func WriteCommitkRecordLog(lgmr *lg.LogManager, tx_num uint64) (uint64, error){
	rec := make([]byte, 2 * UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(COMMIT))
	p.SetInt(UINT64_LENGTH, tx_num)

	return lgmr.Append(rec)
}
```
然后在record_test.go添加代码如下：
```
func TestCommitRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "commit")
	tx_num := uint64(13) 
	WriteCommitkRecordLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewCommitkRecordRecord(pp)
	expected_str := fmt.Sprintf("<COMMIT %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}
```

最后我们完成最简单的CHECKPOINT记录，添加checkpoint_record.go，添加代码如下：
```
package tx

import (
	fm "file_manager"
	lg "log_manager"
	"math"
)

type CheckPointRecord struct{

}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{

	}
}

func (c *CheckPointRecord) Op() RECORD_TYPE {
	return CHECKPOINT
}

func (c *CheckPointRecord) TxNumber() uint64 {
	return math.MaxUint64 //它没有对应的交易号
}

func (c *CheckPointRecord) Undo() {

}

func (c *CheckPointRecord) ToString() string{
	return "<CHECKPOINT>"
}

func WriteCheckPointToLog(lgmr *lg.LogManager) (uint64, error) {
	rec := make([]byte, UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(CHECKPOINT))
	return lgmr.Append(rec)
}
```
最后在record_test.go中添加相应测试用例：
```
func TestCheckPointRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "checkpoint")
	WriteCheckPointToLog(log_manager)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)

	require.Equal(t, val, uint64(CHECKPOINT))

	check_point_rec := NewCheckPointRecord()
	expected_str := "<CHECKPOINT>"
	require.Equal(t, expected_str, check_point_rec.ToString())
}
```

经过调试，所有测试用例都能通过。要想更好的了解代码逻辑，请在B站搜索Coding迪斯尼，我会在视频中进行调试和演示。
