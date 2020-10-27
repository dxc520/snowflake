/**
 * 算法介绍：
 * SnowFlake 算法，是 Twitter 开源的分布式 id 生成算法。其核心思想就是：使用一个 64 bit 的 long 型的数字作为全局唯一 id。在分布式系统中的应用十分广泛，且ID 引入了时间戳，基本上保持自增的.
 * 各部分介绍：
 * 第一个部分，是 1 个 bit：0，这个是无意义的。
 * 第二个部分是 41 个 bit：表示的是时间戳。
 * 第三个部分是 4 个 bit：表示的是机房 id，1001。
 * 第四个部分是 6 个 bit：表示的是机器 id，101001。
 * 第五个部分是 12 个 bit：表示的序号，就是某个机房某台机器上这一毫秒内同时生成的 id 的序号
 *
 * 设计时需要考虑的2点因素：
 *  1、 单机的并发量 即 单位毫秒内并发数:业即第五部分最大的容量 2^12=4096/ms，如果不满足，就的考虑 缩减，二、三、四部分，扩大 第五部分
 *  2、规格：服务实例的总体规模：也即 第二+第三部分的总和。即 2^10=1024.如果不满足，需要考虑这部分扩容，其余部分缩容
 */

package snowflake

import (
	"errors"
	"sync"
	"time"
)

/**
 * 起始的时间戳 毫秒级
 */
const start_timestamp int64 = 1480166465631

/**
 * 每一部分占用的位数
 */
const (
	sequence_bit    uint8 = 12 // 序列号占用的位数 毫秒并发数 2^12=4096/ms;
	machine_bit     uint8 = 6  // 机器标识占用的位数
	data_center_bit uint8 = 4  // 数据中心占用的位数
	timestamp_bit   uint8 = 41 // 毫秒时间占用的位数；
	sign_bit        uint8 = 1  // 符号位，即首位一般不用，代表数字为正数
)

/**
 * 每一部分的最大值
 */

const (
	negativeOne         int64 = -1
	max_sequence        int64 = negativeOne ^ (negativeOne << sequence_bit)
	max_machine_num     int64 = negativeOne ^ (negativeOne << machine_bit)
	max_data_center_num int64 = negativeOne ^ (negativeOne << data_center_bit)
	max_timestamp_num   int64 = negativeOne ^ (negativeOne << timestamp_bit)
)

/**
 * 每一部分向左的位移
 */
const (
	machine_left     = sequence_bit
	data_center_left = sequence_bit + machine_bit
	timestamp_left   = data_center_left + data_center_bit
)

type snowFlakeId struct {
	DataCenterId  int64 // 数据中心
	MachineId     int64 // 机器标识
	Sequence      int64 //= 0 // 序列号
	LastTimeStamp int64 //= -1 // 上一次时间戳
	lock          sync.Mutex
}

func (p *snowFlakeId) getNextMill() int64 {
	mill := p.getNewTimeStamp()
	for {
		if mill <= p.LastTimeStamp {
			mill = p.getNewTimeStamp()
			break
		}
	}
	return mill
}

func (p *snowFlakeId) getNewTimeStamp() int64 {
	//return System.currentTimeMillis();
	//return time.Now().Unix() //秒
	//fmt.Printf("时间戳（纳秒转换为秒）：%v;\n",time.Now().UnixNano() / 1e9)
	return time.Now().UnixNano() / 1e6 //毫秒
}

/**
 * 根据指定的数据中心ID和机器标志ID生成指定的序列号
 *
 * @param dataCenterId 数据中心ID(2^data_center_bit)=8
 * @param machineId    机器标志ID(2^machine_bit)=32
 */
func NewInstance(dataCenterId int64, machineId int64) (snowFlakeId, error) {
	sf := snowFlakeId{Sequence: 0, LastTimeStamp: negativeOne}
	if dataCenterId > max_data_center_num || dataCenterId < 0 {
		return sf, errors.New("DtaCenterId can't be greater than MAX_DATA_CENTER_NUM or less than 0！");
	}
	if machineId > max_machine_num || machineId < 0 {
		return sf, errors.New("MachineId can't be greater than MAX_MACHINE_NUM or less than 0！");
	}
	sf.DataCenterId = dataCenterId
	sf.MachineId = machineId
	return sf, nil
}

/**
 * 产生下一个ID
 *
 * @return
 */
func (p *snowFlakeId) NextId() (int64, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	currTimeStamp := p.getNewTimeStamp()

	if currTimeStamp < p.LastTimeStamp {
		return 0, errors.New("Clock moved backwards.  Refusing to generate id");
	}

	if currTimeStamp == p.LastTimeStamp {
		// 相同毫秒内，序列号自增
		p.Sequence = (p.Sequence + 1) & max_sequence
		// 同一毫秒的序列数已经达到最大
		if p.Sequence == 0 {
			currTimeStamp = p.getNextMill()
		}
	} else {
		// 不同毫秒内，序列号置为0
		p.Sequence = 0
	}

	p.LastTimeStamp = currTimeStamp

	// 时间戳部分 |  数据中心部分 |  机器标识部分 |序列号部分
	var nextId int64 = (currTimeStamp-start_timestamp)<<timestamp_left | p.DataCenterId<<data_center_left | p.MachineId<<machine_left | p.Sequence

	return nextId, nil
}

/// 以下为：已知snowflakeId，反解析为各个字段

/**
 * 按段解析，获取DataCenter的十进制数
 * @param id
 * @return
 */
func ParseDataCenter(id int64) int64 {
	datacenterids := (id & (max_data_center_num << data_center_left)) >> data_center_left
	return datacenterids
}

/**
 * 按段解析，获取Machine的十进制数
 * @param id
 * @return
 */
func ParseMachineId(id int64) int64 {
	machineId := (id & (max_machine_num << machine_left)) >> machine_left
	return machineId
}

/**
 * 按段解析，获取DateTime的十进制数
 * @param id
 * @return
 */
func ParseDateTime(id int64) int64 {
	machineId := (id & (max_timestamp_num << timestamp_left)) >> timestamp_left
	return machineId
}

/**
 * 按段解析，获取Sequence的十进制数
 * @param id
 * @return
 */
func ParseSequence(id int64) int64 {
	machineId := id & max_sequence
	return machineId
}
