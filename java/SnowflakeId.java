package com.hitech.id;

/**
 * 
 * 雪花算法（Snowflake）采用64bit
 * Snowflake生成的是Long类型的ID，一个Long类型占8个字节，每个字节占8比特，也就是说一个Long类型占64个比特。
 * 
 * 结构描述：Snowflake ID组成结构：正数位（占1比特）+ 时间戳（占41比特）+ 机器ID（占5比特）+ 数据中心（占5比特）+
 * 自增值（占12比特），总共64比特组成的一个Long类型。 >
 * 第一个bit位（1bit）：Java中long的最高位是符号位代表正负，正数是0，负数是1，一般生成ID都为正数，所以默认为0。 >
 * 时间戳部分（41bit）：毫秒级的时间，不建议存当前时间戳，而是用（当前时间戳 -
 * 固定开始时间戳）的差值，可以使产生的ID从更小的值开始；41位的时间戳可以使用69年，(1L << 41) / (1000L * 60 * 60 * 24
 * * 365) = 69年 > 工作机器id（10bit）：也被叫做workId，这个可以灵活配置，机房或者机器号组合都可以 >
 * 序列号部分（12bit），自增值支持同一毫秒内同一个节点可以生成4096个ID
 * 
 * url: https://www.jianshu.com/p/03d0fdea45f6
 * 
 * @author xcdong
 *
 */
public class SnowflakeId {

	/**
	 * 起始的时间戳
	 */
	private final static long START_TIMESTAMP = 1480166465631L;

	/**
	 * 每一部分占用的位数
	 */
	private final static long SEQUENCE_BIT = 12; // 序列号占用的位数
	private final static long MACHINE_BIT = 5; // 机器标识占用的位数
	private final static long DATA_CENTER_BIT = 5; // 数据中心占用的位数
	private final static long TIMESTAMP_BIT = 41; // 毫秒时间占用的位数
	private final static long SIGN_BIT = 1; // 符号位，即首位一般不用，代表数字为正数
	/**
	 * 每一部分的最大值
	 */
	private final static long MAX_SEQUENCE = -1L ^ (-1L << SEQUENCE_BIT);
	private final static long MAX_MACHINE_NUM = -1L ^ (-1L << MACHINE_BIT);
	private final static long MAX_DATA_CENTER_NUM = -1L ^ (-1L << DATA_CENTER_BIT);
	private final static long MAX_TIMESTAMP_NUM = -1L ^ (-1L << TIMESTAMP_BIT);

	/**
	 * 每一部分向左的位移
	 */
	private final static long MACHINE_LEFT = SEQUENCE_BIT;
	private final static long DATA_CENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
	private final static long TIMESTAMP_LEFT = DATA_CENTER_LEFT + DATA_CENTER_BIT;

	private long dataCenterId; // 数据中心
	private long machineId; // 机器标识
	private long sequence = 0L; // 序列号
	private long lastTimeStamp = -1L; // 上一次时间戳

	private long getNextMill() {
		long mill = getNewTimeStamp();
		while (mill <= lastTimeStamp) {
			mill = getNewTimeStamp();
		}
		return mill;
	}

	private long getNewTimeStamp() {
		return System.currentTimeMillis();
	}

	/**
	 * 根据指定的数据中心ID和机器标志ID生成指定的序列号
	 *
	 * @param dataCenterId 数据中心ID
	 * @param machineId    机器标志ID
	 */
	public SnowflakeId(long dataCenterId, long machineId) {
		if (dataCenterId > MAX_DATA_CENTER_NUM || dataCenterId < 0) {
			throw new IllegalArgumentException("DtaCenterId can't be greater than MAX_DATA_CENTER_NUM or less than 0！");
		}
		if (machineId > MAX_MACHINE_NUM || machineId < 0) {
			throw new IllegalArgumentException("MachineId can't be greater than MAX_MACHINE_NUM or less than 0！");
		}
		this.dataCenterId = dataCenterId;
		this.machineId = machineId;
	}

	/**
	 * 产生下一个ID
	 *
	 * @return
	 */
	public synchronized long nextId() {
		long currTimeStamp = getNewTimeStamp();
		if (currTimeStamp < lastTimeStamp) {
			throw new RuntimeException("Clock moved backwards.  Refusing to generate id");
		}

		if (currTimeStamp == lastTimeStamp) {
			// 相同毫秒内，序列号自增
			sequence = (sequence + 1) & MAX_SEQUENCE;
			// 同一毫秒的序列数已经达到最大
			if (sequence == 0L) {
				currTimeStamp = getNextMill();
			}
		} else {
			// 不同毫秒内，序列号置为0
			sequence = 0L;
		}

		lastTimeStamp = currTimeStamp;
		long nextId= (currTimeStamp - START_TIMESTAMP) << TIMESTAMP_LEFT // 时间戳部分
				| dataCenterId << DATA_CENTER_LEFT // 数据中心部分
				| machineId << MACHINE_LEFT // 机器标识部分
				| sequence; // 序列号部分
		
		
		System.out.printf("nextId=%d;it's timeStamp=%d,datacenterId=%d,machineId=%d,sequence=%d\n", nextId, (currTimeStamp - START_TIMESTAMP),dataCenterId,machineId,sequence);

		return nextId;
	}

	/**
	 * 按段解析，获取DataCenter的十进制数
	 * @param id
	 * @return
	 */
	public long parseDataCenter(long id) {
		long datacenterids = (id & (MAX_DATA_CENTER_NUM << DATA_CENTER_LEFT)) >> DATA_CENTER_LEFT;
		return datacenterids;
	}
	/**
	 * 按段解析，获取Machine的十进制数
	 * @param id
	 * @return
	 */
	public long parseMachineId(long id) {
		long machineId=(id & (MAX_MACHINE_NUM << MACHINE_LEFT))>>MACHINE_LEFT;
		return machineId;
	}
	/**
	 * 按段解析，获取DateTime的十进制数
	 * @param id
	 * @return
	 */
	public long parseDateTime(long id) {
		long machineId=(id & (MAX_TIMESTAMP_NUM << TIMESTAMP_LEFT))>>TIMESTAMP_LEFT;
		return machineId;
	}
	/**
	 * 按段解析，获取Sequence的十进制数
	 * @param id
	 * @return
	 */
	public long parseSequence(long id) {
		long machineId=id & MAX_SEQUENCE ;
		return machineId;
	}
	
	public static void main(String[] args) {
		
		System.out.println("默认datacenterId=2;machineId=3");
		
		SnowflakeId snowFlake = new SnowflakeId(2, 3);

		System.out.printf("MAX_SEQUENCE=%d \n", MAX_SEQUENCE);
		System.out.printf("MAX_MACHINE_NUM=%d \n", MAX_MACHINE_NUM);
		System.out.printf("MAX_DATA_CENTER_NUM=%d \n", MAX_DATA_CENTER_NUM);
		System.out.printf("MAX_TIMESTAMP_NUM=%d \n", MAX_TIMESTAMP_NUM);
		System.out.printf("currentTimeStamp=%d \n", snowFlake.getNewTimeStamp());

		System.out.printf(" 规定给定数字的使用位数=%d，那么数字的区间是多少？ \n", MACHINE_BIT);
		System.out.printf(" test1=[0,%d) \n", (1L << MACHINE_BIT));
		System.out.printf(" test2=[0,%d] \n", -1L ^ (-1L << MACHINE_BIT)); // 采用负数移位，并异或，主要是为了，减1.
		System.out.printf(" test3=[0,%d] \n", (1L << MACHINE_BIT) - 1);

		for (int i = 0; i < (1 << 4); i++) {
			// 10进制
			long nextId = snowFlake.nextId();
			long dateTimeStamp=snowFlake.parseDateTime(nextId);
			long datacenterIds = snowFlake.parseDataCenter(nextId);
			long machineId=snowFlake.parseMachineId(nextId);
			long sequence=snowFlake.parseSequence(nextId);
			System.out.printf("[%d]=%d ;it's timeStamp=%d,datacenterId=%d,machineId=%d,sequence=%d\n\n", i + 1, nextId, dateTimeStamp,datacenterIds,machineId,sequence);
		}
	}

}
