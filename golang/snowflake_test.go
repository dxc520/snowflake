package snowflake

import (
	"fmt"
	"sync"
	"testing"
)

func TestMainSF(t *testing.T) {
	instance, err := NewInstance(2, 1)
	if err != nil {
		fmt.Printf("new instance is error ")
	}

	size := 100
	wg := sync.WaitGroup{}
	wg.Add(size)

	for i := 0; i < size; i++ {
		go func(wg *sync.WaitGroup, idx int) {
			defer wg.Done()
			nextId, err2 := instance.NextId()
			if err2 != nil {
				fmt.Printf("get next id is error:=%s \n", err2.Error())
			} else {

				dateTimeStamp := ParseDateTime(nextId)
				datacenterIds := ParseDataCenter(nextId)
				machineId := ParseMachineId(nextId)
				sequence := ParseSequence(nextId)

				fmt.Printf("[%d]=%d ;it's timeStamp=%d,datacenterId=%d,machineId=%d,sequence=%d\n\n", idx+1, nextId, dateTimeStamp, datacenterIds, machineId, sequence)

			}
		}(&wg, i)
	}

	wg.Wait()
	fmt.Printf("main is over ")
}
