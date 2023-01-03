package function

import . "goRedis/core"

// database command and command process func core library

type handleProcess func(c *Client)

type DataBaseCommand struct {
	name string
	proc handleProcess
	id   int32
}

// CommandTable 全局CommandTable
var CommandTable map[string]*DataBaseCommand

func init() {
	// TODO 改成多态
	// TODO package command
	CommandTable = make(map[string]*DataBaseCommand, 0)
	CommandTable["GET"] = &DataBaseCommand{
		name: "get",
		proc: getCommandProcess,
		id:   0x01,
	}
	CommandTable["SET"] = &DataBaseCommand{
		name: "set",
		proc: setCommandProcess,
		id:   0x02,
	}
}

// 'get' Process Function
func getCommandProcess(c *Client) {

}

// 'set' Process Function
func setCommandProcess(c *Client) {

}
