package core

import . "goRedis/data_structure"

// database DB core lib

type DataBase struct {
	// 数据
	data *Dict
	// 过期
	expire *Dict
}
