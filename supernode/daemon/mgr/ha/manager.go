package ha

import (
	"fmt"
	"context"
)
type Manager struct{
	useHa       bool
	nodeStatus  int   //0 initial  1 standby  2  active
	conf        haConfig
	tool        *toolMgr
}
func NewManager(isHa bool,status int,conf haConfig) (*Manager, error) {
	toolMgr, err :=NewToolMgr(conf)
	if err != nil {
		return nil, err
	}

	return &Manager{
		useHa: isHa,
		nodeStatus:status,
		conf:conf,
		tool:toolMgr,
	}, nil
}
//HaElect try to change the status from standby to active
func (ha *Manager)HaElect()error{
	if(ha.nodeStatus!=1){
		//there should be a error,i am working with it
		fmt.Println("initial or  active,can not take part in election")
		return   nil
	}
	//add a validity judgment  of etcd ....
    //.........

	//try to change the status from standby to active
	//TryBeActive() try to abtain a  lock on etcd and get the response of etcd
	isactive,_:=ha.tool.TryBeActive(context.TODO())
	if isactive==true{
		ha.nodeStatus=2
	}
	return  nil
}
func (ha *Manager)HaWatchActive(){
	if(ha.nodeStatus!=1){
		//there should be a error,i am working with it
		fmt.Println("initial or  active,can not take part in election")
	}
	ha.tool.WathActiveChange()
}













