package ha

import "time"

//type haStatus  struct{
//	initial  int   //0
//	standby  int   //1
//	active   int   //2
//}


type haConfig struct{
	ip   string
	port string
	timeout  time.Duration
}
func NewHaConfig(ip string,port  string,timeout time.Duration) (*haConfig, error) {
	return &haConfig{
		ip:ip,
		port:port,
		timeout:timeout,
	}, nil
}



























