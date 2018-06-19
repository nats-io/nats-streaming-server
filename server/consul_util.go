package server

import (
	"errors"
	"fmt"
	"github.com/hashicorp/consul/api"
	//log "github.com/xiaomi-tc/log15"
	"github.com/nats-io/nats-streaming-server/logger"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type ConsulUtility struct {
	//定义空对象，方便调用端作为参数
	hostIP   string
	port int
	status_monitor_port int
	serviceID   string

	consul_addr   string
	consul_client *api.Client
	http_svr *http.Server

	isInit bool
	log *logger.StanLogger
}

//获取本机局域网IP地址
func getHostIP(log *logger.StanLogger) string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		os.Exit(1)
	}
	for _, a := range addrs {
		//判断是否正确获取到IP
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && !ipnet.IP.IsLinkLocalMulticast() && !ipnet.IP.IsLinkLocalUnicast() {
			log.Noticef("getHostIP ip:%v", ipnet)
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	log.Errorf("getHostIP error: %v", err)
	os.Exit(1)
	return ""
}

//状态检查服务启动
func startService(addr string, log *logger.StanLogger) *http.Server {
	srv := &http.Server{Addr: addr}
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request){
		fmt.Fprint(w, "status ok!")
	})
	log.Noticef("start listen...   addr %v", addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Noticef("startService error: %v", err)
			//os.Exit(1)
		}
	}()

	return srv
}

//对外接口：初始化
func (u *ConsulUtility) Register(service_name string, cluster_id string,listenPort int, consulAddr string, log *logger.StanLogger) (service_id string, err error) {
	defer func() {
		u.isInit = true
	}()

	if u.isInit {
		return "", errors.New("consul_util already init")
	}

	u.log = log

	if consulAddr != "" {
		u.consul_addr = consulAddr
	} else {
		u.consul_addr = "127.0.0.1:8500" // default consul address: 127.0.0.1:8500
	}

	config := api.DefaultConfig()
	config.Address = u.consul_addr

	client, err := api.NewClient(config)
	if err != nil {
		log.Errorf("Consul NewClient(), error:%v", err)

		return "", errors.New("consul NewClient() failed")
	}
	u.consul_client = client

	u.port = listenPort
	u.hostIP = getHostIP(u.log)

	my_service_id := service_name + "-" + u.hostIP + "-" + strconv.Itoa(u.port)
	u.serviceID = my_service_id
	//正常初始化时开监听
	u.status_monitor_port = u.port + 1

	var tags = []string{cluster_id}
	service := &api.AgentServiceRegistration{
		ID:      my_service_id,
		Name:    service_name,
		Port:    u.port,
		Address: u.hostIP,
		Tags:    tags,
		Check: &api.AgentServiceCheck{
			HTTP:     "http://" + u.hostIP + ":" + strconv.Itoa(u.status_monitor_port) + "/status",
			Interval: "5s",
			Timeout:  "1s",
		},
	}

	if err := client.Agent().ServiceRegister(service); err != nil {
		log.Errorf("RegistService error:%v", err)
		return "" , errors.New("consul ServiceRegister() failed")
	}

	log.Noticef("Registered service:%v   tags:%v", service_name, strings.Join(tags, ","))

	u.http_svr = startService(":" + strconv.Itoa(u.status_monitor_port), u.log)

	return my_service_id, nil
}

//服务注销
func (u *ConsulUtility) UnRegister() {
	if err := u.http_svr.Shutdown(nil); err != nil {
		u.log.Errorf("http_svr Shutdown error:%v", err)
	}

	if u.consul_client == nil {
		u.log.Errorf("UnRegistService error: consul_client nil!")
		return
	}

	if err := u.consul_client.Agent().ServiceDeregister(u.serviceID); err != nil {
		u.log.Errorf("UnRegistService error: %v", err)
	}
	u.log.Debugf("UnRegistService service_id: %v", u.serviceID)
}
