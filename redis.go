package go_redis_pool

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	config "github.com/ynsluhan/go-config"
	"log"
	"strings"
	"time"
)

// 普通redis连接
var rdb *redis.Client

// sentinel
// master
var rdbM *redis.Client

// slave
var rdbS *redis.Client

var conf *config.Config

/**
 * @Author yNsLuHan
 * @Description: 初始化redis连接池
 * @Time 2021-06-08 15:30:03
 */
func init() {
	// 根据env获取host
	conf = config.GetConf()
	// rdb 初始化
	if conf.Redis.Enable {
		SetRedisDb()
	}
	// sentinel 初始化
	if len(conf.Sentinel) != 0 {
		SetRedisSentinel()
	}
}

/**
 * @Author yNsLuHan
 * @Description:
 * @Time 2021-06-08 15:26:02
 */
func SetRedisDb() {
	var host = config.GetEnv(conf.Redis.Host)
	var port = conf.Redis.Port
	//var Db, _ = strconv.Atoi(conf.Redis.Db)
	var db = conf.Redis.Db
	// 最大连接数
	var idle = conf.Redis.MaxIdle
	if idle == 0 {
		idle = 5
	}
	var maxIdle = conf.Redis.MaxIdle
	if maxIdle == 0 {
		maxIdle = 5
	}
	// 最大空闲数
	var maxActive = conf.Redis.MaxActive
	if maxActive == 0 {
		maxActive = 2
	}
	// 设置空闲超时时间
	var idleTimeout = conf.Redis.Timeout
	var timeOut time.Duration
	if idleTimeout == 0 {
		timeOut = time.Second * 5
	} else {
		timeOut = time.Second * time.Duration(idleTimeout)
	}
	//
	rdb = redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", host, port),
		Password:     conf.Redis.Password, // no password set
		DB:           int(db),             // use default DB
		PoolSize:     int(maxIdle),
		MinIdleConns: int(maxActive),
		DialTimeout:  timeOut,
	})
	log.Printf("[redis] Redis connecting address：%s:%d/%d \n", host, port, db)
	// 连接测试
	go func() {
		ping := rdb.Ping(context.Background())
		err := ping.Err()
		if err != nil {
			log.Fatalf("[redis] Redis connect address：%s:%d/%s error \n", host, port, db)
		} else {
			log.Printf("[redis] Redis connect address：%s:%d/%d success \n", host, port, db)
		}
	}()
}

/**
 * @Author yNsLuHan
 * @Description: 创建 redis 哨兵模式
 * @Time 2021-06-08 15:25:44
 */
func SetRedisSentinel() {
	// 根据配置文件配置的哨兵节点个数注册
	for i, node := range conf.Sentinel {
		log.Print("Redis Sentinel connecting address: ", node.SentinelAddress)
		// 将地址进行切割
		AddressList := strings.Split(node.SentinelAddress, ",")
		//
		var db = node.Db
		// 最大连接数
		var poolSize = node.PoolSize
		if poolSize == 0 {
			poolSize = 5
		}
		var minIdleConns = node.MinIdleConns
		if minIdleConns == 0 {
			minIdleConns = 5
		}
		// 设置空闲超时时间
		var idleTimeout = conf.Redis.Timeout
		var timeOut time.Duration
		if idleTimeout == 0 {
			timeOut = time.Second * 5
		} else {
			timeOut = time.Second * time.Duration(idleTimeout)
		}
		//建立连接
		con := &redis.FailoverOptions{
			// The master name.
			MasterName: node.Name,
			// A seed list of host:port addresses of sentinel nodes.
			SentinelAddrs: AddressList,
			// Following options are copied from Options struct.
			Password: node.Password,
			// db
			DB: int(db),
			// 连接池个数
			PoolSize: int(poolSize),
			// 最小空闲个数
			MinIdleConns: int(minIdleConns),
			//
			DialTimeout: timeOut,
		}
		// 获取连接
		// 设置连接
		if i == 0 {
			log.Println("[redis sentinel] master init success.")
			rdbM = redis.NewFailoverClient(con)
		} else {
			log.Println("[redis sentinel] slave init success.")
			rdbS = redis.NewFailoverClient(con)
		}
	}
}

/**
 * @Author yNsLuHan
 * @Description: 获取sentinel master连接
 * @Time 2021-06-08 15:25:26
 * @return *redis.Client
 */
func GetSentinelMaster() *redis.Client {
	return rdbM
}

/**
 * @Author yNsLuHan
 * @Description: 获取sentinel slave连接
 * @Time 2021-06-08 15:25:31
 * @return *redis.Client
 */
func GetSentinelBack() *redis.Client {
	return rdbS
}

/**
 * @Author yNsLuHan
 * @Description: 获取redis
 * @Time 2021-06-08 15:25:37
 * @return *redis.Client
 */
func GetRdb() *redis.Client {
	return rdb
}
