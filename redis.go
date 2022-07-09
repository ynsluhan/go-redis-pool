package go_redis_pool

import (
	"context"
	"github.com/go-redis/redis/v8"
	config "github.com/ynsluhan/go-config"
	"log"
	"strconv"
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
	var Host = config.GetEnv(conf.Redis.Host)
	var Port = conf.Redis.Port
	var Db, _ = strconv.Atoi(conf.Redis.Db)
	// 最大连接数
	var maxIdle, _ = strconv.Atoi(conf.Redis.MaxIdle)
	// 最大空闲数
	var maxActive, _ = strconv.Atoi(conf.Redis.MaxActive)
	// 设置空闲超时时间
	var idleTimeout = time.Minute * 5
	// 连接设置
	var connection = Host + ":" + Port
	//
	rdb = redis.NewClient(&redis.Options{
		Addr:         connection,
		Password:     "", // no password set
		DB:           Db, // use default DB
		PoolSize:     maxIdle,
		MinIdleConns: maxActive,
		DialTimeout:  idleTimeout,
	})
	//
	ping := rdb.Ping(context.Background())
	err := ping.Err()
	if err != nil {
		log.Printf("[redis] Redis connecting address：%s:%s/%d error \n", Host, Port, Db)
	}else {
		log.Printf("[redis] Redis connecting address：%s:%s/%d success \n", Host, Port, Db)
	}
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
		//建立连接
		con := &redis.FailoverOptions{
			// The master name.
			MasterName: node.Name,
			// A seed list of host:port addresses of sentinel nodes.
			SentinelAddrs: AddressList,
			// Following options are copied from Options struct.
			Password: node.Password,
			// db
			DB: node.Db,
			// 连接池个数
			PoolSize: node.PoolSize,
			// 最小空闲个数
			MinIdleConns: node.MinIdleConns,
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
