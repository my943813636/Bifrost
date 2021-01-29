package src

import (
	"encoding/json"
	"fmt"
	"github.com/brokercap/Bifrost/plugin/driver"
	pluginDriver "github.com/brokercap/Bifrost/plugin/driver"
	"github.com/go-redis/redis"
	"strconv"
	"strings"
	"time"
)

const VERSION = "v1.7.0"
const BIFROST_VERION = "v1.7.0"

func init() {
	driver.Register("redis", NewConn, VERSION, BIFROST_VERION)
}

type PluginParam struct {
	KeyConfig      string
	FieldKeyConfig string
	SortedConfig   string
	Type           string

	DataType    string
	ValueConfig string

	Expir int
}

type Conn struct {
	Uri      string
	pwd      string
	database int
	network  string
	status   string
	conn     redis.UniversalClient
	p        *PluginParam
}

func NewConn() pluginDriver.Driver {
	return &Conn{status: "close"}
}

func getUriParam(uri string) (pwd string, network string, url string, database int) {
	i := strings.IndexAny(uri, "@")
	pwd = ""
	if i > 0 {
		pwd = uri[0:i]
		url = uri[i+1:]
	} else {
		url = uri
	}
	i = strings.IndexAny(url, "/")
	if i > 0 {
		databaseString := url[i+1:]
		intv, err := strconv.Atoi(databaseString)
		if err != nil {
			database = -1
		}
		database = intv
		url = url[0:i]
	} else {
		database = 0
	}
	i = strings.IndexAny(url, "(")
	if i > 0 {
		network = url[0:i]
		url = url[i+1 : len(url)-1]
	} else {
		network = "tcp"
	}
	return
}

func (This *Conn) SetOption(uri *string, param map[string]interface{}) {
	This.Uri = *uri
}

func (This *Conn) Open() error {
	if This.conn != nil {
		This.conn.Close()
	}
	pwd, network, uri, database := getUriParam(This.Uri)
	if database < 0 {
		return fmt.Errorf("database must be in 0 and 16")
	}
	if network != "tcp" {
		return fmt.Errorf("network must be tcp")
	}

	This.conn = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    strings.SplitN(uri, ",", -1),
		Password: pwd,
		DB:       database,
		PoolSize: 4096,
	})

	_, err := This.conn.Ping().Result()
	if err != nil {
		This.status = "close"
		return err
	}

	return nil
}

func (This *Conn) Close() bool {
	err := This.conn.Close()
	if err == nil {
		This.status = "close"
	}
	return err == nil
}

func (This *Conn) GetUriExample() string {
	return "pwd@tcp(127.0.0.1:6379)/0 or 127.0.0.1:6379 or pwd@tcp(127.0.0.1:6379,127.0.0.1:6380)/0 or 127.0.0.1:6379,127.0.0.1:6380"
}

func (This *Conn) CheckUri() error {
	defer func() {
		if This.conn != nil {
			This.conn.Close()
		}
	}()
	return This.Open()
}

func (This *Conn) TimeOutCommit() (*driver.PluginDataType, *driver.PluginDataType, error) {
	return nil, nil, nil
}

func (This *Conn) Skip(dataType *driver.PluginDataType) error {
	return nil
}

func (This *Conn) SetParam(p interface{}) (interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("param is nil")
	}
	switch p.(type) {
	case *PluginParam:
		This.p = p.(*PluginParam)
		return p, nil
	default:
		s, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		var param PluginParam
		err = json.Unmarshal(s, &param)
		if err != nil {
			return nil, err
		}
		This.p = &param
		return &param, nil
	}
}

func (This *Conn) Insert(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return This.Update(data, retry)
}

func (This *Conn) Update(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if retry {
		return nil, nil, fmt.Errorf("not support")
	}

	var err error

	index := len(data.Rows) - 1
	key := This.getKeyVal(data, This.p.KeyConfig, index)

	j, err := This.getVal(data, index)
	if err != nil {
		return nil, nil, err
	}

	switch This.p.Type {
	case "string":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				oldKey := This.getKeyVal(data, This.p.KeyConfig, 0)
				pipeline.Del(oldKey)
			}
			pipeline.Set(key, j, time.Duration(This.p.Expir)*time.Second)
			_, err = pipeline.Exec()
		}
	case "hash":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				oldKey := This.getKeyVal(data, This.p.KeyConfig, 0)
				oldFiledKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
				pipeline.HDel(oldKey, oldFiledKey)
			}
			fieldKey := This.getKeyVal(data, This.p.FieldKeyConfig, index)
			pipeline.HSet(key, fieldKey, j)
			_, err = pipeline.Exec()
		}
	case "zset":
		{
			sort, err := strconv.ParseFloat(This.getKeyVal(data, This.p.SortedConfig, index), 64)
			if err != nil {
				sort = 0
			}

			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				oldKey := This.getKeyVal(data, This.p.KeyConfig, 0)
				if jo, err := This.getVal(data, 0); err == nil {
					pipeline.ZRem(oldKey, 1, jo)
				}
			}
			pipeline.ZAdd(key, redis.Z{Score: sort, Member: j})
			_, err = pipeline.Exec()
		}
	case "list":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				if jo, err := This.getVal(data, 0); err == nil {
					oldKey := This.getKeyVal(data, This.p.KeyConfig, 0)
					pipeline.LRem(oldKey, 1, jo)
				}
			}
			pipeline.LPush(key, j)
			_, err = pipeline.Exec()
		}
	case "set":
		{
			pipeline := This.conn.Pipeline()
			if len(data.Rows) >= 2 {
				if jo, err := This.getVal(data, 0); err == nil {
					oldKey := This.getKeyVal(data, This.p.KeyConfig, 0)
					pipeline.SRem(oldKey, 1, jo)
				}
			}
			pipeline.SAdd(key, j)
			_, err = pipeline.Exec()
		}
	default:
		err = fmt.Errorf(This.p.Type + " not in(key=>value,set,zset,hash,list)")
	}

	if err != nil {
		return nil, nil, err
	}
	return nil, nil, nil
}

func (This *Conn) Del(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	if retry {
		return nil, nil, fmt.Errorf("not support")
	}

	key := This.getKeyVal(data, This.p.KeyConfig, 0)
	var err error
	switch This.p.Type {
	case "string":
		err = This.conn.Del(key).Err()
	case "hash":
		fieldKey := This.getKeyVal(data, This.p.FieldKeyConfig, 0)
		err = This.conn.HDel(key, fieldKey).Err()
	case "zset":
		j, e := This.getVal(data, 0)
		if e != nil {
			return nil, nil, e
		}
		err = This.conn.ZRem(key, j).Err()
	case "list":
		j, e := This.getVal(data, 0)
		if e != nil {
			return nil, nil, e
		}
		err = This.conn.LRem(key, 1, j).Err()
	case "set":
		j, e := This.getVal(data, 0)
		if e != nil {
			return nil, nil, e
		}
		err = This.conn.SRem(key, j).Err()
	default:
		err = fmt.Errorf(This.p.Type + " not in(string,set,zset,hash,list)")
	}
	if err != nil {
		return nil, nil, err
	}
	return nil, nil, nil
}

func (This *Conn) Query(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (This *Conn) Commit(data *pluginDriver.PluginDataType, retry bool) (*pluginDriver.PluginDataType, *pluginDriver.PluginDataType, error) {
	return nil, nil, nil
}

func (This *Conn) getKeyVal(data *driver.PluginDataType, key string, index int) string {
	return fmt.Sprint(driver.TransfeResult(key, data, index))
}

func (This *Conn) getVal(data *driver.PluginDataType, index int) (string, error) {
	if This.p.DataType == "string" {
		return fmt.Sprint(driver.TransfeResult(This.p.ValueConfig, data, index)), nil
	} else {
		j, e := json.Marshal(data.Rows[index])
		if e != nil {
			return "", e
		}
		return fmt.Sprint(driver.TransfeResult(string(j), data, index)), nil
	}
}
