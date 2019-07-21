package vastflow

import (
	"errors"
	"fmt"
	"github.com/astaxie/beego/orm"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jack0liu/conf"
	"github.com/jack0liu/logs"
	"github.com/jack0liu/utils"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	initDbLock sync.Mutex
	initDbDone uint32
)

func InitVastFlowDb(configFile, dbPass string) error {
	if atomic.LoadUint32(&initDbDone) == 1 {
		return nil
	}
	// Slow-path.
	initDbLock.Lock()
	defer initDbLock.Unlock()
	if initDbDone == 0 {
		defer atomic.StoreUint32(&initDbDone, 1)
		return initOnce(configFile, dbPass)
	}
	return nil
}

func initOnce(configFile, dbPass string) error {
	basedir := utils.GetBasePath()
	logs.Debug(basedir)
	config := conf.LoadFile(filepath.Join(basedir, "conf", configFile))
	host := config.GetString("db_host")
	port := config.GetInt("db_port")
	if len(host) == 0 {
		return errors.New("host is empty")
	}

	// relative to conf directory
	pass := dbPass
	if len(pass) == 0 {
		pass = config.GetString("db_pass")
	}

	user := config.GetStringWithDefault("db_user", "tom")
	dbName := config.GetStringWithDefault("db_name", "rms")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&loc=Local", user, pass, host, port, dbName)
	// set default database
	maxIdleConnections := config.GetIntWithDefault("max_idle_connections", 30)
	maxOpenConnections := config.GetIntWithDefault("max_open_connections", 30)
	if err := orm.RegisterDataBase("default", "mysql", dsn, maxIdleConnections, maxOpenConnections); err != nil {
		logs.Error(err.Error())
		return err
	}

	// create table
	if err := orm.RunSyncdb("default", false, true); err != nil {
		logs.Error(err.Error())
		return err
	}
	return nil
}
