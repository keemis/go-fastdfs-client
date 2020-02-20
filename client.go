package fdfs

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// Config po
type Config struct {
	TrackerAddrs []string // [127.0.0.1:22122]
	MaxConn      int      // 100
}

// Client po
type Client struct {
	trackerPools    map[string]*connPool
	storagePools    map[string]*connPool
	storagePoolLock *sync.RWMutex
	config          *Config
}

// New client
func New(config *Config) (*Client, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	client := &Client{
		config:          config,
		storagePoolLock: &sync.RWMutex{},
	}
	client.trackerPools = make(map[string]*connPool)
	client.storagePools = make(map[string]*connPool)
	for _, addr := range config.TrackerAddrs {
		trackerPool, err := newConnPool(addr, config.MaxConn)
		if err != nil {
			return nil, err
		}
		client.trackerPools[addr] = trackerPool
	}
	return client, nil
}

// Destory client
func (c *Client) Destory() {
	if c == nil {
		return
	}
	for _, pool := range c.trackerPools {
		pool.Destory()
	}
	for _, pool := range c.storagePools {
		pool.Destory()
	}
}

// UploadByFilename filename
func (c *Client) UploadByFilename(fileName string) (string, error) {
	fileInfo, err := newFileInfo(fileName, nil, "")
	if err != nil {
		return "", err
	}
	defer fileInfo.Close()
	storageInfo, err := c.queryStorageInfoWithTracker(TrackerProtoCmdServiceQueryStoreWithoutGroupOne, "", "")
	if err != nil {
		return "", err
	}
	task := &storageUploadTask{}
	task.fileInfo = fileInfo
	task.storagePathIndex = storageInfo.storagePathIndex
	if err := c.doStorage(task, storageInfo); err != nil {
		return "", err
	}
	return task.fileID, nil
}

// UploadByBuffer buffer
func (c *Client) UploadByBuffer(buffer []byte, fileExtName string) (string, error) {
	fileInfo, err := newFileInfo("", buffer, fileExtName)
	if err != nil {
		return "", err
	}
	defer fileInfo.Close()
	storageInfo, err := c.queryStorageInfoWithTracker(TrackerProtoCmdServiceQueryStoreWithoutGroupOne, "", "")
	if err != nil {
		return "", err
	}
	task := &storageUploadTask{}
	task.fileInfo = fileInfo
	task.storagePathIndex = storageInfo.storagePathIndex
	if err := c.doStorage(task, storageInfo); err != nil {
		return "", err
	}
	return task.fileID, nil
}

// DownloadToFile download
func (c *Client) DownloadToFile(fileID string, localFilename string, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileID(fileID)
	if err != nil {
		return err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TrackerProtoCmdServiceQueryFetchOne, groupName, remoteFilename)
	if err != nil {
		return err
	}
	task := &storageDownloadTask{}
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes
	task.localFilename = localFilename
	return c.doStorage(task, storageInfo)
}

// DownloadToBuffer download
func (c *Client) DownloadToBuffer(fileID string, offset int64, downloadBytes int64) ([]byte, error) {
	groupName, remoteFilename, err := splitFileID(fileID)
	if err != nil {
		return nil, err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TrackerProtoCmdServiceQueryFetchOne, groupName, remoteFilename)
	if err != nil {
		return nil, err
	}
	task := &storageDownloadTask{}
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes
	if err := c.doStorage(task, storageInfo); err != nil {
		return nil, err
	}
	return task.buffer, nil
}

// DownloadToAllocatedBuffer download
func (c *Client) DownloadToAllocatedBuffer(fileID string, buffer []byte, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileID(fileID)
	if err != nil {
		return err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TrackerProtoCmdServiceQueryFetchOne, groupName, remoteFilename)
	if err != nil {
		return err
	}
	task := &storageDownloadTask{}
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes
	task.buffer = buffer
	if err := c.doStorage(task, storageInfo); err != nil {
		return err
	}
	return nil
}

// DeleteFile delete
func (c *Client) DeleteFile(fileID string) error {
	groupName, remoteFilename, err := splitFileID(fileID)
	if err != nil {
		return err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TrackerProtoCmdServiceQueryFetchOne, groupName, remoteFilename)
	if err != nil {
		return err
	}
	task := &storageDeleteTask{}
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	return c.doStorage(task, storageInfo)
}

func (c *Client) doTracker(task task) error {
	trackerConn, err := c.getTrackerConn()
	if err != nil {
		return err
	}
	defer func() {
		_ = trackerConn.Close()
	}()
	if err := task.SendReq(trackerConn); err != nil {
		return err
	}
	if err := task.RecvRes(trackerConn); err != nil {
		return err
	}
	return nil
}

func (c *Client) doStorage(task task, storageInfo *storageInfo) error {
	storageConn, err := c.getStorageConn(storageInfo)
	if err != nil {
		return err
	}
	defer func() {
		_ = storageConn.Close()
	}()
	if err := task.SendReq(storageConn); err != nil {
		return err
	}
	if err := task.RecvRes(storageConn); err != nil {
		return err
	}
	return nil
}

func (c *Client) queryStorageInfoWithTracker(cmd int8, groupName string, remoteFilename string) (*storageInfo, error) {
	task := &trackerTask{}
	task.cmd = cmd
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	if err := c.doTracker(task); err != nil {
		return nil, err
	}
	return &storageInfo{
		addr:             fmt.Sprintf("%s:%d", task.ipAddr, task.port),
		storagePathIndex: task.storePathIndex,
	}, nil
}

func (c *Client) getTrackerConn() (net.Conn, error) {
	var trackerConn net.Conn
	var err error
	var getOne bool
	for _, trackerPool := range c.trackerPools {
		trackerConn, err = trackerPool.get()
		if err == nil {
			getOne = true
			break
		}
	}
	if getOne {
		return trackerConn, nil
	}
	if err == nil {
		return nil, fmt.Errorf("no connPool can be use")
	}
	return nil, err
}

func (c *Client) getStorageConn(storageInfo *storageInfo) (net.Conn, error) {
	c.storagePoolLock.Lock()
	storagePool, ok := c.storagePools[storageInfo.addr]
	if ok {
		c.storagePoolLock.Unlock()
		return storagePool.get()
	}
	storagePool, err := newConnPool(storageInfo.addr, c.config.MaxConn)
	if err != nil {
		c.storagePoolLock.Unlock()
		return nil, err
	}
	c.storagePools[storageInfo.addr] = storagePool
	c.storagePoolLock.Unlock()
	return storagePool.get()
}
