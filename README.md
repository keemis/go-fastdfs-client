
# go fastdfs client

## 1. 使用说明

**代码**：
```
package main

import (
	"fmt"
	"sync"
	"testing"

	fdfs "github.com/keemis/go-fastdfs-client"
)

func main() {
	// 配置
	conf := &fdfs.Config{
		TrackerAddrs: []string{"127.0.0.1:22122"},
		MaxConn:      100,
	}
	client, err := fdfs.New(conf)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer client.Destory()

	// 上传
	fileID, err := client.UploadByFilename("1.jpeg")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("upload success: ", fileID)

	// 下载
	if err := client.DownloadToFile(fileID, "tempFile.jpg", 0, 0); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("download file success")

	// 下载 Buffer
	if _, err := client.DownloadToBuffer(fileID, 0, 19); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("download buffer success")
	}

	// 删除
	if err := client.DeleteFile(fileID); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("delete success")

	// 删除后再下载
	if err := client.DownloadToFile(fileID, "tempFile2.jpg", 0, 0); err != nil {
		fmt.Println("download after delete error: ", err.Error())
		return
	}
	fmt.Println("download after delete success: ", fileID)
}
```


**输出**：
```
go run main.go 

upload success:  group1/M00/00/00/fwAAAV5Ov4GANm8aAAQkA1uvMBA07.jpeg
download file success
download buffer success
delete success
download after delete error:  StorageDownloadTask RecvRes recv resp status 2 != 0
```
