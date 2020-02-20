package fdfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	TrackerProtoCmdResp                             = 100
	TrackerProtoCmdServiceQueryStoreWithoutGroupOne = 101
	TrackerProtoCmdServiceQueryFetchOne             = 102
	StorageProtoCmdUploadFile                       = 11
	StorageProtoCmdDeleteFile                       = 12
	StorageProtoCmdDownloadFile                     = 14
	ProtoCmdActiveTest                              = 111

	GroupNameMaxLen = 16
)

type storageInfo struct {
	addr             string
	storagePathIndex int8
}

type fileInfo struct {
	fileSize    int64
	buffer      []byte
	file        *os.File
	fileExtName string
}

func newFileInfo(fileName string, buffer []byte, fileExtName string) (*fileInfo, error) {
	if fileName != "" {
		file, err := os.Open(fileName)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if int(stat.Size()) == 0 {
			_ = file.Close()
			return nil, fmt.Errorf("file %q size is zero", fileName)
		}
		var fileExtName string
		index := strings.LastIndexByte(fileName, '.')
		if index != -1 {
			fileExtName = fileName[index+1:]
			if len(fileExtName) > 6 {
				fileExtName = fileExtName[:6]
			}
		}
		return &fileInfo{
			fileSize:    stat.Size(),
			file:        file,
			fileExtName: fileExtName,
		}, nil
	}
	if len(fileExtName) > 6 {
		fileExtName = fileExtName[:6]
	}
	return &fileInfo{
		fileSize:    int64(len(buffer)),
		buffer:      buffer,
		fileExtName: fileExtName,
	}, nil
}

func (f *fileInfo) Close() {
	if f == nil {
		return
	}
	if f.file != nil {
		_ = f.file.Close()
	}
	return
}

type task interface {
	SendReq(net.Conn) error
	RecvRes(net.Conn) error
}

type header struct {
	pkgLen int64
	cmd    int8
	status int8
}

func (h *header) SendHeader(conn net.Conn) error {
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, h.pkgLen); err != nil {
		return err
	}
	buffer.WriteByte(byte(h.cmd))
	buffer.WriteByte(byte(h.status))
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (h *header) RecvHeader(conn net.Conn) error {
	buf := make([]byte, 10)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)

	if err := binary.Read(buffer, binary.BigEndian, &h.pkgLen); err != nil {
		return err
	}
	cmd, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	status, err := buffer.ReadByte()
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("recv resp status %d != 0", status)
	}
	h.cmd = int8(cmd)
	h.status = int8(status)
	return nil
}

func splitFileID(fileID string) (string, string, error) {
	str := strings.SplitN(fileID, "/", 2)
	if len(str) < 2 {
		return "", "", fmt.Errorf("invalid fildId")
	}
	return str[0], str[1], nil
}
