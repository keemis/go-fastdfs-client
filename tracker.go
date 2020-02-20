package fdfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type trackerStorageInfo struct {
	groupName      string
	ipAddr         string
	port           int64
	storePathIndex int8
}

type trackerTask struct {
	header
	groupName      string
	remoteFilename string
	trackerStorageInfo
}

func (t *trackerTask) SendReq(conn net.Conn) error {
	if t.groupName != "" {
		t.pkgLen = int64(GroupNameMaxLen + len(t.remoteFilename))
	}
	if err := t.SendHeader(conn); err != nil {
		return err
	}
	if t.groupName != "" {
		buffer := new(bytes.Buffer)
		byteGroupName := []byte(t.groupName)
		var bufferGroupName [16]byte
		for i := 0; i < len(byteGroupName); i++ {
			bufferGroupName[i] = byteGroupName[i]
		}
		buffer.Write(bufferGroupName[:])
		buffer.WriteString(t.remoteFilename)
		if _, err := conn.Write(buffer.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (t *trackerTask) RecvRes(conn net.Conn) error {
	if err := t.RecvHeader(conn); err != nil {
		return fmt.Errorf("TrackerTask RecvHeader %v", err)
	}
	if t.pkgLen != 39 && t.pkgLen != 40 {
		return fmt.Errorf("recvStorageInfo pkgLen %d invaild", t.pkgLen)
	}
	buf := make([]byte, t.pkgLen)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)
	var err error
	t.groupName, err = readCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return err
	}
	t.ipAddr, err = readCStrFromByteBuffer(buffer, 15)
	if err != nil {
		return err
	}
	if err := binary.Read(buffer, binary.BigEndian, &t.port); err != nil {
		return err
	}
	if t.pkgLen == 40 {
		storePathIndex, err := buffer.ReadByte()
		if err != nil {
			return err
		}
		t.storePathIndex = int8(storePathIndex)
	}
	return nil
}
