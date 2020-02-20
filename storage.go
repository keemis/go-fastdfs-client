package fdfs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type storageUploadTask struct {
	header
	fileInfo         *fileInfo
	storagePathIndex int8
	fileID           string
}

func (s *storageUploadTask) SendReq(conn net.Conn) error {
	s.cmd = StorageProtoCmdUploadFile
	s.pkgLen = s.fileInfo.fileSize + 15
	if err := s.SendHeader(conn); err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	buffer.WriteByte(byte(s.storagePathIndex))
	if err := binary.Write(buffer, binary.BigEndian, s.fileInfo.fileSize); err != nil {
		return err
	}
	byteFileExtName := []byte(s.fileInfo.fileExtName)
	var bufferFileExtName [6]byte
	for i := 0; i < len(byteFileExtName); i++ {
		bufferFileExtName[i] = byteFileExtName[i]
	}
	buffer.Write(bufferFileExtName[:])
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	var err error
	if s.fileInfo.file != nil {
		_, err = conn.(pConn).Conn.(*net.TCPConn).ReadFrom(s.fileInfo.file)
	} else {
		_, err = conn.Write(s.fileInfo.buffer)
	}

	if err != nil {
		return err
	}
	return nil
}

func (s *storageUploadTask) RecvRes(conn net.Conn) error {
	if err := s.RecvHeader(conn); err != nil {
		return err
	}

	if s.pkgLen <= 16 {
		return fmt.Errorf("recv file id pkgLen <= GROUP_NAME_MAX_LEN")
	}
	if s.pkgLen > 100 {
		return fmt.Errorf("recv file id pkgLen > 100,can't be so long")
	}

	buf := make([]byte, s.pkgLen)
	if _, err := conn.Read(buf); err != nil {
		return err
	}

	buffer := bytes.NewBuffer(buf)
	groupName, err := readCStrFromByteBuffer(buffer, 16)
	if err != nil {
		return err
	}
	remoteFileName, err := readCStrFromByteBuffer(buffer, int(s.pkgLen)-16)
	if err != nil {
		return err
	}

	s.fileID = groupName + "/" + remoteFileName
	return nil
}

type storageDownloadTask struct {
	header
	groupName      string
	remoteFilename string
	offset         int64
	downloadBytes  int64
	localFilename  string
	buffer         []byte
}

func (s *storageDownloadTask) SendReq(conn net.Conn) error {
	s.cmd = StorageProtoCmdDownloadFile
	s.pkgLen = int64(len(s.remoteFilename) + 32)

	if err := s.SendHeader(conn); err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	if err := binary.Write(buffer, binary.BigEndian, s.offset); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.BigEndian, s.downloadBytes); err != nil {
		return err
	}
	byteGroupName := []byte(s.groupName)
	var bufferGroupName [16]byte
	for i := 0; i < len(byteGroupName); i++ {
		bufferGroupName[i] = byteGroupName[i]
	}
	buffer.Write(bufferGroupName[:])
	buffer.WriteString(s.remoteFilename)
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func (s *storageDownloadTask) RecvRes(conn net.Conn) error {
	if err := s.RecvHeader(conn); err != nil {
		return fmt.Errorf("StorageDownloadTask RecvRes %v", err)
	}
	if s.localFilename != "" {
		if err := s.recvFile(conn); err != nil {
			return fmt.Errorf("StorageDownloadTask RecvRes %v", err)
		}
	} else {
		if err := s.recvBuffer(conn); err != nil {
			return fmt.Errorf("StorageDownloadTask RecvRes %v", err)
		}
	}
	return nil
}

func (s *storageDownloadTask) recvFile(conn net.Conn) error {
	file, err := os.Create(s.localFilename)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	writer := bufio.NewWriter(file)

	if err := writeFromConn(conn, writer, s.pkgLen); err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %s", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("StorageDownloadTask RecvFile %s", err)
	}
	return nil
}

func (s *storageDownloadTask) recvBuffer(conn net.Conn) error {
	var err error
	if s.buffer != nil {
		if int64(len(s.buffer)) < s.pkgLen {
			return fmt.Errorf("StorageDownloadTask buffer < pkgLen can't recv")
		}
		if err = writeFromConnToBuffer(conn, s.buffer, s.pkgLen); err != nil {
			return fmt.Errorf("StorageDownloadTask writeFromConnToBuffer %s", err)
		}
		return nil
	}
	writer := new(bytes.Buffer)

	if err = writeFromConn(conn, writer, s.pkgLen); err != nil {
		return fmt.Errorf("StorageDownloadTask RecvBuffer %s", err)
	}
	s.buffer = writer.Bytes()
	return nil
}

type storageDeleteTask struct {
	header
	groupName      string
	remoteFilename string
}

func (s *storageDeleteTask) SendReq(conn net.Conn) error {
	s.cmd = StorageProtoCmdDeleteFile
	s.pkgLen = int64(len(s.remoteFilename) + 16)

	if err := s.SendHeader(conn); err != nil {
		return err
	}
	buffer := new(bytes.Buffer)
	byteGroupName := []byte(s.groupName)
	var bufferGroupName [16]byte
	for i := 0; i < len(byteGroupName); i++ {
		bufferGroupName[i] = byteGroupName[i]
	}
	buffer.Write(bufferGroupName[:])
	buffer.WriteString(s.remoteFilename)
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		return err
	}
	return nil
}

func (s *storageDeleteTask) RecvRes(conn net.Conn) error {
	return s.RecvHeader(conn)
}
