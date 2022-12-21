package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
)

func rewriteAppendOnlyFileBackground() error {
	if server.aofRewriteChan != nil {
		return errors.New("already rewriting")
	}
	defer func() {
		if err := recover(); err != nil {
			log.Printf("aof rewrite panic: %v \n", err)
			server.aofRewriteChan <- false
		}
	}()

	server.aofRewriteChan = make(chan bool, 1)
	go func(tempfile string) {
		if err := rewriteAppendOnlyFile(tempfile); err != nil {
			server.aofRewriteChan <- false
		} else {
			server.aofRewriteChan <- true
		}
	}("temp-rewriteAof-bg.aof")
	return nil
}

//This function also called when the save command is executed
//it will overwrite the rewritten data to the 'filename' file
func rewriteAppendOnlyFile(filename string) error {
	now := GetTimeMs()
	//create temp file
	tempFile := "temp-rewriteAOF.aof"

	//if the file already exists, it is truncated
	//if the file does not exist, it is created with mode 0666
	fp, err := os.OpenFile(tempFile, os.O_APPEND|os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("open file in rewriteAppendOnlyFile error: %v \n", err)
		return err
	}
	aof := NewRioWithFile(fp)
	di := NewDictSafeIterator(server.db.data)
	for e := di.DictNext(); e != nil; e = di.DictNext() {
		key := e.Key
		o := e.Val

		//don't save if it has expired
		expireTime := getExpire(key)
		if expireTime != -1 && expireTime < now {
			continue
		}

		switch o.Type_ {
		case STR:
			setCmd := []byte("*3\r\n$3\r\nset\r\n")
			if err = aof.Write(setCmd, len(setCmd)); err != nil {
				goto wErr
			}
			if err = aof.WriteBulkString(key.StrVal()); err != nil {
				goto wErr
			}
			if err = aof.WriteBulkString(o.StrVal()); err != nil {
				goto wErr
			}
		}
		// save the expiry time
		if expireTime != -1 {
			cmd := []byte("*3\r\n$9\r\npexpireat\r\n")
			if aof.Write(cmd, len(cmd)) != nil {
				goto wErr
			}
			if aof.WriteBulkString(key.StrVal()) != nil {
				goto wErr
			}
			if aof.WriteBulkInt64(expireTime) != nil {
				goto wErr
			}
		}
	}
	ReleaseIterator(di)

	//make sure data will not remain on the OS's output buffers
	if err = aof.file.fp.Sync(); err != nil {
		log.Printf("sync disk error when aof rewriting: %v \n", err)
		goto wErr
	}
	if err = aof.file.fp.Close(); err != nil {
		log.Printf("close file error when aof rewriting: %v \n", err)
		goto wErr
	}

	//Use RENAME to make sure the DB file is changed atomically only if the generate DB file is ok.
	if err = os.Rename(tempFile, filename); err != nil {
		log.Printf("moving temp append only file on the final destination error: %v", err)
		_ = os.Remove(tempFile)
		return err
	}

	log.Printf("SYNC append only file rewrite done\n")
	return nil

wErr:
	aof.file.fp.Close()
	_ = os.Remove(tempFile)
	if di != nil {
		ReleaseIterator(di)
	}
	return err
}

func loadAppendOnlyFile(filename string) error {
	//create a fake client
	fakeClient := NewClient(0)
	f, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("open append only file for reading error: %v \n", err)
		return err
	}
	sn := bufio.NewScanner(f)
	for true {
		if !sn.Scan() {
			if sn.Err() != nil {
				goto rErr
			}
			break
		}

		if sn.Bytes()[0] != '*' {
			goto fmtErr
		}
		argc := bytes2int(sn.Bytes()[1:])
		if argc < 1 {
			goto fmtErr
		}

		for i := 0; i < argc; i++ {
			//there will be 'args' number of argument be read in expectations. if not we consider it format error
			if !sn.Scan() {
				goto fmtErr
			}

			if sn.Bytes()[0] != '$' {
				goto fmtErr
			}

			strLen := bytes2int(sn.Bytes()[1:])

			if !sn.Scan() {
				goto fmtErr
			}

			argStr := string(sn.Bytes())
			if len(argStr) != strLen {
				goto fmtErr
			}
			fakeClient.args = append(fakeClient.args, NewObject(STR, argStr))
		}

		//command lookup
		cmd := lookUpCommand(fakeClient.args[0].StrVal())
		if cmd == nil {
			log.Printf("unknown command %s readind append only file \n", fakeClient.args[0].StrVal())
			return errors.New("unknown command")
		}

		cmd.proc(fakeClient)
		//reset fake client
		fakeClient.args = make([]*GObj, 0)
	}

	// this point can only be reached when EOF is reached without errors.
	_ = f.Close()
	aofUpdateCurrentSize()
	server.aofRewriteBaseSize = 0
	return nil

rErr:
	log.Printf("read append only file error: %v \n", err)
	return err

fmtErr:
	log.Printf("bad file format reading the append only file \n")
	return errors.New("invalid aof file format")
}

//write the append only file buffer on disk.
func flushAppendOnlyFile() {
	if len(server.aofBuf) == 0 {
		return
	}
	f, err := os.OpenFile(server.aofFileName, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		log.Printf("can't open append only file: %v \n", err)
		return
	}

	n, err := f.Write([]byte(server.aofBuf))
	if err != nil {
		log.Printf("flush buffer to append only file error: %v \n", err)
		if n > 0 {
			server.aofCurrentSize += int64(n)
			server.aofBuf = server.aofBuf[n:]
		}
		return
	}

	server.aofCurrentSize += int64(n)
	server.aofBuf = ""
}

//the master goroutine calls this function when the child goroutine completes the AOF rewrite.
func bgRewriteDoneHandler(exitFlag bool) {
	if exitFlag == true {
		f, err := os.OpenFile("temp-rewriteAof-bg.aof", os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("Unable to open the temporary AOF produced by the child: %v \n", err)
			goto clearUp
		}
		if err = aofRewriteBufferWrite(f); err != nil {
			log.Printf("Error trying to flush the parent diff to the rewritten AOF: %v \n", err)
			_ = f.Close()
			goto clearUp
		}
		//The only remaining thing to do is to rename the temporary file to the configured file
		if err = os.Rename("temp-rewriteAof-bg.aof", server.aofFileName); err != nil {
			goto clearUp
		}

		aofUpdateCurrentSize()

		server.aofRewriteBaseSize = server.aofCurrentSize

		server.aofBuf = ""
	} else {
		log.Printf("Background AOF rewrite terminated with error \n")
	}

clearUp:
	server.aofRewriteBuf = make([]byte, 0)

	_ = os.Remove("temp-rewriteAof-bg.aof")

	server.aofRewriteChan = nil
}

//append the command to the AOF file or, if the AOF rewrite is in progress, to the AOF rewrite buffer.
func feedAppendOnlyFile(cmd *GedisCommand, args []*GObj) error {
	//Translate EXPIRE into PEXPIREAT
	var buf string

	if cmd.name == "expire" {
		buf = catAppendOnlyExpireAtFile(cmd, args[1], args[2])
	} else if cmd.name == "set" || cmd.name == "pexpireat" {
		buf = catAppendOnlyGenericCommand(args)
	} else {
		return nil
	}

	server.aofBuf += buf

	// If a background append only file rewriting is in progress we want to
	// accumulate the differences between the child DB and the current one
	// in a buffer, so that when the child process will do its work we
	// can append the differences to the new append only file.
	if server.aofRewriteChan != nil {
		aofRewriteBufferAppend(buf)
	}
	return nil
}

//append data to the AOF rewrite buffer,
func aofRewriteBufferAppend(s string) {
	server.aofRewriteBuf = append(server.aofRewriteBuf, []byte(s)...)
}

//write the buffer into the file
func aofRewriteBufferWrite(f *os.File) error {
	buf := server.aofRewriteBuf
	_, err := f.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func aofUpdateCurrentSize() {
	fi, err := os.Stat(server.aofFileName)
	if err != nil {
		log.Printf("Unable to obtain the AOF file length. stat: %v \n", err)
		return
	}
	server.aofCurrentSize = fi.Size()
}

//create the string representation of an PEXPIREAT command
func catAppendOnlyExpireAtFile(cmd *GedisCommand, key *GObj, seconds *GObj) string {
	when := seconds.IntVal()
	if cmd.name == "expire" {
		when = GetTimeMs() + when*1000
	}
	// build 'pexpireat' command
	args := []*GObj{
		NewObject(STR, "pexpireat"),
		key,
		NewObject(STR, strconv.FormatInt(when, 10)),
	}
	return catAppendOnlyGenericCommand(args)
}

//restore the command to the protocol format
func catAppendOnlyGenericCommand(args []*GObj) string {
	genericCmd := fmt.Sprintf("*%d\r\n", len(args))
	for i := 0; i < len(args); i++ {
		str := args[i].StrVal()
		genericCmd = genericCmd + fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
	}
	return genericCmd
}

func bytes2int(b []byte) int {
	num, _ := strconv.Atoi(string(b))
	return num
}
